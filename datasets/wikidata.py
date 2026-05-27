#
#		Wikidata
#		Cross-domain enrichment authority for the taxonomy pipeline. ~1.4M taxon entities after filtering.
#		Provides cross-authority IDs at scale (IPNI, Fungorum, POWO, WFO, Tropicos, GBIF, IUCN etc),
#		Wikipedia/Wikicommons/Wikispecies links, trait flags (edible, toxic, medicinal, annual, perennial),
#		page-count confidence signal, and broad multilingual vernacular names from 4 sources.
#		Source dump is 100GB+ JSON, pre-filtered with ripgrep before DuckDB ingestion.
#
#		TODO: https://www.wikidata.org/wiki/Q312959 via https://wfoplantlist.org/taxon/wfo-4000015502-2024-12?matched_id=wfo-4000008322&page=1
#
from ..utils.log import mesologger
import os
from typing import Dict
import duckdb

# File downloads & imports
from .. import settings, TMP_DIR
from ..utils.queries import build_rank_and_status, strip_rank_from_name, find_hybrids, name_cleanup, validate, write_to_disc, language_mappings
from ..utils.filehandlers import fetch, filter_gzip, get_system_resources
from ..utils.downloader import aria_ready

# Configuration
CHUNK_SIZE = 1000000  # Write new file every 1M relevant lines
MEMORY_SAFETY_MARGIN = 0.8  # Use 80% of available memory max
# /entities/20250318/wikidata-20250318-all.json.gz

source = {
	"name": "wikidata",
	"url": "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz",
	"use_aria": 3,
	"citation": '<a href="https://wikimediafoundation.org/" class="medium">Wikimedia</a>, Individual Contributors. Wikidata: A Free Collaborative Knowledgebase. Version YYYY-MM-DD. Wikimedia Foundation, San Francisco, CA. <a href="https://www.wikidata.org/" class="medium">https://www.wikidata.org/</a>'
}

# All taxonomic properties we want to keep - used to ripgrep-filter the 100GB+ JSON dump
RIPGREP_FILTERS = [
	# Plant and Fungi databases
	'P961',		# IPNI
	'P960',		# Tropicos
	'P6034',	# Plant Finder (Missouri Botanical)
	'P1391',	# Fungorum
	'P962',		# Mycobank
	'P7715',	# WFO ID
	'P5037',	# POWO
	'P1070',	# Plant List (old WFO)
	'P1727',	# Flora of North America
	'P1747',	# Flora of China
	# 'P3101',	# Flora of Australia (APNI) - disabled, has non-plant entities like Vertebrata (Q25241)
	'P4301',	# Pfaf.org, edibility etc
	'P8765',	# Royal Horticultural Society ID
	'P3102',	# Plantarium.ru, plenty of edge cases and very high ranks
	'P10701',	# Reflora (Brazil)
	# Q Identifiers - match entities that are instances/subclasses of these
	'Q4886',	# Cultivar
	'Q756',		# Plant
	'Q764'		# Fungi
]

# Main flow
async def update_wikidata(session):
	mesologger.info(f"############### Starting Wikidata Update  ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (settings.FORCE or update_available) and not settings.DOWNLOAD_ONLY:
		# Ensure aria downloads are complete (resume when .aria2 exists)
		if await aria_ready(source): process_wikidata(source)
		# Otherwise skip processing this round
		else: mesologger.warning(f"Skipping wikidata processing because source file is not ready")
	# Always return source for fuse, diff etc
	return source

# Pre-filter 100GB+ JSON dump with ripgrep to ~1.4M taxon entities, then extract into DuckDB
def process_wikidata(source: Dict):
	filtered = f"{os.path.splitext(source['latest_download'])[0]}.filtered"
	# Reuse existing filtered file if available, otherwise ripgrep the full dump
	if not os.path.isfile(os.path.join(TMP_DIR,filtered)): filtered = filter_gzip(source, "|".join([f'"{prop}"' for prop in RIPGREP_FILTERS]))
	# Process with DuckDB
	with duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Update settings
		db.execute(f"""
			SET threads = { int(get_system_resources()[0] - 1) };
			SET preserve_insertion_order = false;
			SET enable_progress_bar = false;
		""")
		# Parse filtered NDJSON into lookup table - keeps claims, labels, aliases, sitelinks as JSON
		db.execute(f"""
			CREATE TEMPORARY TABLE wikidata_parsed AS SELECT
				id, claims AS jclaims, labels AS jlabels, aliases AS jaliases, sitelinks AS jsitelinks
			FROM read_json('{ os.path.join(TMP_DIR,filtered)}', format='newline_delimited', ignore_errors=True, maximum_depth=6, map_inference_threshold=100)
			-- Only Q-entities (skip P-property definitions that slip through the filter)
			WHERE starts_with(id, 'Q')
			{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
		""")
		# db.sql(f"""SELECT jlabels FROM wikidata_parsed""")
		# db.sql(f"DESCRIBE wikidata_parsed").show(max_rows=200)
		# db.sql(f"SUMMARIZE wikidata_parsed").show(max_rows=200)
		mesologger.info(f"Wikidata JSON parsed")
		# Build initial table
		db.execute(f"""
			CREATE TABLE wikidata AS SELECT
				-- Wikidata Q-ID, ~1.4M rows after filtering for taxa
				id AS id_raw,
				-- P225 taxon name, 99% populated
				jclaims.P225[1].mainsnak.datavalue->>'$.value' AS name_raw,
				lower(name_raw) AS name_clean,
				-- P105 rank as Q-ID, mapped to name strings later in map_ranks()
				jclaims.P105[1].mainsnak.datavalue.value->>'$.id' AS rank_id,
				-- P171 parent taxon as Q-ID
				jclaims.P171[1].mainsnak.datavalue.value->>'$.id' AS parent_raw,
				-- P574 year nested in P225 qualifiers, ~9% populated. JSON 0-indexed
				REPLACE(jclaims.P225[1].qualifiers.P574[0].datavalue.value->>'$.time', '+', '') AS year_raw,
				-- P405 author as Q-ID nested in P225 qualifiers, 74% populated
				jclaims.P225[1].qualifiers.P405[0].datavalue.value->>'$.id' AS qauthor,
				-- P687 BHL page ID from P225 references, <1% populated
				jclaims.P225[1].references[1].snaks.P687[0].datavalue->>'$.value' AS bhl_page,
				-- Core name DB IDs (population rates from ~1.4M total):
				-- P961 IPNI 64%, strip LSID prefix
				REPLACE(jclaims.P961[1].mainsnak.datavalue->>'$.value', 'urn:lsid:ipni.org:names:', '') AS ipni_id,
				-- P1391 Fungorum 26%
				CAST(jclaims.P1391[1].mainsnak.datavalue->>'$.value' AS UINTEGER) AS fungorum_id,
				-- P962 MycoBank 26%
				CAST(jclaims.P962[1].mainsnak.datavalue->>'$.value' AS UINTEGER) AS mycobank_id,
				-- P960 Tropicos 42%
				CAST(jclaims.P960[1].mainsnak.datavalue->>'$.value' AS UINTEGER) AS tropicos_id,
				-- P3151 iNaturalist 18%, regex to handle dirty values like '923164-Sanangoideae'
				CAST(regexp_extract(jclaims.P3151[1].mainsnak.datavalue->>'$.value', '\\d+') AS UINTEGER) AS inaturalist_id,
				-- P7715 WFO 65%
				jclaims.P7715[1].mainsnak.datavalue->>'$.value' AS wfo_id,
				-- P685 NCBI 18%
				jclaims.P685[1].mainsnak.datavalue->>'$.value' AS ncbi_id,
				-- P846 GBIF 90%, normalize URL-shaped IDs occasionally ending up in Wikidata
				TRY_CAST(NULLIF(regexp_extract(jclaims.P846[1].mainsnak.datavalue->>'$.value', '\\d+$'), '') AS UINTEGER) AS gbif_id,
				-- P830 EoL 18%
				jclaims.P830[1].mainsnak.datavalue->>'$.value' AS eol_id,
				-- P5037 POWO 63%, strip LSID prefix
				replace(jclaims.P5037[1].mainsnak.datavalue->>'$.value','urn:lsid:ipni.org:names:','') AS powo_id,
				-- P10585 Catalogue of Life 65%
				jclaims.P10585[1].mainsnak.datavalue->>'$.value' AS col_id,
				-- Additional taxonomic databases (all <10% populated):
				-- P627 IUCN 4%
				jclaims.P627[1].mainsnak.datavalue->>'$.value' AS iucn_id,
				-- P815 ITIS 8%
				jclaims.P815[1].mainsnak.datavalue->>'$.value' AS itis_id,
				-- P1772 USDA 3%
				jclaims.P1772[1].mainsnak.datavalue->>'$.value' AS usda_id,
				-- P1421 GRIN 8%, strip URL prefix
				REPLACE(jclaims.P1421[1].mainsnak.datavalue->>'$.value', 'https://npgsweb.ars-grin.gov/gringlobal/', '') AS grin_id,
				-- P3606 BOLD 4%
				jclaims.P3606[1].mainsnak.datavalue->>'$.value' AS bold_id,
				-- P3031 EPPO 3%
				jclaims.P3031[1].mainsnak.datavalue->>'$.value' AS eppo_id,
				-- P6034 PlantFinder <1%
				jclaims.P6034[1].mainsnak.datavalue->>'$.value' AS plantfinder_id,
				-- P1070 PlantList 28%
				jclaims.P1070[1].mainsnak.datavalue->>'$.value' AS plantlist_id,
				-- P4301 PFAF <1%
				jclaims.P4301[1].mainsnak.datavalue->>'$.value' AS pfaf_id,
				-- P8765 RHS <1%
				jclaims.P8765[1].mainsnak.datavalue->>'$.value' AS rhs_id,
				-- Regional flora databases (all sparse):
				jclaims.P1727[1].mainsnak.datavalue->>'$.value' AS fna_id,
				jclaims.P1747[1].mainsnak.datavalue->>'$.value' AS foc_id,
				jclaims.P3101[1].mainsnak.datavalue->>'$.value' AS apni_id,
				jclaims.P2752[1].mainsnak.datavalue->>'$.value' AS nzor_id,
				jclaims.P9157[1].mainsnak.datavalue->>'$.value' AS otol_id,
				-- P8193 hardiness Q-ID, <1% populated
			 	CAST(jclaims.P8193[1].mainsnak.datavalue.value->>'$.id' AS VARCHAR) AS hardiness,
				-- P13162 illustration, <1% populated
				jclaims.P13162[1].mainsnak.datavalue->>'$.value' AS illustration,
				-- Wikimedia Commons: P373 category or P935 gallery, 8% populated
			 	COALESCE(jclaims.P373[1].mainsnak.datavalue->>'$.value',jclaims.P935[1].mainsnak.datavalue->>'$.value') AS wikicommons
			FROM wikidata_parsed;
		""")
		# Log
		mesologger.info(f"Loaded {db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} plants & fungi from { source['name'] }")
		# Parse year_raw ISO timestamp to USMALLINT, strict validation to reject malformed dates
		# Only ~9% populated — many Wikidata taxa lack P574 year qualifier on P225
		db.execute(f"""
			ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS year USMALLINT;
    		UPDATE wikidata SET year = CAST(DATE_PART('year', CAST(year_raw AS TIMESTAMP)) AS USMALLINT)
			WHERE year_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}:\\d{{2}}Z$'
			AND SUBSTR(year_raw, 6, 2) BETWEEN '01' AND '12'
			AND SUBSTR(year_raw, 9, 2) BETWEEN '01' AND '31';
			ALTER TABLE wikidata DROP COLUMN year_raw;
		""")
		mesologger.info(f"Extracted {db.execute('SELECT COUNT(*) FROM ' + source['name']  + ' WHERE year IS NOT NULL;').fetchone()[0]:,} proper years from { source['name'] }")
		# Add sitelinks & pagecount - 100% have pages, 14% have wikispecies, 8% wikicommons
		db.execute(f"""
			ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS wikipedia_pages MAP(VARCHAR, VARCHAR);
			-- page_count used downstream as confidence/ranking signal
			ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS page_count USMALLINT;
			ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS wikispecies VARCHAR;
			-- Build lang->title map from jsitelinks (e.g. 'en' -> 'Rosa canina')
			UPDATE wikidata SET wikipedia_pages = (
				SELECT map_from_entries(list_transform(list_filter(map_entries(jsitelinks), x -> x.key LIKE '%wiki'), x -> row(REPLACE(x.key, 'wiki', ''), x.value.title)))
				FROM wikidata_parsed wp WHERE wp.id = wikidata.id_raw
			);
			-- count of Wikipedia language editions for this taxon
			UPDATE wikidata SET page_count = cardinality(wikipedia_pages);
			-- fill wikicommons from sitelinks if P373/P935 claims were empty, extract wikispecies
			UPDATE wikidata SET
				wikicommons = COALESCE(wikicommons,list_any_value(map_extract(wikipedia_pages, 'commons'))),
			 	wikispecies = list_any_value(map_extract(wikipedia_pages, 'species'))
			WHERE wikipedia_pages IS NOT NULL;
		""")
		mesologger.info("Added Wikipedia pages and Commons/Species links")
		# Map Q ranks to name and clean them
		map_ranks(db,source)
		build_rank_and_status(db,source)
		strip_rank_from_name(db, source)
		# Find hybrids
		find_hybrids(db, source)
		# Set various flags like is edible, medicinal etc.
		set_flags(db, source)
		# Clean up name
		name_cleanup(db, source)
		# Get vernacular names
		wikidata_vernacular(db, source)
		# Debug: spot-check psychoactive taxa and top taxa by Wikipedia presence
		db.sql("SELECT * FROM wikidata WHERE psychoactive = true LIMIT 200").show(max_rows=200)
		db.sql(f"SELECT id_raw, name_raw, ipni_id, fungorum_id, page_count, vernacular FROM wikidata ORDER BY page_count DESC LIMIT 200;").show(max_rows=200)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)

# Map Wikidata Q-IDs for taxonomic ranks to our canonical rank strings
# ~50 Q-IDs mapped, unmapped ones fall through to 'unranked'
def map_ranks(db: duckdb.DuckDBPyConnection, source: dict):
	db.execute(f"""
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS rank_raw VARCHAR;
		UPDATE wikidata SET rank_raw = CASE rank_id
			WHEN 'Q7432' THEN 'species'
			WHEN 'Q34740' THEN 'genus'
			WHEN 'Q68947' THEN 'subspecies'
			WHEN 'Q767728' THEN 'variety'
			WHEN 'Q35409' THEN 'family'
			WHEN 'Q164280' THEN 'subfamily'
			WHEN 'Q3238261' THEN 'subgenus'
			WHEN 'Q227936' THEN 'tribe'
			WHEN 'Q3181348' THEN 'section'
			WHEN 'Q36602' THEN 'order'
			WHEN 'Q2136103' THEN 'superfamily'
			WHEN 'Q3965313' THEN 'subtribe'
			WHEN 'Q4886' THEN 'cultivar'
			WHEN 'Q279749' THEN 'form'
			WHEN 'Q5867959' THEN 'suborder'
			WHEN 'Q3025161' THEN 'series'
			WHEN 'Q5998839' THEN 'subsection'
			WHEN 'Q37517' THEN 'class'
			WHEN 'Q5867051' THEN 'subclass'
			WHEN 'Q5868144' THEN 'superorder'
			WHEN 'Q2889003' THEN 'subvariety'
			WHEN 'Q38348' THEN 'phylum'
			WHEN 'Q112082101' THEN 'subform'
			WHEN 'Q3825509' THEN 'form'
			WHEN 'Q1153785' THEN 'infraorder'
			WHEN 'Q4150646' THEN 'cultivar'
			WHEN 'Q6311258' THEN 'parvorder'
			WHEN 'Q2007442' THEN 'infrafamily'
			WHEN 'Q14817220' THEN 'supertribe'
			WHEN 'Q334460' THEN 'infraclass'
			WHEN 'Q3491997' THEN 'subdivision'
			WHEN 'Q125838332' THEN 'oogenus'
			WHEN 'Q3504061' THEN 'superclass'
			WHEN 'Q10861375' THEN 'subsection'
			WHEN 'Q21061732' THEN 'series'
			WHEN 'Q10861426' THEN 'section'
			WHEN 'Q36732' THEN 'kingdom'
			WHEN 'Q2752679' THEN 'subkingdom'
			WHEN 'Q113015256' THEN 'ichnospecies'
			WHEN 'Q1993179' THEN 'supersection'
			WHEN 'Q855769' THEN 'strain'
			WHEN 'Q6054237' THEN 'magnorder'
			WHEN 'Q2111790' THEN 'superphylum'
			WHEN 'Q6541077' THEN 'subcohort'
			WHEN 'Q13198444' THEN 'subseries'
			WHEN 'Q2981883' THEN 'cohort'
			WHEN 'Q19858692' THEN 'superkingdom'
			-- unmapped Q-IDs fall through to unranked
			ELSE 'unranked'
		END;
	""")

# Set trait boolean flags from P279 (subclass of), P366 (has use), P789 (edibility)
# Sparse but high-value: medicinal ~11k, edible ~2.2k, vegetable ~753, fruit ~177
def set_flags(db: duckdb.DuckDBPyConnection, source: dict):
	db.execute(f"""
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS edible BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS toxic BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS psychoactive BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS herb BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS useful BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS annual BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS perennial BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS medicinal BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS vegetable BOOLEAN;
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS fruit BOOLEAN;

		-- Extract Q-ID lists from three claim properties in one pass over wikidata_parsed
		WITH p_data AS (
			SELECT
				id,
				-- P279 "subclass of" — herbs, useful plants, annuals, perennials, poisonous
				COALESCE(list_transform(jclaims.P279, x -> json_extract_string(x.mainsnak.datavalue.value, '$.id')), []) AS p279_list,
				-- P366 "has use" — medicinal, vegetable, fruit, psychoactive
				COALESCE(list_transform(jclaims.P366, x -> json_extract_string(x.mainsnak.datavalue.value, '$.id')), []) AS p366_list,
				-- P789 "edibility" — edible/deadly mushroom flags
				COALESCE(list_transform(jclaims.P789, x -> json_extract_string(x.mainsnak.datavalue.value, '$.id')), []) AS p789_list
			FROM wikidata_parsed
		)
		UPDATE wikidata SET
			-- edible: food plant (P279) OR food use (P366) OR edible mushroom (P789)
			edible = list_contains(p279_list, 'Q9323487') OR list_contains(p366_list, 'Q2095') OR list_contains(p789_list, 'Q654236'),
			-- toxic: deadly mushroom (P789) OR poisonous plant (P279)
			toxic = list_contains(p789_list, 'Q19888591') OR list_contains(p279_list, 'Q21028485'),
			-- fruit: from both P366 and P279
			fruit = list_contains(p366_list, 'Q3314483') OR list_contains(p279_list, 'Q3314483'),
			-- P279 subclass-of flags
			herb = list_contains(p279_list, 'Q207123'),
			useful = list_contains(p279_list, 'Q11004'),
			annual = list_contains(p279_list, 'Q192691'),
			perennial = list_contains(p279_list, 'Q157957'),
			-- P366 has-use flags
			medicinal = list_contains(p366_list, 'Q188840'),
			vegetable = list_contains(p366_list, 'Q11004'),
			psychoactive = list_contains(p366_list, 'Q3706669')
		FROM p_data WHERE wikidata.id_raw = p_data.id;
	""")
	mesologger.info(f"Added wikidata flags like edible, medicinal, food, perennial etc")

# Build vernacular names from 4 sources: wikipedia page titles, JSON labels, aliases, P1843 claims
# ~208k taxa end up with vernacular, filtered to remove scientific name leakage
def wikidata_vernacular(db: duckdb.DuckDBPyConnection, source: dict):
	# Staging table for all vernacular candidates before dedup and cleanup
	db.execute(f"""CREATE TEMPORARY TABLE vernacular (id VARCHAR, language VARCHAR, common_name VARCHAR);""")

	# Source 1: Wikipedia page titles as common names, excluding meta wikis and scientific name matches
	db.execute(f"""
		INSERT INTO vernacular SELECT w.id_raw, x.key, x.value
		FROM wikidata AS w, UNNEST(map_entries(w.wikipedia_pages)) AS t(x)
		WHERE x.key NOT IN ('commons', 'species', 'wikidata', 'mediawiki', 'simple') AND LOWER(x.value) NOT IN (LOWER(w.name_raw), w.name_clean);
	""")
	inital_count = db.execute('SELECT COUNT(*) FROM vernacular').fetchone()[0]
	mesologger.info(f"Copied {inital_count:,} names from wikipedia page titles")
	# Source 2: Wikidata JSON labels (one per language per entity), skip scientific name matches
	db.execute("""
		WITH unnested AS (SELECT id, UNNEST(map_values(jlabels)) AS entry FROM wikidata_parsed)
		INSERT INTO vernacular SELECT id, entry.language, entry.value AS common_name FROM unnested
		WHERE common_name IS NOT NULL AND NOT EXISTS (
			SELECT 1 FROM wikidata
			WHERE id_raw = unnested.id AND (LOWER(name_raw) = LOWER(common_name) OR name_clean = LOWER(common_name))
		);
	""")
	label_count = db.execute('SELECT COUNT(*) FROM vernacular').fetchone()[0] - inital_count
	mesologger.info(f"Added {label_count:,} names from jlabels")
	# Source 3: Wikidata JSON aliases (multiple per language), split on comma/semicolon
	db.execute("""
		WITH flattened AS (
			SELECT id, language, UNNEST(string_split_regex(value, '[,;]')) AS common_name
			FROM (SELECT id, UNNEST(map_values(jaliases), recursive := true) FROM wikidata_parsed)
		)
		INSERT INTO vernacular SELECT * FROM flattened
		WHERE common_name IS NOT NULL AND LOWER(common_name) NOT IN (
			SELECT LOWER(name_raw) FROM wikidata WHERE id_raw = flattened.id
			UNION SELECT name_clean FROM wikidata WHERE id_raw = flattened.id
		);
	""")
	alias_count = db.execute('SELECT COUNT(*) FROM vernacular').fetchone()[0] - inital_count - label_count
	mesologger.info(f"Added {alias_count:,} names from jaliases")
	# Source 4: P1843 common name claims - structured multilingual names with language tags
	db.execute("""CREATE TEMPORARY TABLE P1843_data AS SELECT wp.id, UNNEST(wp.jclaims.P1843) AS claim FROM wikidata_parsed wp WHERE wp.jclaims.P1843 IS NOT NULL""")
	mesologger.info('Unnested P1843 claims')
	# Split comma/semicolon-separated names, skip undetermined (und) and miscellaneous (mis) languages
	db.execute("""
		WITH extracted AS (
			SELECT id, v[1] AS language, UNNEST(string_split_regex(v[2], '[,;]')) AS common_name
			FROM (SELECT p.id, json_extract_string(p.claim.mainsnak.datavalue.value, ['$.language', '$.text']) AS v FROM P1843_data p)
		)
		INSERT INTO vernacular SELECT * FROM extracted
		WHERE language NOT IN ('und', 'mis') AND common_name IS NOT NULL;
	""")
	highest_count = db.execute('SELECT COUNT(*) FROM vernacular').fetchone()[0]
	p_count = highest_count - inital_count - label_count - alias_count
	mesologger.info(f"Added {p_count:,} vernacular names from P1843 claims")
	# db.sql('SELECT COUNT(*) FROM vernacular').show(max_rows=80)
	# db.sql(f"SELECT * FROM vernacular WHERE id = 'Q27734'").show(max_rows=80)
	# Remove scientific name leakage, infrarank markers, hybrid names, and empty strings
	db.execute(f"""
		DELETE FROM vernacular v WHERE
			-- Names that are or start with the scientific name (but not short suffixes like German -baum)
			EXISTS (
				SELECT 1 FROM wikidata w WHERE w.id_raw = v.id AND (
					(len(w.name_raw) > 5 AND len(v.common_name) > 8 AND starts_with(lower(v.common_name),lower(w.name_raw)) AND ABS(LENGTH(v.common_name) - LENGTH(w.name_raw)) > 4)
					OR lower(v.common_name) IN (w.name_clean, lower(w.name_raw))
				)
			)
			-- Infrarank markers and wiki category prefixes (e.g. "Category:Begonia Palmata")
			OR REGEXP_MATCHES(lower(v.common_name), '(category:|subgen\\.|[ ]subgen[ ]|ssp\\.|[ ]ssp[ ]|subg\\.|[ ]subg[ ]|var\\.|[ ]var[ ]|subsp\\.|[ ]subsp[ ]|fo\\.|f\\.|sp\\. nov\\.)')
			OR CONTAINS(v.common_name, '×') OR trim(v.common_name) = '';
	""")
	mesologger.info(f"Deleted {(highest_count - db.execute('SELECT COUNT(*) FROM vernacular').fetchone()[0]):,} unwanted vernacular names")
	# Normalize 3-letter ISO 639-2 language codes to 2-letter ISO 639-1
	db.execute(f"""CREATE TEMPORARY TABLE IF NOT EXISTS language_map AS SELECT * FROM (VALUES {language_mappings}) AS t(iso639_2, iso639_1);""")
	db.execute(f"""
		UPDATE vernacular SET language = COALESCE((SELECT iso639_1 FROM language_map WHERE iso639_2 = vernacular.language), language)
		WHERE language IN (SELECT iso639_2 FROM language_map);
	""")
	mesologger.info(f"Normalized language in vernacular names")
	# db.sql(f"SELECT DISTINCT common_name, COUNT(common_name) FROM vernacular GROUP BY common_name ORDER BY COUNT(common_name) DESC").show(max_rows=200)
	# Aggregate cleaned vernacular into MAP(lang -> names[]) on main table
	db.execute(f"""
		ALTER TABLE wikidata ADD COLUMN IF NOT EXISTS vernacular MAP(VARCHAR, VARCHAR[]);
		UPDATE wikidata w SET vernacular = (
			SELECT MAP_FROM_ENTRIES(ARRAY_AGG(ROW(language, names)))
			FROM (SELECT v.language, ARRAY_AGG(v.common_name) AS names FROM vernacular v WHERE v.id = w.id_raw GROUP BY v.language)
		);
	""")
	mesologger.info(f"Added {db.execute('SELECT COUNT(*) FROM vernacular').fetchone()[0]:,} vernacular names to wikidata table")
	# Debug: check for anomalies in vernacular data
	if settings.VERBOSE:
		# Suspiciously long names that might be descriptions or sentences
		db.sql("SELECT * FROM vernacular WHERE length(common_name) > 30;").show(max_rows=250)
		# NULL language or name that slipped through filters
		db.sql("SELECT * FROM vernacular WHERE common_name IS NULL OR language IS NULL;").show(max_rows=250)
		# Type mismatches from JSON extraction
		db.sql("SELECT * FROM vernacular WHERE typeof(common_name) != 'VARCHAR' OR typeof(language) != 'VARCHAR';").show(max_rows=250)
		# Names with non-letter characters (numbers, symbols) that might be codes or IDs
		db.sql("SELECT * FROM vernacular WHERE common_name IS NOT NULL AND regexp_matches(common_name, '[^a-zA-Z\\p{L}\\s\\-]');").show(max_rows=250)
		# Language distribution to spot unexpected codes
		db.sql("SELECT DISTINCT language, COUNT(language) FROM vernacular GROUP BY language ORDER BY COUNT(language) DESC;").show(max_rows=450)
	# Free staging table — vernacular data is now on the main wikidata table
	db.execute(f"""DROP TABLE vernacular;""")
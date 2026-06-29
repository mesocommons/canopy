#
# 		The Catalogue of Life
#
# 		An assembly of expert-based global species checklists with the aim to build a comprehensive catalogue of
# 		all known species of organisms on Earth. Continuous progress is made towards completion, but for now, it probably
# 		includes just over 80% of the world's known species. The Catalogue of Life estimates 2.3M extant species on the
# 		planet recognised by taxonomists at present time. This means that for many groups it continues to be deficient,
# 		and users may notice that many species are still missing from the Catalogue.
#
# 		### What's new in March 2025 edition? #### New global checklist:
# 		* WoRMS Mysidacea: World List of Lophogastrida, Stygiomysida and Mysida replaced order Mysida from ITIS and added new global checklists for crustacean orders Stygiomysida and †Pygocephalomorpha
#
# 		#### 88 checklists have been updated:
# 		* 3i Auchenorrhyncha * Alucitoidea * GLI * ITIS * Pterophoroidea * Scarabs * SF Aphid * SF Chrysididae * SF Coreoidea
# 		* SF Dermaptera * SF Isoptera * SF Lygaeoidea * SF Mantodea * SF Orthoptera * SF Phasmida * SF Plecoptera * SF Psocodea
# 		* Tortricid.net * WCO * WOL * World Ferns * World Plants * WSC (a new import via API) * WoRMS, 65 checklists
#
# 		#### Other changes: * Systema Dipterorum ver. 5.6 of Dec 2024 and ReptileDB of Mar 2024 re-synced.
#
# 		Operational note (2026-04): CoL publishes roughly monthly releases (recent ~22-41 day spacing, median ~29 days)
# 		through ChecklistBank with explicit release metadata, quality markers, and governance roles. References:
# 		https://api.checklistbank.org/dataset/3 (working project), https://api.checklistbank.org/dataset/314909 (COL26.4),
# 		https://raw.githubusercontent.com/CatalogueOfLife/data/master/release-config.yaml (release config + governance notes),
# 		and the governance principles paper https://link.springer.com/article/10.1007/s13127-021-00516-w.
#
# 		Data policy here stays conservative for year voting inputs: keep CoL year extraction narrow and high-confidence for fuse,
# 		while treating broader publication-year hints as optional future side signals (separate columns) to avoid swaying consensus.

source = {
    "name": "col",
    "url": "https://download.checklistbank.org/col/latest_coldp.zip",
	"citation": '<a href="https://www.catalogueoflife.org/" class="medium">Catalogue of Life</a>, Bánki, O., Roskov, Y., Döring, M., Ower, G., Hernández Robles, D. R., Plata Corredor, C. A., Stjernegaard Jeppesen, T., Örn, A., Pape, T., Hobern, D., Garnett, S., Little, H., DeWalt, R. E., Ma, K., Miller, J., & Orrell, T. Catalogue of Life (working draft). Version YYYY-MM-DD. The Catalogue of Life Partnership. <a href="https://www.checklistbank.org/dataset/308619" class="medium">https://www.checklistbank.org/dataset/308619</a>'
}

# Internal
from ..utils.log import mesologger
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import os, re, unicodedata, zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import publication_filter, name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc, language_mappings

# Import shared WGSRPD lookup mapping from canopy config schema
from ..config.schema import WGSRPDLOOKUP

# Main function called as asyncio Task from run.py
async def update_col(session):
	mesologger.info(f"############### Updating Catalogue of Life ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_col(source)
	# Return the source dict containing processing outcomes
	return source

def process_col(source: dict):
	# Resolve local source path (already ensured by fetch in S3 mode)
	source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
	# Load zipfile and duckdb
	with zipfile.ZipFile(source_path, 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load TSVs — quotechar='\0' disables quoting (CoL fields contain unescaped quotes)
		# Force mixed-form authorship year column to VARCHAR to avoid BIGINT inference/binder failures
		name_tsv = db.read_csv(zip.open('NameUsage.tsv'),parallel=True,null_padding=True, delimiter='\t', quotechar='\0', dtype={'col:combinationAuthorshipYear': 'VARCHAR'})
		vernacular_tsv = db.read_csv(zip.open('VernacularName.tsv'),parallel=True,null_padding=True, delimiter='\t', quotechar='\0')
		# col:areaID forced to VARCHAR because mixed numeric/text values cause type detection failures
		distribution_tsv = db.read_csv(zip.open('Distribution.tsv'),parallel=True,null_padding=True, delimiter='\t', quotechar='\0', dtype={'col:areaID': 'VARCHAR'})
		# Materialize distribution as temp table — queried multiple times across habitat strategies
		db.execute("CREATE TEMP TABLE col_distribution AS SELECT * FROM distribution_tsv")
		# reference.json only — read_csv chokes on the nested JSON structure
		# Pin sparse fields so DuckDB sampling cannot drop optional keys like DOI
		reference_json = db.read_json(zip.open('reference.json'),ignore_errors=True, columns={
			'id': 'VARCHAR',
			'issued': 'STRUCT("date-parts" BIGINT[][], literal VARCHAR)',
			'title': 'VARCHAR',
			'container-title': 'VARCHAR',
			'DOI': 'VARCHAR',
		})
		mesologger.info(f"""{source['name']} archive unzipped""")
		db.execute(f"""
			CREATE TABLE col AS SELECT
				-- CoL taxon ID, ~633k rows after kingdom filter
				"col:ID" AS id_raw,
				-- full scientific name, lowercased
			 	lower("col:scientificName") AS name_clean,
				-- parent taxon for hierarchy traversal
			 	"col:parentID" AS parent_raw,
				-- taxonomic authority, ~5% null
			 	"col:authorship" AS author_raw,
				-- 16 ranks, species ~544k dominant
			 	"col:rank" AS rank_raw,
				-- 2 kingdoms: plantae ~463k, fungi ~170k
				lower("col:kingdom") AS kingdom,
				-- 22 phyla, ~99% populated
				lower("col:phylum") AS phylum,
				-- 6 subphyla, only ~3k rows populated
				lower("col:subphylum") AS subphylum,
				-- 106 classes, ~624k populated
				lower("col:class") AS class,
				-- 20 subclasses, only ~25k populated
				lower("col:subclass") AS subclass,
				-- 536 orders, ~612k populated
				lower("col:order") AS "order",
				-- 2177 families, ~611k populated
				lower("col:family") AS family,
				-- 389 subfamilies, ~236k populated (~37%)
				lower("col:subfamily") AS subfamily,
				-- ~29k distinct genera, ~596k populated
				lower("col:genus") AS genus,
				-- ~22k distinct epithets, only ~52k populated (infraspecific taxa)
				lower("col:species") AS species,
				-- fallback year, reference.json year preferred
			 	CAST(regexp_extract("col:combinationAuthorshipYear", '\\d{{4}}', 0) AS USMALLINT) AS year,
				-- notho marks hybrids, ~9k rows
			 	CAST ("col:notho" IS NOT NULL AS BOOLEAN) AS hybrid,
				-- accepted or provisionally accepted
			 	"col:status" AS status_raw,
				-- ~2k extinct taxa
			 	CAST("col:extinct" AS BOOLEAN) AS extinct,
				-- external URLs: IPNI, Fungorum, Tropicos, WFO
				"col:link" AS link_raw
			FROM name_tsv n
			WHERE lower("col:kingdom") IN ('plantae','fungi')
		""")
		mesologger.info(f"Loaded {db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} plants & fungi from { source['name'] }")
		# Build compact WGSRPD name lookup table once (small dictionary -> DuckDB)
		wgsrpd_lookup = build_wgsrpd_name_lookup()
		db.execute("CREATE TEMP TABLE col_name_lookup(normalized_name VARCHAR, location_code VARCHAR)")
		db.executemany("INSERT INTO col_name_lookup VALUES (?, ?)", [(name, code) for name, code in wgsrpd_lookup.items()])
		# Build mapped text-area lookup and apply habitat enrichment using option 3
		create_col_area_lookup(db, 'col')
		mesologger.warning(f"Resolved {db.execute('SELECT COUNT(*) FROM col_area_lookup').fetchone()[0]:,} CoL text areas to WGSRPD/ISO fallback codes")
		apply_col_habitats(db, 'col')
		mesologger.info(f"Added native habitats to {db.execute('SELECT COUNT(*) FROM ' + source['name'] + ' WHERE native_to IS NOT NULL;').fetchone()[0]:,} { source['name'] } rows")
		mesologger.info(f"Added regions to {db.execute('SELECT COUNT(*) FROM ' + source['name'] + ' WHERE regions IS NOT NULL;').fetchone()[0]:,} { source['name'] } rows")
		# Enrich from reference.json: year, publication name, BHL title ID
		db.execute(f"""
			ALTER TABLE col ADD COLUMN publication_short VARCHAR;
			ALTER TABLE col ADD COLUMN bhl_title UINTEGER;
			UPDATE col SET
				-- issued.date-parts -> year, preferred over combinationAuthorshipYear
			 	year = COALESCE(
		 			CASE WHEN NOT signbit(flatten(r.issued."date-parts")[1]) THEN flatten(r.issued."date-parts")[1] ELSE NULL END,
		 			col.year,
		 			-- strict title fallback -> trailing parenthesized year only, e.g. "... (1957)"
		 			CAST(NULLIF(REGEXP_EXTRACT(r.title, '\\((\\d{{4}})\\)\\s*$', 1), '') AS USMALLINT)
		 		),
				-- container-title -> short publication name, max 120 chars
			 	publication_short = substring(trim(REGEXP_EXTRACT(REGEXP_REPLACE(r."container-title", '^(In: |in )', ''), { publication_filter }, 1)), 1, 120),
				-- DOI -> BHL title ID when DOI contains bhl.title pattern
				bhl_title = NULLIF(REGEXP_EXTRACT(lower(r.DOI), 'bhl\\.title\\.(\\d+)', 1),'')
			FROM name_tsv n JOIN reference_json r ON n."col:nameReferenceID" = r.id WHERE n."col:ID" = col.id_raw;
		""")
		mesologger.info(f"Added {db.execute('SELECT COUNT(publication_short) FROM ' + source['name']).fetchone()[0]:,} publications to { source['name'] }")
		# Add vernacular names from CoL VernacularName.tsv
		col_vernacular(vernacular_tsv, db)
		find_hybrids(db,source)
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Extract external/source IDs
		external_col_ids(db,source)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)

# Extract external IDs from link_raw URLs into dedicated columns, then drop link_raw
def external_col_ids(db: duckdb.DuckDBPyConnection, source: dict):
	# Index Fungorum ID from URL, ~156k matched
	db.execute(f"""
		ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS fungorum_id UINTEGER;
		UPDATE col SET fungorum_id = REGEXP_EXTRACT(link_raw, '(?:https?://)?www\\.indexfungorum\\.org/Names/NamesRecord\\.asp\\?RecordID=(\\d+)', 1)
		WHERE link_raw LIKE '%indexfungorum.org/Names/NamesRecord.asp?RecordID=%'
	""")
	mesologger.info(f"""Added { db.execute(f"SELECT COUNT(*) FROM {source['name']} WHERE fungorum_id IS NOT NULL;").fetchone()[0] } Fungorum IDs to CoL""")
	# POWO/IPNI ID from LSID URN, ~121k matched. Uses POWO IDs not IPNI as some end in -4
	db.execute(f"""
		ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS powo_id VARCHAR;
		UPDATE col SET powo_id = REGEXP_EXTRACT(link_raw, 'urn:lsid:ipni\\.org:names:(\\d+-\\d+)', 1)
		WHERE link_raw LIKE '%urn:lsid:ipni.org:names:%'
	""")
	mesologger.info(f"""Added { db.execute(f"SELECT COUNT(*) FROM {source['name']} WHERE powo_id IS NOT NULL;").fetchone()[0] } POWO IDs to CoL""")
	# WFO ID from worldfloraonline.org URL, ~1.2k matched (very sparse)
	db.execute(f"""
		ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS wfo_id VARCHAR;
		UPDATE col SET wfo_id = REGEXP_EXTRACT(link_raw, '(?:https?://)?list\\.worldfloraonline\\.org/(wfo-\\d+)(?:-\\d{4}-\\d{2})?', 1)
		WHERE link_raw LIKE '%worldfloraonline.org/wfo-%'
	""")
	mesologger.info(f"""Added { db.execute(f"SELECT COUNT(*) FROM {source['name']} WHERE wfo_id IS NOT NULL;").fetchone()[0] } WFO IDs to CoL""")
	# Tropicos ID from tropicos.org URL, ~25k matched
	db.execute(f"""
		ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS tropicos_id UINTEGER;
		UPDATE col SET tropicos_id = REGEXP_EXTRACT(link_raw, 'https://www\\.tropicos\\.org/name/(\\d+)', 1)
		WHERE link_raw LIKE '%tropicos.org/name/%'
	""")
	mesologger.info(f"""Added { db.execute(f"SELECT COUNT(*) FROM {source['name']} WHERE tropicos_id IS NOT NULL;").fetchone()[0] } Tropicos IDs to CoL""")
	# All IDs extracted, drop the raw URL column to save parquet space
	db.execute(f"""ALTER TABLE col DROP COLUMN link_raw;""")


# Map CoL 3-letter language codes to 2-letter ISO and build lang:name vernacular array
# VernacularName.tsv has 313 distinct languages, ~471k name rows for filtered plantae/fungi
def col_vernacular(vernacular_tsv, db: duckdb.DuckDBPyConnection):
	# Add vernacular column to col table
	db.execute("ALTER TABLE col ADD COLUMN IF NOT EXISTS vernacular VARCHAR[]")
	# Map 3-letter ISO codes to 2-letter via shared language_mappings, aggregate as lang:name pairs
	db.execute(f"""
		-- 3-letter to 2-letter ISO language mapping
		WITH mapping_values AS (SELECT * FROM (VALUES {language_mappings}) AS t(lang3, lang2)),
		names AS (
			SELECT
				v."col:taxonID" AS id_raw,
				-- fall back to raw language code if no mapping exists (e.g. already 2-letter)
				COALESCE(mv.lang2, v."col:language") AS iso_lang,
				trim(lower(v."col:name")) AS name
			FROM vernacular_tsv v
			LEFT JOIN mapping_values mv ON lower(v."col:language") = mv.lang3
			WHERE v."col:name" IS NOT NULL
			AND length(trim(v."col:name")) > 0
			-- only keep names for taxa in our filtered plantae/fungi set
			AND EXISTS (SELECT 1 FROM col c WHERE c.id_raw = v."col:taxonID")
		)
		-- aggregate into lang:name array per taxon
		UPDATE col c SET vernacular = sub.vern
		FROM (
			SELECT id_raw, array_agg(iso_lang || ':' || name) AS vern
			FROM names WHERE iso_lang IS NOT NULL
			GROUP BY id_raw
		) sub
		WHERE c.id_raw = sub.id_raw;
	""")
	# Log
	count = db.execute("SELECT COUNT(*) FROM col WHERE vernacular IS NOT NULL AND len(vernacular) > 0").fetchone()[0]
	mesologger.info(f"Added vernacular names to {count:,} CoL entries")


# Build CoL text area lookup: 5 progressively aggressive normalization passes
# (exact -> strip parens -> first semicolon segment -> comma split -> last comma segment)
# then fuzzy match against WGSRPD names, with ISO 2-letter code fallback
def create_col_area_lookup(db: duckdb.DuckDBPyConnection, target: str = 'col'):
	db.execute(f"""
		CREATE TEMP TABLE col_area_lookup AS
		-- collect distinct free-text area strings from distribution rows with gazetteer='text'
		WITH text_areas AS (
			SELECT DISTINCT d."col:area" AS area_raw
			FROM col_distribution d
			JOIN {target} c ON c.id_raw = d."col:taxonID"
			WHERE lower(d."col:gazetteer") = 'text' AND d."col:area" IS NOT NULL AND len(trim(d."col:area")) > 0
		),
		-- 5 progressively aggressive text extraction passes, ordered by specificity
		candidates AS (
			-- pass 0: exact raw text
			SELECT area_raw, area_raw AS candidate, 0 AS ord FROM text_areas
			UNION ALL
			-- pass 1: strip parenthesized qualifiers e.g. "Brazil (south)"
			SELECT area_raw, trim(regexp_replace(area_raw, '\\([^)]*\\)', '', 'g')) AS candidate, 1 AS ord FROM text_areas
			UNION ALL
			-- pass 2: take first semicolon-delimited segment
			SELECT area_raw, trim(split_part(area_raw, ';', 1)) AS candidate, 2 AS ord FROM text_areas WHERE area_raw LIKE '%;%'
			UNION ALL
			-- pass 3: split on commas and try each segment
			SELECT area_raw, trim(value) AS candidate, 3 AS ord FROM text_areas, unnest(string_split(area_raw, ',')) AS t(value)
			UNION ALL
			-- pass 4: last comma segment (often the country in "city, country" patterns)
			SELECT area_raw, trim(regexp_extract(area_raw, '([^,]+)$', 1)) AS candidate, 4 AS ord FROM text_areas WHERE area_raw LIKE '%,%'
		),
		-- normalize candidates: strip diacritics, punctuation, collapse whitespace
		norm AS (
			SELECT area_raw, candidate, ord,
				lower(trim(regexp_replace(regexp_replace(replace(
					regexp_replace(
						translate(replace(lower(candidate), '&', ' and '), 'àáâãäåāăąçćčďèéêëēėęěìíîïīįıñńňòóôõöøōřśšùúûüūýÿžźż', 'aaaaaaaaacccdeeeeeeeeiiiiiiinnnooooooorssuuuuuyyzzz'),
						'[\\.`]+', '', 'g'
					),
					'''', ''
				),
				'[^a-z0-9]+', ' ', 'g'),
				'\\s+', ' ', 'g'))) AS normalized
			FROM candidates WHERE candidate IS NOT NULL AND len(trim(candidate)) > 0
		),
		-- join normalized candidates against WGSRPD lookup, keep best match per area_raw
		matched AS (
			SELECT n.area_raw, m.location_code,
				ROW_NUMBER() OVER (PARTITION BY n.area_raw ORDER BY n.ord ASC, length(n.candidate) DESC) AS rn
			FROM norm n JOIN col_name_lookup m ON m.normalized_name = n.normalized
		),
		-- bare 2-letter codes (e.g. "BR") used as ISO fallback when no WGSRPD match
		iso_fallback AS (
			SELECT area_raw, lower(trim(area_raw)) AS location_code
			FROM text_areas
			WHERE regexp_matches(trim(area_raw), '^[A-Za-z]{{2}}$')
		)
		SELECT area_raw, location_code FROM matched WHERE rn = 1
		UNION ALL
		SELECT i.area_raw, i.location_code
		FROM iso_fallback i LEFT JOIN matched m ON m.area_raw = i.area_raw AND m.rn = 1
		WHERE m.area_raw IS NULL
	""")

# Apply habitat aggregation from Distribution.tsv (3 gazetteer types: tdwg, iso, text)
# Produces native_to ~2.3k rows (native flag set) and regions ~344k rows (any occurrence)
def apply_col_habitats(db: duckdb.DuckDBPyConnection, target: str = 'col'):
	db.execute(f"""
		-- Unify all 3 gazetteer types into one base table with resolved location codes
		CREATE OR REPLACE TEMP TABLE habitat_base AS
		SELECT d."col:taxonID" AS id,
			COALESCE(
				-- tdwg/iso: use areaID directly, strip '-oo' country-level suffix
				CASE WHEN lower(d."col:gazetteer") in ('tdwg','iso') THEN replace(lower(d."col:areaID"::VARCHAR),'-oo','') END,
				-- text: use the fuzzy-matched WGSRPD code from col_area_lookup
				CASE WHEN lower(d."col:gazetteer") = 'text' THEN m.location_code END
			) AS location,
			-- native flag from either degreeOfEstablishment or establishmentMeans
			(d."col:degreeOfEstablishment" = 'native' OR d."col:establishmentMeans" = 'native') AS native_flag
		FROM col_distribution d LEFT JOIN col_area_lookup m ON m.area_raw = d."col:area"
		WHERE lower(d."col:gazetteer") in ('tdwg','iso','text');
		ALTER TABLE {target} ADD COLUMN IF NOT EXISTS native_to VARCHAR[];
		ALTER TABLE {target} ADD COLUMN IF NOT EXISTS regions VARCHAR[];
		-- native_to: only locations where native flag is set (~2.3k taxa)
		WITH native_habitats AS (
			SELECT id, list_distinct(array_agg(location)) AS locations
			FROM habitat_base WHERE native_flag AND location IS NOT NULL GROUP BY id
		)
		UPDATE {target} SET native_to = h.locations FROM native_habitats h WHERE {target}.id_raw = h.id;
		-- regions: all locations regardless of native status (~344k taxa)
		WITH region_habitats AS (
			SELECT id, list_distinct(array_agg(location)) AS locations
			FROM habitat_base WHERE location IS NOT NULL GROUP BY id
		)
		UPDATE {target} SET regions = h.locations FROM region_habitats h WHERE {target}.id_raw = h.id;
	""")

# Strip diacritics, punctuation, collapse whitespace for fuzzy WGSRPD name matching
def normalize_area_name(value: str) -> str:
	value = unicodedata.normalize('NFKD', value.strip().lower())
	value = ''.join(char for char in value if not unicodedata.combining(char))
	value = value.replace('&', ' and ')
	value = re.sub(r"[\.''`]+", '', value)
	return ' '.join(re.sub(r'[^a-z0-9]+', ' ', value).split())

# Build normalized reverse lookup from WGSRPD names to codes
# Prefers level-3 (country) matches over broader regions when names collide
def build_wgsrpd_name_lookup() -> dict:
	name_to_candidates = {}
	for key, data in WGSRPDLOOKUP.items():
		if not isinstance(data, dict) or not data.get('name'): continue
		normalized = normalize_area_name(str(data['name']))
		name_to_candidates.setdefault(normalized, []).append((int(data.get('level', 0)), str(key).lower()))
	# Prefer canonical country/region levels when names collide across levels
	level_priority = {3: 0, 2: 1, 4: 2, 1: 3}
	lookup = {}
	for normalized, candidates in name_to_candidates.items():
		candidates.sort(key=lambda item: (level_priority.get(item[0], 9), len(item[1])))
		lookup[normalized] = candidates[0][1]
	# Practical aliases from CoL free-text exports
	for alias, code in {
		'us': '7', 'usa': '7', 'uk': 'grb',
		'ivory coast': 'ivo', 'gambia the': 'gam', 'zaire': 'zai',
		'south africa': '27', 'rep south africa': '27',
		'bosnia herzegovina': 'yug-bh', 'antigua barbuda': 'lee-ab',
		'dr congo': 'zai', 'democratic republic of congo': 'zai',
		'north yemen': 'yem-ny', 'south yemen': 'yem-sy'
	}.items():
		lookup[normalize_area_name(alias)] = code
	return lookup

#
#			Reusable DuckDB queries we use in different importer pipelines
#			Generally should be called in the order they listed here, ie do name cleanup after having found hybrids and removed ranks/authors
#

from .log import mesologger
import os
import duckdb
from .. import settings, PROCESSED_DIR
# Load shared storage proxy for local/S3 transparent file operations
from ..utils.s3 import storage
# Load run-state helper to persist latest processed output metadata
from ..utils.state import update_source_state

# Filters most of the series, pages, year etc out of publications:
publication_filter = "'^(.*?)(?:\\s+\\d+(?:\\s*\\([^)]*\\))?(?:\\s*:|$))'"

# Try to identify hybrids and clean up name_clean if necessary
# Hybrid handling follows ICN nothotaxon conventions:
# https://www.iapt-taxon.org/nomen/pages/main/art_h8.html
# We normalize common hybrid markers, persist explicit hybrid flags, and remove × from canonical names.
def find_hybrids(db: duckdb.DuckDBPyConnection, source: dict):
	# Usual x / × misspelling
	rows = db.execute(f"UPDATE {source['name']} SET name_clean = replace(name_clean, ' x ', ' × ') WHERE name_clean LIKE '% x %' RETURNING 1").fetchdf()
	if len(rows) > 0: mesologger.info(f"Changed {len(rows):,} ' x ' to ' × ' in {source['name']}")
	# Always add column
	db.execute(f"""ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS hybrid BOOLEAN;""")
	# Check for x symbols first
	name = db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source['name']}' AND column_name = 'name_clean' ").fetchone() is not None
	if name: db.execute(f"""UPDATE {source['name']} SET hybrid = CONTAINS(name_clean, '×') WHERE COALESCE(hybrid, FALSE) = FALSE;""")
	# Check by name/rank
	rank = db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source['name']}' AND column_name = 'rank_raw' ").fetchone() is not None
	if rank:
		db.execute(f"""
			UPDATE {source['name']} SET hybrid = (starts_with(rank_raw, 'notho') OR rank_raw IN ('genushybrid', 'hybrid', 'infrahybrid','subhybr.','grex','[infragen.grex]','infragen.grex','grex_sect.'))
			WHERE COALESCE(hybrid, FALSE) = FALSE;
		""")
	count = db.execute(f"SELECT count(*) FROM {source['name']} WHERE hybrid").fetchone()[0]
	if count > 0: mesologger.info(f"Found {count:,} hybrids in {source['name']}")
	# Note position of ×
	db.execute(f"""
		ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS hybridpos UTINYINT;
		UPDATE {source['name']} SET hybridpos = NULLIF(position('×' in name_clean) - 1, -1) WHERE hybrid;
	""")	
	mesologger.info(F"Noted hybrid sign positions")
	# Remove × from name
	db.execute(f"UPDATE {source['name']} SET name_clean = replace(replace(name_clean,'× ',''),'×','')")	  
	mesologger.info(F"Removed hybrid sign from names")

# Lot of datasets have the author in the scientificName and separate, thus we remove it from name_clean
def strip_author_from_name(db: duckdb.DuckDBPyConnection, source: dict):
	rows = db.execute(f"""
		UPDATE {source['name']} SET name_clean = REGEXP_REPLACE(
		TRIM(REGEXP_REPLACE(lower(name_clean), REGEXP_ESCAPE(TRIM(lower(author_raw))), '','g')), '\\s+', ' ')
		WHERE LOWER(name_clean) LIKE '%' || LOWER(TRIM(author_raw)) || '%'
		RETURNING 1
	""").fetchdf()
	if len(rows) > 0: mesologger.info(f"Removed {len(rows):,} authors from name_clean in {source['name']}")

# Remove rank terms from name_clean to avoid duplication with rank field
def strip_rank_from_name(db: duckdb.DuckDBPyConnection, source: dict):
	rows = db.execute(f"""
		UPDATE {source['name']} SET name_clean = REGEXP_REPLACE(
		TRIM(REGEXP_REPLACE(lower(name_clean), REGEXP_ESCAPE(TRIM(lower(rank_raw))), '','g')), '\\s+', ' ')
		WHERE LOWER(name_clean) LIKE '%' || LOWER(TRIM(rank_raw)) || '%'
		RETURNING 1
		""").fetchdf()
	if len(rows) > 0: mesologger.info(f"Removed {len(rows):,} rank terms from name_clean in {source['name']}")

# Generic cleanup of misspellings etc in names
def name_cleanup(db: duckdb.DuckDBPyConnection, source: dict):
	# Other erroneous rank leftovers etc
	rows = db.execute(f"""
		UPDATE {source['name']}
			SET name_clean = regexp_replace(name_clean, ' (notho)?(ser|sect|subsect|subgen|sub|bb|ab|mut|var|subvar|convar|race|subsp|subspec|ssp|forma|form|subf|f|fo|fma|unr|lus|lusus|prol|proles|sp nov|sp\\. nov|f\\.sp\\.|\\[unranked\\])(\\.?)(\\s|$)', ' ', 'g')
    		WHERE regexp_matches(name_clean, ' (notho)?(ser|sect|subsect|subgen|sub|bb|ab|mut|var|subvar|convar|race|subsp|subspec|ssp|forma|form|subf|f|fo|fma|unr|lus|lusus|prol|proles|sp nov|sp\\. nov|f\\.sp\\.|\\[unranked\\])(\\.?)(\\s|$)')
		RETURNING 1;
	""").fetchdf()
	if len(rows) > 0: mesologger.info(f"Removed {len(rows):,} rank leftovers from {source['name']}")
	# Replace all quote characters quotes with single quotes (U+0027 : APOSTROPHE {single quote; APL quote})
	quote_char_pattern = """["ʽ‛‟′″‴’‘]"""
	rows = db.execute(f"""UPDATE {source['name']} SET name_clean = regexp_replace(name_clean, '{quote_char_pattern}', '''', 'g') WHERE regexp_matches(name_clean, '{quote_char_pattern}') RETURNING 1;""").fetchdf()
	if len(rows) > 0: mesologger.info(f"Replaced {len(rows):,} quote characters with single quotes in {source['name']}")
	# Check for and remove stray characters in name_clean (keeping spaces, quotes, simple dashes)
	stray_regex_pattern = """[^a-z -'']"""
	stray_chars = db.execute(f"""SELECT COUNT(*) FROM {source['name']} WHERE regexp_matches(name_clean, '{stray_regex_pattern}');""").fetchone()[0]
	if stray_chars > 0:
		db.execute(f"""UPDATE {source['name']} SET name_clean = regexp_replace(name_clean, '{stray_regex_pattern}', '', 'g') WHERE regexp_matches(name_clean, '{stray_regex_pattern}');""")
		mesologger.info(f"Cleaned characters that are not standard lowercase latin from {stray_chars:,} rows in {source['name']}")
	# Check and clean multiple spaces, leading/trailing spaces
	rows = db.execute(f"""
		UPDATE {source['name']} SET name_clean = trim(
			regexp_replace(
				regexp_replace(name_clean, ' {{2,}}', ' ', 'g'),  	-- replace all 2+ spaces with single space, globally
				'\t+', ' ', 'g')                              		-- replace any tabs with single space, globally
			)                                     					-- remove leading/trailing spaces
		WHERE regexp_matches(name_clean, ' {{2,}}|\t|^ | $')     	-- find entries with 2+ spaces, tabs, leading or trailing spaces
		RETURNING 1;
	""").fetchdf()
	if len(rows) > 0: mesologger.info(f"Cleaned {len(rows):,} entries with multiple/leading/trailing spaces in {source['name']}")
	delete_count = db.execute(f"""SELECT COUNT(*) FROM {source['name']} WHERE name_clean = '';""").fetchone()[0]
	if delete_count > 0: 
		db.execute(f"DELETE FROM {source['name']} WHERE name_clean = '';")
		mesologger.info(f"Deleted {delete_count:,} entries with empty {source['name']} name")

# Normalize taxon ranks and optional status flags into canonical canopy values
# Rank normalization follows ICN rank hierarchy and includes practical source-specific aliases.
# Cultivar/grex handling follows ICNCP guidance where source datasets provide those concepts.
# ICN: https://www.iapt-taxon.org/nomen/pages/main/art_4.html
# ICNCP: https://www.ishs.org/sites/default/files/static/ScriptaHorticulturae_18.pdf
def build_rank_and_status(db: duckdb.DuckDBPyConnection, source: dict):
	# Use full words first, trying to cover all spellings and variants we encounter throughout datasets
	# We also remove notho- prefixes for hybrids, as we track if something is a hybrid through a dedicated BOOL
	# db.sql(f"SELECT DISTINCT rank_raw, COUNT(rank_raw) FROM {source['name']} GROUP BY rank_raw ORDER BY COUNT(rank_raw) DESC").show(max_rows=30)
	db.execute(f"""
		ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS rank_clean VARCHAR;
		-- Lower only once
		WITH lowered AS ( SELECT id_raw, lower(rank_raw) as rank_raw_lower FROM {source['name']} )
		UPDATE {source['name']} AS t SET rank_clean = CASE
			-- ICN ranks:
			-- Don't use kingdom yet, eg Mycobank has couple off erroneus onsed
			WHEN l.rank_raw_lower IN ('kingdom','regn.','domain') THEN 'kingdom'
            WHEN l.rank_raw_lower IN ('subkingdom', 'subregn.') THEN 'subkingdom'
            WHEN l.rank_raw_lower IN ('division', 'div.','phylum','superphylum') THEN 'phylum'
            WHEN l.rank_raw_lower IN ('subdivision', 'subdiv.','subdivf.','subphylum','cohort') THEN 'subphylum'
            WHEN l.rank_raw_lower IN ('order', 'ordo','superorder') THEN 'order'
            WHEN l.rank_raw_lower IN ('suborder','subordo','infraorder','parvorder') THEN 'suborder'
            WHEN l.rank_raw_lower IN ('class', 'cl.','superclass') THEN 'class'
            WHEN l.rank_raw_lower IN ('subclass', 'subcl.','infraclass') THEN 'subclass'
            WHEN l.rank_raw_lower IN ('family', 'fam.','superfamily') THEN 'family'
            WHEN l.rank_raw_lower IN ('subfamily', 'subfam.') THEN 'subfamily'
            WHEN l.rank_raw_lower IN ('tribe', 'trib.','tr.','supertribe', 'supertrib.') THEN 'tribe'
            -- Including hybrid subtribes
            WHEN l.rank_raw_lower IN ('subtribe', 'subtr.','subtrib.','nothosubtrib.','supersubtribe', 'supersubtrib.') THEN 'subtribe'
            -- Including parent of hybrid
            WHEN l.rank_raw_lower IN ('genus', 'gen.','genitor') THEN 'genus'
            -- Including micro and hybrid divisions
            WHEN l.rank_raw_lower IN ('subgenus', 'subg.','subgen.', 'nothosubgen.','subgenitor','microgen.','microgène','microgene','genushybrid') THEN 'subgenus'
            -- Including hybrid and grex sections
            WHEN l.rank_raw_lower IN ('section', 'sect.', 'nothosect.','grex_sect.','section botany','supersection', 'supersect.','suprasect.') THEN 'section'
            -- Including hybrid subsections
            WHEN l.rank_raw_lower IN ('subsection', 'subsect.', 'nothosubsect.') THEN 'subsection'
            -- Including hybrid and generic series
            WHEN l.rank_raw_lower IN ('series', 'ser.', 'nothoser.','gen. ser.','superseries', 'superser.') THEN 'series'
            WHEN l.rank_raw_lower IN ('subseries', 'subser.') THEN 'subseries'
            -- Including asexual and pseudo species																Tropicos edge cases:
            WHEN l.rank_raw_lower IN ('species', 'spec.','agamosp.','psp.','sp.','nothosp.','sp','ichnospecies','aff.','cf.','mad.','s.') THEN 'species'
            -- All grex variations
            WHEN l.rank_raw_lower IN ('grex', '[infragen.grex]','infragen.grex','nothogrex') THEN 'grex'
            -- subspecies (plantae) in IUCN;
            -- All subspecies variations
            WHEN l.rank_raw_lower IN ('subspecies', 'subsp.', 'subspec.', 'ssp.','nothosubsp.','subspecioid','subspecies (plantae)','subsubsp.') THEN 'subspecies'
            -- * only happens once in IPNI, https://www.ipni.org/n/1007308-2 vs https://www.worldfloraonline.org/taxon/wfo-1200066315
            -- Including cultivated, provisional and asexual vars
            WHEN l.rank_raw_lower IN ('variety', 'var.', 'nothovar.','convariety','convar.','agamovar.','prol.', 'proles','provar.','[beta].','*','infrahybrid','hybrid','pseudovar.','var','varietas') THEN 'variety'
            -- Only 2 subsubvar and one subforma in all of IPNI
            -- Including lineages and races
            WHEN l.rank_raw_lower IN ('subvariety', 'subvar.','subsubvar.','stirps','linea','subhybr.','subproles','race') THEN 'subvariety'
            -- Including mutations and morphological variants, 'f.sp.' is common in Fungi
            WHEN l.rank_raw_lower IN ('form', 'f.', 'fo.','f.sp.', 'forma', 'nothof.','mut.','modif.','cycl.','ap.','oec.','nm.','monstr.','micromorphe', 'ecas.','mod.', 'forma specialis','morph') THEN 'form'
            -- Including micro forms
            WHEN l.rank_raw_lower IN ('subform', 'subf.','subfo.','subsubforma','microf.','subsubf.','strain') THEN 'subform'
            -- Ornamental horticulture
            WHEN l.rank_raw_lower IN ('lusus', 'lus.','sublus.','sublusus') THEN 'lusus'
			-- Cultivars
            WHEN l.rank_raw_lower IN ('cultivar','cult.') THEN 'cultivar'			
            -- Intentional grouping concepts
            WHEN l.rank_raw_lower IN ('complex', 'gruppe', 'group', 'agglom.', 'nid', 'species group', 'species subgroup','pathogroup') THEN 'complex'
            -- True unranked/unspecified ranks
            WHEN l.rank_raw_lower IN ('unranked', '[unranked]','[infragen.unranked]', '[infrasp.unranked]', '[infragen.]', '[infragen]', '[infrafam.unranked]', 'positio', '-', 'no rank') THEN 'unranked'
			ELSE NULL
		END FROM lowered l WHERE t.id_raw = l.id_raw;
	""")
	if settings.VERBOSE: db.sql(f"SELECT DISTINCT rank_clean, list_distinct(list(rank_raw)), COUNT(rank_clean) FROM {source['name']} GROUP BY rank_clean ORDER BY COUNT(rank_clean) DESC").show(max_rows=75)	
	# Also build status, if we have a column
	if db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source['name']}' AND column_name = 'status_raw'").fetchone() is not None:
		db.execute(f"""
			ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS status_clean VARCHAR;
			UPDATE {source['name']} AS s SET status_clean = CASE
				-- Any form of acceptance
				-- We also use name status for fungi
				WHEN l.status_lower IN ('accepted','provisionally accepted','legitimate') THEN 'accepted'
				-- Not meeting their taxonomic criteria, but still usefull in our case
				WHEN l.status_lower IN ('artificial hybrid','local biotype') THEN 'edgecase'
				-- Not checked
				WHEN l.status_lower IN ('unchecked','unplaced','uncertain') THEN 'unplaced'
				-- Def a synonym
				WHEN l.status_lower IN ('synonym','ambiguous synonym','heterotypic synonym','heterotypic_synonym','homotypic synonym','homotypic_synonym','proparte synonym') THEN 'synonym'
				-- Or an issue
				WHEN l.status_lower IN ('doubtful','illegitimate','invalid','orthographic','misapplied','orthographic variant','unavailable','deleted') THEN 'problematic'
				ELSE NULL
			END FROM (SELECT id_raw, LOWER(status_raw) as status_lower FROM {source['name']}) l 
			WHERE s.id_raw = l.id_raw;
		""")
		# Also set flags
		db.execute(f"""
			ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS synonym BOOLEAN;
			ALTER TABLE {source['name']} ADD COLUMN IF NOT EXISTS accepted BOOLEAN;
			UPDATE {source['name']} SET synonym = (status_clean = 'synonym'), accepted = (status_clean = 'accepted')
		""")
		count = db.execute(f"""SELECT sum(accepted), sum(synonym), COUNT(*) FILTER (WHERE status_clean = 'problematic'), COUNT(*) FILTER (WHERE status_clean = 'edgecase') FROM {source['name']}""").fetchone()
		if count: mesologger.info(f"{int(count[0] or 0):,} accepted, {int(count[1] or 0):,} synonymic, {int(count[2] or 0):,} problematic taxons in {source['name']}, with {int(count[3] or 0):,} edge-cases")
		if settings.VERBOSE: db.sql(f"SELECT DISTINCT status_clean, list_distinct(list(status_raw)), COUNT(status_clean) FROM {source['name']} GROUP BY status_clean ORDER BY COUNT(status_clean) DESC").show(max_rows=75)	

# See if we have any issues left
def validate(db: duckdb.DuckDBPyConnection, source: dict):
	# Check if we have any names with more than 3 words left
	columns = "id_raw, name_clean"
	# Add name_raw only if it exists
	has_name_raw = db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source['name']}' AND column_name = 'name_raw'").fetchone() is not None
	if has_name_raw: columns += ", name_raw"
	# Add author_raw only if it exists
	has_author = db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source['name']}' AND column_name = 'author_raw'").fetchone() is not None
	if has_author: columns += ", author_raw"
	# Add hybrid only if it exists
	hybrid = db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source['name']}' AND column_name = 'hybrid'").fetchone() is not None
	if hybrid: columns += ", hybrid"
	# Tropicos doesn't have any ranks
	ranks = db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source['name']}' AND column_name = 'rank_raw'").fetchone() is not None
	if ranks: columns += ", rank_raw, rank_clean"
	# See if it'd cause some issues
	accepted = db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source['name']}' AND column_name = 'accepted'").fetchone() is not None
	if accepted: columns += ", accepted"
	# Check for more than trinomial
	result = db.sql(f"""SELECT {columns} FROM {source['name']} WHERE array_length(regexp_split_to_array(trim(name_clean), '\\s+')) > 3 {" AND rank_clean != 'cultivar'" if ranks else ''};""")
	rows = result.fetchdf()
	if len(rows) > 0:
		mesologger.warning(f"{source['name']} still has {len(rows):,} non-trinomial rows (more than 3 words in name_clean):")
		result.show(max_rows=200)
	# Check for non-alphabetic characters in name_clean
	result = db.sql(f"SELECT {columns} FROM {source['name']} WHERE name_clean ~ '[^a-z -'']';")
	rows = result.fetchdf()
	if len(rows) > 0:
		mesologger.info(f"{source['name']} has {len(rows):,} rows with non-alphabetic characters in name_clean:")
		result.show(max_rows=200)
	# General overview
	if settings.VERBOSE: 
		db.sql(f"SUMMARIZE {source['name']}").show(max_rows=100)
		db.sql(f"SELECT * FROM {source['name']}").show(max_rows=40)

# Store final ouput
def write_to_disc(db: duckdb.DuckDBPyConnection, source: dict, dir = PROCESSED_DIR, filename = None):
	# Import here to prevent circular import
	from ..utils.filehandlers import delete_older_files
	# Set our default filename
	if not filename: filename = f"{ source['name'] }.{ source['timestamp_download'] }"
	# Get number of rows
	rows = db.sql(f"SELECT COUNT(*) FROM {source['name']}").fetchone()[0]
	# Build canonical output path for local or S3 parquet writes
	output_path = storage.parquet_url(os.path.join(dir, f"{ filename }.parquet"))
	# Configure DuckDB S3 settings when writing to object storage
	if storage.is_s3(): storage.configure_duckdb(db)
	# Write parquet via COPY for consistent local and S3 support
	db.execute(f"COPY {source['name']} TO '{output_path}' (FORMAT PARQUET)")
	mesologger.info(f"Wrote {rows:,} rows to parquet file {output_path}")
	# Write .csv as well if we have our flag set
	if settings.CSV: 
		db.sql(f"SELECT * FROM {source['name']}").write_csv(f"{ dir }/{ filename }.tsv",sep='\t') 
		mesologger.info(f"Wrote {rows:,} rows to tsv file { dir }/{ filename }.tsv")
	# If it's not a release candidate
	if source['name'] != 'meso':
		# Update latest processed once we wrote it to disc
		source['latest_processed'] = filename + '.parquet'
		# Parse and store processed timestamp when filename uses source.YYYYMMDD pattern
		try: source['timestamp_processed'] = int(filename.split('.')[1])
		except (ValueError, IndexError): pass
		# Persist latest processed metadata in shared state manifest
		update_source_state(source, 'process')
		# Delete older processed files
		delete_older_files(filename.split('.')[0],filename.split('.')[1],PROCESSED_DIR)

# Add normalized vernacular names from a ColDP VernacularName table
def coldp_vernacular(vernacular_tsv, db: duckdb.DuckDBPyConnection, source: dict):
	# Use the source name as the normalized table name
	target = source['name']
	# Add the standard canopy vernacular field
	db.execute(f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS vernacular VARCHAR[]")
	# Normalize language codes and aggregate names by usage
	db.execute(f"""
		WITH mapping_values AS (SELECT * FROM (VALUES {language_mappings}) AS t(lang3, lang2)),
		names AS (
			SELECT
				v."col:taxonID" AS id_raw,
				COALESCE(m.lang2, v."col:language") AS iso_lang,
				trim(lower(v."col:name")) AS name
			FROM vernacular_tsv v
			LEFT JOIN mapping_values m ON lower(v."col:language") = m.lang3
			WHERE v."col:name" IS NOT NULL
				AND length(trim(v."col:name")) > 0
				AND EXISTS (SELECT 1 FROM {target} t WHERE t.id_raw = v."col:taxonID")
		),
		aggregated AS (
			SELECT id_raw, array_agg(iso_lang || ':' || name) AS values
			FROM names
			WHERE iso_lang IS NOT NULL
			GROUP BY id_raw
		)
		UPDATE {target}
		SET vernacular = aggregated.values
		FROM aggregated
		WHERE {target}.id_raw = aggregated.id_raw;
	""")
	# Report vernacular coverage for this source
	count = db.execute(f"SELECT COUNT(*) FROM {target} WHERE vernacular IS NOT NULL AND len(vernacular) > 0").fetchone()[0]
	# Log the number of usages with vernacular values
	mesologger.info(f"Added vernacular names to {count:,} {target} entries")

# Default way we map more exotic or 3 letter iso codes, including local dialects like Bavarian to German etc (Wikipedia)
# Also reducing variants, eg en-gb becomes en and zh-hans becomes zh
language_mappings = f"""
    -- Major languages
    ('aar', 'aa'),  ('aka', 'ak'), ('aln', 'sq'), ('amh', 'am'), ('arg', 'an'), ('asm', 'as'), ('aym', 'ay'), ('bam', 'bm'),
    ('bih', 'bh'), ('bis', 'bi'), ('bod', 'bo'), ('bos', 'bs'), ('cha', 'ch'), ('cat', 'ca'), ('ces', 'cs'), ('cos', 'co'), ('cym', 'cy'),
    ('dzo', 'dz'),  ('eus', 'eu'), ('ewe', 'ee'), ('fij', 'fj'), ('gle', 'ga'), ('kat', 'ka'), ('glg', 'gl'),
    ('grn', 'gn'), ('guj', 'gu'), ('hau', 'ha'), ('heb', 'he'), ('hrv', 'hr'), ('hun', 'hu'), ('ibo', 'ig'), ('ido', 'io'), ('isl', 'is'),
     ('jav', 'jv'), ('kan', 'kn'), ('kas', 'ks'), ('khm', 'km'), ('kin', 'rw'), ('kon', 'kg'), ('lao', 'lo'), ('lat', 'la'),
    ('lav', 'lv'), ('lin', 'ln'), ('lit', 'lt'), ('lub', 'lu'), ('lug', 'lg'), ('mal', 'ml'), ('mar', 'mr'), ('mkd', 'mk'), ('mlg', 'mg'),
    ('mlt', 'mt'), ('mon', 'mn'), ('mri', 'mi'), ('nep', 'ne'), ('nno', 'nn'), ('ori', 'or'), ('orm', 'om'), ('pol', 'pl'), 
    ('run', 'rn'), ('sag', 'sg'), ('san', 'sa'), ('sin', 'si'), ('slk', 'sk'), ('slv', 'sl'), ('sna', 'sn'), ('snd', 'sd'), ('som', 'so'),
    ('sot', 'st'), ('sqi', 'sq'), ('srd', 'sc'), ('ssw', 'ss'), ('sun', 'su'), ('swa', 'sw'), ('tah', 'ty'), ('tam', 'ta'), ('tel', 'te'),
    ('ton', 'to'), ('tsn', 'tn'), ('tso', 'ts'), ('twi', 'tw'), ('ukr', 'uk'), ('urd', 'ur'), ('ven', 've'), ('vie', 'vi'), ('wol', 'wo'),
    ('xho', 'xh'), ('yor', 'yo'), ('zul', 'zu'), ('ben', 'bn'), ('bul', 'bg'),

    -- Legacy languages, eg Moldovan now Romanian
    ('mo', 'ro'),

	-- Abkhaz variants:
	('abk', 'ab'), ('ady', 'ab'),

	-- Afrikaans
	('afr', 'af'), ('kj', 'af'), ('ng', 'af'),

    -- Arabic variants:
    ('ara', 'ar'), ('arz', 'ar'), ('arq', 'ar'), ('ary', 'ar'), ('aeb-arab', 'ar'), ('aeb-latn', 'ar'), ('apc', 'ar'),
	('tru', 'ar'),

    -- Armenian variants:
    ('hye', 'hy'), ('hyw', 'hy'),

    -- Azerbaijani variants:
    ('aze', 'az'), ('azb', 'az'),

    -- Belarusian variants:
    ('bel', 'be'), ('be_x_old', 'be'), ('be-tarask', 'be'),

    -- Berber languages:
    ('zgh', 'ber'), ('tzm', 'ber'), ('rif', 'ber'),

    -- Chinese variants:
    ('zho', 'zh'), ('zh-hans', 'zh'), ('zh-cn', 'zh'), ('zh-tw', 'zh'), ('zh-hant', 'zh'), ('zh-hk', 'zh'), ('zh-sg', 'zh'), ('zh-my', 'zh'), ('zh-mo', 'zh'),
    ('zh_min_nan', 'zh'), ('zh_yue', 'zh'), ('zh_classical', 'zh'), ('cdo', 'zh'), ('gan', 'zh'), ('gan-hans', 'zh'), ('gan-hant', 'zh'), ('hak', 'zh'),
    ('lzh', 'zh'), ('nan', 'zh'), ('nan-hani', 'zh'), ('wuu', 'zh'), ('yue', 'zh'), ('nan-latn-pehoeji', 'zh'), ('nan-latn-tailo', 'zh'), ('nan-hant', 'zh'),
    ('ii', 'zh'),

    -- Constructed languages:
    ('epo', 'eo'), ('avk', 'eo'), ('tok', 'eo'),

	-- Danish variants:
	('dan', 'da'), ('jut', 'da'),

    -- Dutch variants:
    ('nld', 'nl'), ('nds-nl', 'nl'), ('nds_nl', 'nl'), ('zea', 'nl'),

    -- English variants:
    ('eng', 'en'), ('en-gb', 'en'), ('en-ca', 'en'), ('en-us', 'en'), ('en-au', 'en'), ('en-in', 'en'), ('en-nz', 'en'),
    ('tpi', 'en'), ('pcm', 'en'), ('jam', 'en'), ('pih', 'en'), ('gpe', 'en'), ('wes', 'en'),

	-- Estonian
	('est', 'et'), ('liv', 'et'),

    -- Filipino related languages:
    ('fil', 'tl'), ('tgl', 'tl'), ('krj', 'tl'),

    -- Finnish variants:
    ('fin', 'fi'), ('fit', 'fi'), ('vot', 'fi'),

    -- French variants:
    ('fra', 'fr'), ('fr-ca', 'fr'), ('gcr', 'fr'), ('frc', 'fr'), ('bsk', 'fr'), ('fmp', 'fr'),

    -- Fulah and other West African languages:
    ('ful', 'ff'), ('gur', 'ff'), ('kus', 'ff'), ('knc', 'ff'),

    -- Bantu languages (Central, East, Southern Africa):
    ('yao', 'bnt'), ('ybb', 'bnt'), ('ewo', 'bnt'), ('bas', 'bnt'), ('agq', 'bnt'), ('nmg', 'bnt'), ('bbj', 'bnt'), ('nnh', 'bnt'), ('byv', 'bnt'), ('mua', 'bnt'),
    ('dua', 'bnt'), ('yav', 'bnt'), ('bkm', 'bnt'), ('loz', 'bnt'), ('nd', 'bnt'), ('bax', 'bnt'), ('tiv', 'bnt'), ('nup', 'bnt'), ('lem', 'bnt'),
    ('bag', 'bnt'), ('bkh', 'bnt'), ('mcp', 'bnt'), ('bkc', 'bnt'), ('vut', 'bnt'), ('ker', 'bnt'), ('etu', 'bnt'), ('gya', 'bnt'), ('ann', 'bnt'), ('yat', 'bnt'),
    ('fat', 'bnt'), ('nla', 'bnt'), ('yas', 'bnt'), ('eto', 'bnt'), ('lns', 'bnt'), ('guw', 'bnt'), ('isu', 'bnt'), ('tvu', 'bnt'),

    -- German variants:
    ('deu', 'de'), ('de-ch', 'de'), ('de-at', 'de'), ('gsw', 'de'), ('bar', 'de'), ('als', 'de'), ('nds', 'de'), ('ksh', 'de'), ('vmf', 'de'),
    ('pdt', 'de'), ('sli', 'de'), ('sdc', 'de'), ('got', 'de'), ('prg', 'de'),

    -- Greek variants:
    ('ell', 'el'), ('pnt', 'el'),

    -- Hindi variants:
    ('hin', 'hi'), ('mai', 'hi'), ('bho', 'hi'), ('hno', 'hi'), ('lus', 'hi'), ('rwr', 'hi'), ('xnr', 'hi'), ('mag', 'hi'),

    -- Indigenous North American languages:
    ('arn', 'na'), ('cr', 'na'), ('den', 'na'), ('ess', 'na'), ('ojb', 'na'), ('cho', 'na'), ('mus', 'na'), ('oj', 'na'),

	-- Inuit / Inuktitut
	('iku', 'iu'), ('ike', 'iu'),

    -- Indonesian related languages:
    ('ind', 'id'), ('bug', 'id'), ('tet', 'id'), ('btm', 'id'),

    -- Iranian languages:
    ('fas', 'fa'), ('lrc', 'fa'), ('bal', 'fa'), ('bgn', 'fa'), ('brh', 'fa'),

    -- Italian variants:
    ('ita', 'it'), ('roa-tara', 'it'), ('roa_tara', 'it'), ('egl', 'it'), ('rgn', 'it'),

    -- Japanese variants:
    ('jpn', 'ja'), ('ja-hani', 'ja'), ('ja-kana', 'ja'), ('ja-hrkt', 'ja'),

    -- Kazakh variants:
    ('kaz', 'kk'), ('kk-cyrl', 'kk'), ('kk-latn', 'kk'), ('kk-arab', 'kk'), ('kk-tr', 'kk'), ('kk-cn', 'kk'), ('kk-kz', 'kk'),

    -- Korean variants:
    ('kor', 'ko'), ('ko-kp', 'ko'),

    -- Kurdish variants:
    ('kur', 'ku'), ('ku-latn', 'ku'), ('ku-arab', 'ku'), ('ckb', 'ku'),

    -- Malay and Burmese variants:
    ('msa', 'ms'), ('mya', 'my'), ('ms-arab', 'ms'), ('rki', 'my'), ('blk', 'my'),

    -- Marshallese:
    ('mah', 'mh'),

    -- Norwegian variants:
    ('nor', 'no'),

    -- Pacific island languages:
    ('smo', 'pc'), ('niu', 'pc'), ('gil', 'pc'), ('tvl', 'pc'), ('wls', 'pc'), ('ho', 'pc'), ('mh', 'pc'),

    -- Portuguese variants:
    ('por', 'pt'), ('pt-br', 'pt'), ('yrl', 'pt'),

    -- Punjabi variants:
    ('pan', 'pa'), ('pnb', 'pa'),

	-- Quechua
	('qug', 'qu'), ('que', 'qu'),

    -- Romanian variants:
    ('ron', 'ro'), ('ruq', 'ro'), ('ruq-latn', 'ro'),

    -- Russian/Slavic variants:
    ('rus', 'ru'), ('cu', 'ru'), ('ltg', 'ru'), ('sty', 'ru'),

    -- Serbian variants:
    ('srp', 'sr'), ('sr-el', 'sr'), ('sr-ec', 'sr'), ('sr-latn', 'sr'), ('sr-cyrl', 'sr'), ('sh-latn', 'sr'), ('sh-cyrl', 'sr'),

    -- Spanish variants:
    ('spa', 'es'), ('es-formal', 'es'), ('es-419', 'es'), ('es-mx', 'es'),
    ('cbk-zam', 'es'), ('sei', 'es'), ('arw', 'es'),

    -- Swedish variants:
    ('swe', 'sv'), ('sje', 'se'), ('sju', 'se'),

    -- Tajik variants:
    ('tgk', 'tg'), ('tg-latn', 'tg'), ('tg-cyrl', 'tg'),

    -- Tatar variants:
    ('tat', 'tt'), ('tt-cyrl', 'tt'), ('tt-latn', 'tt'),

    -- Thai related languages:
    ('tha', 'th'), ('shn', 'th'),

    -- Tigrinya related languages:
    ('tir', 'ti'), ('tig', 'ti'),

    -- Turkish and Turkic languages:
    ('tur', 'tr'), ('alt', 'tr'), ('ota', 'tr'), ('gag', 'tr'), ('lzz', 'tr'),

    -- Uyghur variants:
    ('uig', 'ug'), ('ug-latn', 'ug'),

    -- Uzbek variants:
    ('uzb', 'uz'), ('uz-latn', 'uz'),

    -- Three letter codes:
    ('crh-latn', 'crh'), ('crh-ro', 'crh'), ('map-bms', 'bms'), ('map_bms', 'bms'), ('pap-aw', 'pap'), ('roa_rup', 'rup'), ('bat_smg', 'sgs'),
    ('fiu_vro', 'vro'),  ('xnr-takr', 'xnr'), ('ike-latn', 'ike'), ('ban-bali', 'ban'), ('xnr-deva', 'xnr'),
    ('pi-sidd', 'pi'), ('gom-latn', 'gom'), ('gom-deva', 'gom'), ('hif-latn', 'hif'), ('shi-latn', 'shi')
"""


"""
	TODOs/unmapped:
	ace, alt, ang, anp, arc, arn, atj, awa, ban, bcl, bjn, bpy, bug, bxr, chy, csb, dag, diq, dsb, eml, ext,  frp, frr, 
	fur, gcr, glk, gom, got, grc, hil, hsb, ilo, inh, kaa, kab, kbd, koi, krc, lad, lbe, lez, lfn, lij, lld, lmo, mad, mdf, mhr, min, 
	mni, mrj, mul, mwl, myv, mzn, nah, nap, new, nov, nrm, nys, olo, pag, pam, pcd, pdc, pfl, pms, rsk, rue, sah, sat, scn, sco, sgs, sma, smj, 
	smn, sms, stq, szl, tiv, tly, tyv, udm, vec, vep, vls, vmf, vro, xal, yao, zea, zgh, xmf, haw, szy, tcy, pwn, bto, mnw, kge, tay, trv, chr, dtp, 
	shi, guc, nia, dty, nso, hif, kcg, iba, ami, kbp, srn, din, avk, ryu, bew, gor, dga, mos, bdr, sjd, mcn, syl, fon, nqo, jam, jbo, tum, skr, bbc, 
	tpi, shn, krj, rki, rmy, ltg, tdd, pcm, blk, sje, gag, rmf, ann, ybb, tet, bas, mo, agq, sju, fmp, pih, btm, frc, knc, lrc, tok, fit, ady, prg, 
	bag, den, mcp, sli, nla, tvu, lns, lem, yas, dua, yav, yat, wls, byv, mua, wes, bgn, bbj, isu, jut, pdt, vut, bkc, ker, rgn, nmg, brh, kus, pnt, 
	ewo, etu, gya, bkm, eto, bkh, nnh, loz, apc, bal, sdc, ess, lus, gpe, niu, bsk, lzz, sei, guw, mag, ruq, tzm, vot, fat, sty, gil, rwr, mus, 
	nup, liv,  bax, cho, egl, aln, gur, tru, arw, qug, ojb, hno, ota, tig, rif, tvl, yrl

"""
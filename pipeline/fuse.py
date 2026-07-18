#
#
#			Merge the various datasets after we processed them
#
#			FLOW:
#			- We first build a solid ICN name foundation with IPNI, Fungorum, TROPICOS and TODO AlgaeBase as foundation
#			- We then enrich them with corresponding human reviewed extensions like WCVP
# 			- We then compare and augment with other aggregated name datasets like CoL
#			- Next we add encyclopedic extensions like EoL
#			- And finally we add special interest datasets like IUCN, indigenous use etc
#
#	 		DONE: Add accepted flag/array to check if at least one / which taxonomic authorities recognize this name
#			TODO: Handle outdated genus like asclepiadaceae
#			TODO: order (cupressales │ pinales | pinales) class (equisetopsida │ magnoliopsida │ equisetopsida) and phylum (streptophyta │ tracheophyta │ streptophyta) all seem to have one mismatch each

from ..utils.log import mesologger
import os

# Settings
from .. import PROCESSED_DIR, RELEASES_DIR, TMP_DIR, settings
from ..utils.filehandlers import check_release, get_latest_processed
from ..utils.queries import write_to_disc
# Load shared storage proxy for local/S3 transparent file operations
from ..utils.s3 import storage
# Load shared state manifest reader for release-manifest source metadata merge
from ..utils.state import load_state
# DB
import duckdb
# UDFs
import uuid
import pyarrow as pa
import polars as pl

# Main kingdoms (used if we execute steps per kingdom instead of overall data)
kingdoms = ['plantae','fungi']
# Who we trust with names etc, ordinality is a tie breaker
core_authorities = ['ipni','wcvp','powo','wfo','col','tropicos','fungorum','mycobank','wikidata','inaturalist','gbif']
# Other IDs we add 
additional_ids = [
	'ncbi_id', 'eol_id', 'itis_id', 'iucn_id', 'usda_id', 'grin_id', 'bold_id', 'eppo_id', 'plantfinder_id', 
	'plantlist_id', 'pfaf_id', 'rhs_id', 'fna_id', 'foc_id', 'apni_id', 'nzor_id', 'otol_id'
]
# IDs we create as UINTEGER instead of VARCHAR
int_ids = [
	# Core
	'fungorum_id', 'wcvp_id', 'tropicos_id', 'mycobank_id', 'inaturalist_id', 'gbif_id',
	# Additional
	'ncbi_id', 'eol_id',  'itis_id', 'iucn_id', 'bold_id', 'plantfinder_id', 'fna_id', 'foc_id','otol_id',
	'rhs_id', 'apni_id'
]
# Flags we support
wikidata_flags = ['edible', 'toxic', 'herb', 'useful', 'annual', 'perennial', 'medicinal', 'psychoactive', 'vegetable', 'fruit']
# Votes we held collected throughout the process and then used in dedupe
votes_held = []
# Higher ranks we want to add to add
higher_ranks = ['species','genus','family','"order"','class','phylum']

# Execute full fusion flow from processed source parquets into one canopy release
# The flow runs in three phases: map, enrich, then reduce.
def fuse(results):
	# Announce fusion stage start
	mesologger.info(f"############### Fusing processed results ###############")
	# Fall back to latest processed artifacts when caller skipped process step
	if not results:
		# Log fallback behavior for operator visibility
		mesologger.warning(f"No processed file list provided, falling back on latest processed files")
		# Load latest processed source inventory from disk
		results = get_latest_processed()
	# Keep one in-memory DuckDB connection for full fusion flow
	with duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Configure DuckDB S3 settings when reading/writing S3 parquet files
		if storage.is_s3(): storage.configure_duckdb(db)
		# MAP phase: maximize name coverage and cross-source identifier linking
		mesologger.info(f"############### Building Initial Name Index ###############")
		# Load data
		load_map_sources(results, db)
		# Start with complete list of proper names
		initial_backbone(results, db)
		# Add Wikidata and other IDs so we have more consensus candidates
		add_ids(results, db) 
		# Consolidate names, authorships, years of publication, and ranks
		basic_consensus(results, db)
		# Create a hash for every entry we want to use
		create_hashes(results, db)
		# Optionally inspect mapped backbone table statistics
		if settings.VERBOSE: db.sql("SUMMARIZE meso;").show()
		# ENRICH phase: add non-core attributes like vernacular, status flags, and external context
		mesologger.info(f"############### Enriching Data ###############")
		# Load extra tables we might need in vernacular, enrich etc
		load_enrich_sources(results, db)
		# Add more data from IUCN, Wikidata etc
		enrich(results, db)
		# Normalize all vernacular names we have
		reduce_vernacular(results, db)
		# Optionally inspect enriched backbone table statistics
		if settings.VERBOSE: db.sql("SUMMARIZE meso;").show()
		# REDUCE phase: collapse to accepted taxa and stable parent hierarchy
		mesologger.info(f"############### Reducing Backbone ###############")
		# Pre-create higher rank columns as NULL so dedupe_names can MODE-aggregate them safely
		# add_higher_ranks fills them later when it walks the parent tree
		for rank in higher_ranks:
			db.execute(f"ALTER TABLE meso ADD COLUMN IF NOT EXISTS {rank} VARCHAR;")
		# Decide acceptance first from direct authority votes at all ranks
		decide_acceptance(results, db)
		# Enforce one accepted row per name on the directly-accepted set
		dedupe_names(results,db)
		# Build parent links and higher ranks over the established accepted set
		add_higher_ranks(results, db)
		# Final parent hygiene now that the tree is built over accepted rows
		consolidate_parents(results, db)
		# Add any potential GBIF, NCBI etc name matches, fix dangling parents
		polish(results, db)
		# Final check
		validate(results, db)
		# Optionally inspect final reduced backbone statistics
		if settings.VERBOSE: db.sql(f"SUMMARIZE meso;").show(max_rows=60)
		# Package final fused release and manifest into canopy releases dir
		release = package_release(results, db)
		# Return release metadata to caller
		return release

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 	
#
#		Short tasks on top for improved readibility, in their chronological order
#
# Everything we need for hashing and all entries, even if they're just synonyms and don't get their own page
def load_map_sources(results: dict, db: duckdb.DuckDBPyConnection):
	# CoL (plants and fungi) and Tropicos (name matching only) should come last
	for source in core_authorities: load_parquet(results,db,source)
	# Fetch taxon rank Enum from canopy config
	from ..config.enums import TaxonRank
	# Create enum for ranks
	db.execute(f"""CREATE TYPE taxon_rank_enum AS ENUM ('{"', '".join([e.name for e in TaxonRank])}')""")
	# Create enum for sources
	db.execute(f"""CREATE TYPE source_enum AS ENUM ('{"', '".join(core_authorities)}')""")
	mesologger.info(f"Created taxon_rank_enum and source_enum")

def load_enrich_sources(results: dict, db: duckdb.DuckDBPyConnection):
	# Extra source for enrichment
	for source in ['iucn','ncbi']: load_parquet(results,db,source)

def basic_consensus(results: dict, db: duckdb.DuckDBPyConnection):
	mesologger.info(f"############### Creating basic consensus ###############")
	# Core votes
	vote(results,db,'name_clean')
	vote(results,db,'rank_clean',[a for a in core_authorities if a != 'gbif'])
	vote(results,db,'author_raw')
	vote(results,db,'year')

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#
#		Tasks in their chronological order, often used utility functions at the end
#
# Build initial backbone from trusted core naming authorities before enrichment and reduction
# We trust the following sources for naming backbone input: IPNI, Fungorum, Mycobank, WCVP, POWO, CoL, and Tropicos.
# POWO and WCVP use IPNI LSID-style identifiers; names that are not reconciled against IPNI can appear with temporary '-4' IDs.
# Context: https://powo.science.kew.org/about
# The importer intentionally backfills from WCVP because IPNI can reference some names as synonyms without providing a primary row.
def initial_backbone(results: dict, db: duckdb.DuckDBPyConnection):
	# Create our initial table
	db.execute(f"""
		CREATE TABLE meso AS
		SELECT
			CAST('ipni' AS source_enum) AS source,
			id_raw AS ipni_id,
			CAST(NULL AS UINTEGER) AS fungorum_id,
			name_clean,
			'plantae' AS kingdom,
		FROM ipni
		UNION ALL
		SELECT
			CAST('fungorum' AS source_enum) AS source,
			NULL AS ipni_id,
			id_raw AS fungorum_id,
			name_clean,
			COALESCE(kingdom, 'fungi') AS kingdom,	
		FROM fungorum
		{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
	""")
	mesologger.info(f"""Added {db.execute("SELECT COUNT(*) FROM meso WHERE source = 'ipni'").fetchone()[0]:,} plant names from IPNI""")
	mesologger.info(f"""Added {db.execute("SELECT COUNT(*) FROM meso WHERE source = 'fungorum'").fetchone()[0]:,} fungi names from Fungorum""")

	# Add WCVP plants that are not (yet) in IPNI
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS wcvp_id UINTEGER;
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS powo_id VARCHAR;
		-- Insert new rows from wcvp that don't exist in meso
		INSERT INTO meso BY NAME
		SELECT
			CAST('wcvp' AS source_enum) AS source,
			w.ipni_id,
			w.name_clean,
			'plantae' AS kingdom,
			w.id_raw AS wcvp_id,
			w.powo_id
		FROM wcvp w
		WHERE (w.ipni_id IS NULL OR NOT EXISTS (SELECT 1 FROM meso m WHERE m.ipni_id IS NOT NULL AND m.ipni_id = w.ipni_id))
		AND NOT EXISTS (SELECT 1 FROM meso m WHERE m.name_clean = w.name_clean)
		{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
	""")
	mesologger.info(f"""Added {db.execute("SELECT COUNT(*) FROM meso WHERE source = 'wcvp'").fetchone()[0]:,} plant names from WCVP""")
	# Also add WCVP IDs to existing IPNI rows
	db.execute("""
		WITH wcvp_lookup AS (SELECT DISTINCT id_raw, powo_id FROM wcvp WHERE powo_id IS NOT NULL)
		UPDATE meso m SET wcvp_id = w.id_raw
		FROM wcvp_lookup w WHERE m.wcvp_id IS NULL AND m.ipni_id = w.powo_id;
	""")
	mesologger.info(f"Added WCVP IDs to existing IPNI rows")

	# Add POWO plants that are neither in IPNI nor WCVP
	db.execute(f"""
		-- Insert new rows from powo that don't exist in meso
		INSERT INTO meso BY NAME
		SELECT
			CAST('powo' AS source_enum) AS source,
			p.name_clean,
			COALESCE(p.kingdom, 'plantae') AS kingdom,
			p.id_raw as powo_id,
			p.wcvp_id
		FROM powo p
		WHERE 
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.powo_id IS NOT NULL AND m.powo_id = p.id_raw) AND 
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.wcvp_id IS NOT NULL AND m.wcvp_id = p.wcvp_id) AND 
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.name_clean = p.name_clean)
		{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
	""")
	mesologger.info(f"""Added {db.execute("SELECT COUNT(*) FROM meso WHERE source = 'powo'").fetchone()[0]:,} plant names from POWO""")
	# Also add WCVP & POWO IDs to existing rows
	# This order is weirdly much faster than any CTE 
	db.execute("""UPDATE meso m SET wcvp_id = p.wcvp_id FROM powo p WHERE m.wcvp_id IS NULL AND m.powo_id = p.id_raw;""")
	db.execute("""UPDATE meso m SET powo_id = p.id_raw FROM powo p WHERE p.wcvp_id IS NOT NULL AND m.wcvp_id = p.wcvp_id;""")
	mesologger.info(f"Added POWO IDs to existing IPNI/WCVP rows")

	# Start adding WFO
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS wfo_id VARCHAR;
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS tropicos_id UINTEGER;
		-- Insert new rows from wfo that don't exist in meso
		INSERT INTO meso BY NAME
		SELECT
			CAST('wfo' AS source_enum) AS source,
			w.name_clean,
			'plantae' AS kingdom,
			w.tropicos_id,
			w.ipni_id,
			w.id_raw AS wfo_id
		FROM wfo w
		WHERE 
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.ipni_id IS NOT NULL AND m.ipni_id = w.ipni_id) AND 
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.name_clean = w.name_clean)
		{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
	""")
	mesologger.info(f"""Added {db.execute("SELECT COUNT(*) FROM meso WHERE source = 'wfo'").fetchone()[0]:,} plant names from WFO""")	
	# Also add WFO IDs to existing WCVP rows
	db.execute("""
		WITH wfo_candidates AS (
			-- Score exact agreement with the IPNI nomenclatural row
			SELECT w.*, CASE WHEN EXISTS (SELECT 1 FROM ipni i WHERE i.id_raw = w.ipni_id AND i.name_clean = w.name_clean) THEN 1 ELSE 0 END
				-- Add agreement with the POWO treatment
				+ CASE WHEN EXISTS (SELECT 1 FROM powo p WHERE p.id_raw = w.ipni_id AND p.name_clean = w.name_clean) THEN 1 ELSE 0 END
				-- Add agreement with the linked WCVP treatment
				+ CASE WHEN EXISTS (SELECT 1 FROM wcvp v WHERE v.powo_id = w.ipni_id AND v.name_clean = w.name_clean) THEN 1 ELSE 0 END AS name_support
			-- Restrict scoring to WFO rows eligible for the IPNI backfill
			FROM wfo w WHERE w.ipni_id IS NOT NULL
		),
		wfo_lookup AS (
			-- Reduce duplicate IPNI mappings to one active WFO concept before UPDATE FROM
			SELECT DISTINCT ON (ipni_id) id_raw, ipni_id, tropicos_id FROM wfo_candidates
			-- Prefer WFO status and graph evidence before cross-source agreement and stable ID
			ORDER BY ipni_id, CASE status_clean WHEN 'accepted' THEN 0 WHEN 'synonym' THEN 1 ELSE 2 END, backlinks DESC, name_support DESC, id_raw
		)
		-- Attach the selected WFO concept and its compatible Tropicos link
		UPDATE meso m SET wfo_id = w.id_raw, tropicos_id = w.tropicos_id
		-- Update only unlinked plant backbone rows carrying the shared IPNI identifier
		FROM wfo_lookup w WHERE m.wfo_id IS NULL AND m.source NOT IN ('wfo','fungorum') AND m.ipni_id = w.ipni_id;
	""")
	mesologger.info(f"Added WFO IDs to existing IPNI/WCVP/POWO rows")

	# Add MycoBank becore CoL, as CoL has both plants and Fungi
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS mycobank_id UINTEGER;
		-- Insert new rows from wfo that don't exist in meso
		INSERT INTO meso BY NAME
		SELECT
			CAST('mycobank' AS source_enum) AS source,
			mb.name_clean,
			'fungi' AS kingdom,
			mb.fungorum_id AS fungorum_id,
			-- The ID raw in the spreadsheet doesn't seem to be what they are using
			mb.fungorum_id AS mycobank_id
		FROM mycobank mb
		WHERE 
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.fungorum_id IS NOT NULL AND m.fungorum_id = mb.fungorum_id) AND 
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.name_clean = mb.name_clean)
		{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
	""")
	mesologger.info(f"""Added {db.execute("SELECT COUNT(*) FROM meso WHERE source = 'mycobank'").fetchone()[0]:,} fungi names from MycoBank""")	
	# Also add IDs to existing Fungorum rows
	db.execute("""
		WITH mb_lookup AS (SELECT DISTINCT id_raw, fungorum_id FROM mycobank WHERE fungorum_id IS NOT NULL)
		UPDATE meso m SET mycobank_id = mb.fungorum_id
		FROM mb_lookup mb WHERE m.wfo_id IS NULL AND m.source != 'mycobank' AND m.fungorum_id = mb.fungorum_id;
	""")
	mesologger.info(f"Added MycoBank IDs to existing Fungorum rows")

	# Add CoL / World of Plants
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS col_id VARCHAR;
		-- Insert new rows from wfo that don't exist in meso
		INSERT INTO meso BY NAME
		SELECT
			CAST('col' AS source_enum) AS source,
			c.name_clean,
			c.kingdom,
			c.id_raw AS col_id,
			c.fungorum_id, 
			c.powo_id, 
			c.wfo_id, 
			c.tropicos_id
		FROM col c
		WHERE 
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.fungorum_id IS NOT NULL AND m.fungorum_id = c.fungorum_id) AND
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.powo_id IS NOT NULL AND m.powo_id = c.powo_id) AND
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.wfo_id IS NOT NULL AND m.wfo_id = c.wfo_id) AND
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.tropicos_id IS NOT NULL AND m.tropicos_id = c.tropicos_id) AND
			NOT EXISTS (SELECT 1 FROM meso m WHERE m.name_clean = c.name_clean)
		{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
	""")
	mesologger.info(f"""Added {db.execute("SELECT COUNT(*) FROM meso WHERE source = 'col'").fetchone()[0]:,} plant & Fungi names from CoL""")	
	# Also add CoL IDs to existing rows, one by one is fastest
	for extra_col_id in ['wfo_id', 'powo_id','fungorum_id','tropicos_id']:
		db.execute(f"""
			WITH col_lookup AS (SELECT DISTINCT id_raw, fungorum_id, powo_id, wfo_id, tropicos_id FROM col WHERE {extra_col_id} IS NOT NULL)
			UPDATE meso m SET 
				col_id = c.id_raw,
				fungorum_id = COALESCE(m.fungorum_id, c.fungorum_id),
				powo_id = COALESCE(m.powo_id, c.powo_id),
				wfo_id = COALESCE(m.wfo_id, c.wfo_id),
				tropicos_id = COALESCE(m.tropicos_id, c.tropicos_id)
			FROM col_lookup c WHERE m.col_id IS NULL AND m.{extra_col_id} = c.{extra_col_id};
		""")
	mesologger.info(f"Added CoL IDs to existing IPNI/Fungorum/WCVP/POWO/WFO rows")

	# Insert unique Tropicos names into the meso table
	db.execute(f"""
		INSERT INTO meso BY NAME
		SELECT 
			CAST('tropicos' AS source_enum) AS source,
			CAST(id_raw AS UINTEGER) AS tropicos_id,
			name_clean,
			COALESCE(kingdom,'plantae') AS kingdom,
		FROM tropicos WHERE name_clean NOT IN (SELECT name_clean FROM meso)
		{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
	""")
	mesologger.info(f"""Added {db.execute("SELECT COUNT(*) FROM meso WHERE source = 'tropicos'").fetchone()[0]:,} plant names from Tropicos""")
	# Also add Tropicos IDs to existing WCVP rows
	# db.execute("""
	#	WITH tropicos_lookup AS (SELECT DISTINCT id_raw, name_clean, rank_clean FROM tropicos)
	#	UPDATE meso m SET tropicos_id = t.id_raw
	#	-- We only can match Tropicos based on name and rank, let's check how that goes
	#	FROM tropicos_lookup t WHERE m.tropicos_id IS NULL AND m.source NOT IN ('tropicos','fungorum','mycobank') AND m.name_clean = t.name_clean AND m.rank_clean = t.rank_clean;
	#""")
	#mesologger.info(f"Added Tropicos IDs to existing IPNI/WCVP/POWO/WFO/CoL rows")
	# Log
	mesologger.info(f"Added {db.table('meso').shape[0]:,} names to meso")
	if settings.VERBOSE: db.sql(f"SUMMARIZE meso").show(max_rows=20)	

# IDs to make future lookups easier while we don't have a unique id yet
def add_ids(results: dict, db: duckdb.DuckDBPyConnection):		
	mesologger.info(f"############### Adding Dataset ID's ###############")
	# db.execute(f"CREATE TEMP TABLE wikidata_ids AS SELECT id_raw, ipni_id, fungorum_id, powo_id, wfo_id, tropicos_id FROM wikidata")
	if settings.VERBOSE: db.sql(f"SUMMARIZE wikidata").show(max_rows=60)
	# Make sure we have the wikidata column
	db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS wikidata_id VARCHAR;""")
	# Add ncbi for protein lookups etc
	id_authorities = core_authorities + ['ncbi']
	# Do this in batches to avoid ORs
	for source in id_authorities: 
		id = f"{source}_id"
		# Make sure we have all other needed columns
		db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS {id} {'UINTEGER' if id in int_ids else 'VARCHAR'};""")
		# Wikidata doesn't have wcvp, and don't self reference
		if source in ['wcvp','wikidata']: continue
		# Add Wikidata ID with deterministic tie-breaking when one authority ID maps to multiple Wikidata rows
		db.execute(f"""
			WITH wikidata_matches AS (
				-- Pick exactly one Wikidata row per meso row so UPDATE FROM cannot choose an arbitrary match
				SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, w.id_raw AS wikidata_id
				-- Match through the current authority ID because Wikidata links many external identifiers
				FROM meso m
				-- Keep this as an equality join so DuckDB can use a hash join instead of row-wise lookups
				JOIN wikidata w ON w.{id} IS NOT NULL AND m.{id} = w.{id}
				-- Only assign rows that have not already been matched by a higher-priority authority
				WHERE m.wikidata_id IS NULL
				-- Keep rows grouped by target row for DISTINCT ON
				ORDER BY m.rowid,
					-- Prefer Wikidata names that agree with the Meso name to avoid synonym/accepted cross-fills
					CASE WHEN w.name_clean = m.name_clean THEN 0 ELSE 1 END,
					-- Prefer better-supported Wikidata entries when several same-name candidates exist
					COALESCE(w.page_count, 0) DESC,
					-- Finish with stable QID ordering so identical inputs produce identical outputs
					w.id_raw
			)
			-- Apply the pre-ranked mapping with one source row per target row
			UPDATE meso SET wikidata_id = wm.wikidata_id
			FROM wikidata_matches wm WHERE meso.rowid = wm.meso_rowid;
		""")
	initial_count = db.execute("SELECT COUNT(wikidata_id) FROM meso").fetchone()[0]
	mesologger.info(f"""Added {initial_count:,} Wikidata IDs via core authority IDs""")
	# Try backfilling rest via name, eg Angiosperms have no core IDs
	db.execute(f"""
		WITH wikidata_name_matches AS (
			-- Pick exactly one same-name Wikidata row per meso row for deterministic name fallback
			SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, w.id_raw AS wikidata_id
			-- Restrict this fallback to exact cleaned names to avoid broad Wikidata cross-fills
			FROM meso m
			-- Match names directly after authority-ID matching has had first chance
			JOIN wikidata w ON m.name_clean = w.name_clean
			-- Avoid reusing a Wikidata row that was already assigned to another meso row
			WHERE m.wikidata_id IS NULL AND NOT EXISTS (SELECT 1 FROM meso used WHERE used.wikidata_id = w.id_raw)
			-- Prefer better-supported entries and then stable QID ordering among same-name candidates
			ORDER BY m.rowid, COALESCE(w.page_count, 0) DESC, w.id_raw
		)
		-- Apply the ranked same-name Wikidata fallback
		UPDATE meso SET wikidata_id = wm.wikidata_id
		FROM wikidata_name_matches wm WHERE meso.rowid = wm.meso_rowid;
	""")	
	mesologger.info(f"Added {int(db.execute("SELECT COUNT(wikidata_id) FROM meso").fetchone()[0]-initial_count):,} more Wikidata IDs via name match")
	# See if we have any IDs that we didn't correlate when building the initial backbone - keep seperate from loop above as coalesce() would otherwise slow both down
	db.execute(f"""
		UPDATE meso m SET {','.join([f'{authority}_id = COALESCE(m.{authority}_id, w.{authority}_id{("::UINTEGER" if str(authority + "_id") in int_ids else "")})' for authority in id_authorities if authority not in ('wikidata', 'wcvp')])}
		FROM wikidata w WHERE m.wikidata_id = w.id_raw;			
	""")
	mesologger.info(f"Supplemented existing sources with IDs from Wikidata")
	# Also try backfilling based on name from each source dataset
	for authority in id_authorities:
		# Don't do Wikidata again, and we can also be sure to have 100% of ipni and fungorum when we started the table
		if authority in ['wikidata','ipni','fungorum','ncbi']: continue
		before = db.execute(f"SELECT COUNT({authority}_id) FROM meso").fetchone()[0]
		# Prefer accepted GBIF rows when exact-name matching has multiple source candidates
		if authority == 'gbif': authority_order = "CASE WHEN a.status_clean = 'accepted' THEN 0 WHEN a.status_clean = 'synonym' THEN 1 ELSE 2 END, a.id_raw::VARCHAR"
		# Otherwise use stable source ID ordering so DuckDB never chooses an arbitrary update row
		else: authority_order = "a.id_raw::VARCHAR"
		db.execute(f"""
			WITH single_matches AS (
				-- Pick exactly one source ID per target row so exact-name fallback is deterministic
				SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, a.id_raw AS auth_id
				-- Scan the authority table once and join by cleaned name
				FROM {authority} a
				JOIN (
					-- For each name_clean, get exactly one meso row deterministically
					SELECT DISTINCT ON (name_clean) rowid, name_clean FROM meso WHERE {authority}_id IS NULL ORDER BY name_clean, rowid
				) m ON m.name_clean = a.name_clean
				-- Do not assign an authority ID that is already linked to another meso row
				WHERE NOT EXISTS (SELECT 1 FROM meso WHERE {authority}_id = a.id_raw)
				-- Prefer accepted GBIF rows via authority_order and always finish with stable source ID ordering
				ORDER BY m.rowid, {authority_order}
			)
			-- Apply the one-row-per-target fallback mapping
			UPDATE meso SET {authority}_id = sm.auth_id
			FROM single_matches sm WHERE meso.rowid = sm.meso_rowid;
		""")
		after = db.execute(f"SELECT COUNT({authority}_id) FROM meso").fetchone()[0]
		mesologger.info(f"Added {after-before:,} {authority} IDs from source datasets via name match.")
	# Do this here as we need rank, pagecount etc in our core consensus logic and logging
	db.execute(f"""
		-- Add wikidata details
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS wikidata_pagecount SMALLINT;
	""")
	db.execute(f"""UPDATE meso m SET wikidata_pagecount = w.page_count FROM wikidata w WHERE w.page_count IS NOT NULL AND m.wikidata_id = w.id_raw;""")
	mesologger.info(f"Added Wikidata pagecount")

# Create our kingdom:phylum:class:order:family:genus:species:foo.bar unique string and then hash it into UUID5
def create_hashes(results: dict, db: duckdb.DuckDBPyConnection):
	"""
	https://www.iapt-taxon.org/nomen/pages/main/art_3.html
	3.1. The principal ranks of taxa in descending sequence are: kingdom (regnum), division or phylum (divisio or phylum), 
	class (classis), order (ordo), family (familia), genus (genus), and species (species). Thus, each species is assignable 
	to a genus, each genus to a family, etc. 
	"""
	# String first
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS id_input VARCHAR;
		-- Make author optional, IPNI has a bunch of names without authors https://www.ipni.org/n/17051740-1
		UPDATE meso SET id_input = lower(concat_ws(':',kingdom,rank_consensus,replace(name_consensus, ' ', '.'),author_consensus,year_consensus))
	""")
	mesologger.info(f"Creating Meso ID hashes...")
	# Register pyarrow hashing function
	db.create_function('make_uuid',uuid_v5_udf,['VARCHAR'],'UUID',type='arrow')
	# Then hash the main ID
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS id_meso UUID;
		UPDATE meso SET id_meso = make_uuid(id_input);
		ALTER TABLE meso DROP COLUMN IF EXISTS id_input;
	""")
	mesologger.info(f"Hashing complete")
	# Start handling duplicates
	"""
			Duplicate IDs are mostly a function of bad ID mapping, both between source datasets like WCVP/POWO, WFO, etc
			as well redundant and erroneous Wikidata entries
	"""
	# Mark dupes
	db.execute(f""" 
		ALTER TABLE meso ADD COLUMN dedupe BOOLEAN DEFAULT FALSE;
		-- Mark duplicate IDs
		UPDATE meso SET dedupe = TRUE 
		WHERE id_meso IN (SELECT id_meso FROM meso GROUP BY id_meso HAVING COUNT(*) > 1);
	""")
	dupes = db.sql(f"SELECT COUNT(*) FROM meso WHERE dedupe").fetchone()[0]
	if dupes: 
		mesologger.info(f"Consolidating {dupes:,} rows with duplicate IDs after initial hashing...")
		# Show redundant data
		if settings.VERBOSE: 
			db.sql(f"""
		  		SELECT id_meso, array_agg(source), {', '.join([f'array_agg({id}_id)' for id in core_authorities])}, {', '.join([f'array_agg({v})' for v in votes_held])}, COUNT(*) as count 
				FROM meso WHERE dedupe GROUP BY id_meso HAVING COUNT(*) > 1 ORDER BY count DESC
			""").show(max_rows=20)
		initial_count = db.sql(f"SELECT COUNT(*) FROM meso").fetchone()[0]
		# Execute
		db.execute(f"""
			-- Create unique ID rows by picking a value for each column from first the row with the most non-NULL overall values, and 
			-- then fall back on the row with the most IDs 
			CREATE TEMP TABLE deduped AS SELECT
				id_meso,
				FIRST(source ORDER BY non_null_count DESC, authority_count DESC) AS source,
				-- All core IDs plus NCBI
				{', '.join([f'FIRST({id}_id ORDER BY non_null_count DESC, authority_count DESC) AS {id}_id' for id in core_authorities + ['ncbi']])},
				-- Votes  
				{', '.join([f'FIRST({consensus} ORDER BY non_null_count DESC, authority_count DESC) AS {consensus}' for consensus in votes_held])},
				-- Kingdom
				FIRST(kingdom ORDER BY non_null_count DESC, authority_count DESC) AS kingdom,
				-- Other values
				MAX(wikidata_pagecount) AS wikidata_pagecount,
				FALSE AS dedupe
			FROM (
				SELECT *,
					-- Count non-null authority IDs
					({' + '.join([f'CASE WHEN {id}_id IS NOT NULL THEN 1 ELSE 0 END' for id in core_authorities + ['ncbi']])}) AS authority_count,
					-- Count all non-null columns
					({' + '.join([f'CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END' for col in ['source'] + [f'{id}_id' for id in core_authorities + ['ncbi']] + votes_held + ['kingdom', 'wikidata_pagecount']])}) AS non_null_count
				FROM meso 
				WHERE dedupe
			) ranked GROUP BY id_meso;
			-- Delete duplicates
			DELETE FROM meso WHERE dedupe;
			-- Insert deduplicated rows
			INSERT INTO meso BY NAME SELECT * FROM deduped;
			-- Clean up
			DROP TABLE deduped;
			ALTER TABLE meso DROP COLUMN dedupe;
		""")
		reduced_id_count = db.sql(f"SELECT COUNT(*) FROM meso").fetchone()[0]
		mesologger.info(f"""Reduced {initial_count:,} rows to {reduced_id_count:,} by consolidating those {dupes:,} duplicate IDs""")

# Additional iNaturalist, IUCN, Wikidata to complete full dataset
def enrich(results: dict, db: duckdb.DuckDBPyConnection):
	"""
		Notes:
		- IUCN needs to come after wikidata as it has no external references

	"""
	# Hybrid status
	vote(results,db,'hybrid')
	vote(results,db,'hybridpos')
	# Log potential outliers
	result = db.sql("SELECT * FROM meso WHERE hybridpos_consensus > 25 ORDER BY hybridpos_consensus DESC")
	rows = result.fetchdf()
	if len(rows) > 0:
		mesologger.warning(f"{len(rows)} rows with suspicious hybridpos")
		if settings.VERBOSE: result.show()
	# Add extra ID fields
	for id in additional_ids: 
		db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS {id} {'UINTEGER' if id in int_ids else 'VARCHAR'};""")
		db.execute(f"""UPDATE meso m SET {id} = w.{id} FROM wikidata w WHERE w.{id} IS NOT NULL AND m.wikidata_id = w.id_raw;""")
	mesologger.info(f"""Added additional IDs via wikidata to about {db.execute("SELECT COUNT(wikidata_pagecount) FROM meso").fetchone()[0]:,} entries""")
 	# Set Wikidata flags
	for flag in wikidata_flags: 
		# We don't set default false here to not break mode() later
		db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS {flag} BOOLEAN;""")
		db.execute(f"""UPDATE meso m SET {flag} = w.{flag} FROM wikidata w WHERE w.{flag} IS NOT NULL AND m.wikidata_id = w.id_raw;""")
	mesologger.info(f"""Added Wikidata flags to about {db.execute("SELECT COUNT(wikidata_pagecount) FROM meso").fetchone()[0]:,} entries""")
	# Add english wikipedia page name
	db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS wikipedia_page VARCHAR;""")
	db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS wikicommons VARCHAR;""")
	db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS wikispecies VARCHAR;""")
	db.execute(f"""UPDATE meso m SET wikipedia_page = w.wikipedia_pages.en, wikicommons = w.wikicommons, wikispecies = w.wikispecies FROM wikidata w WHERE m.wikidata_id = w.id_raw;""")
	mesologger.info(f"""Added English/Commons/Species wikipedia pages to {db.execute("SELECT COUNT(*) FROM meso WHERE wikipedia_page IS NOT NULL").fetchone()[0]:,} entries""")	
	# Add wikipedia author Q identifier and BHL page
	db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS qauthor VARCHAR;""")
	db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS bhl_page UINTEGER;""")
	# Parse BHL page from wikidata as first numeric chunk (e.g. 665972-1 -> 665972) and null invalid values instead of failing fusion
	db.execute(f"""UPDATE meso m SET qauthor = w.qauthor, bhl_page = TRY_CAST(regexp_extract(w.bhl_page, '^(\\d+)', 1) AS UINTEGER) FROM wikidata w WHERE m.wikidata_id = w.id_raw;""")
	# Add perennial / annual from POWO 
	db.execute(f"""
		UPDATE meso SET 
			annual = COALESCE(NULLIF(meso.annual, FALSE), powo.annual), 
			perennial = COALESCE(NULLIF(meso.perennial, FALSE), powo.perennial) 
		FROM powo WHERE meso.powo_id = powo.id_raw;
	""")
	# Add perennial / annual from WCVP
	db.execute(f"""
		UPDATE meso SET 
			annual = COALESCE(NULLIF(meso.annual, FALSE), wcvp.annual), 
			perennial = COALESCE(NULLIF(meso.perennial, FALSE), wcvp.perennial) 
		FROM wcvp WHERE meso.wcvp_id = wcvp.id_raw;
	""")
	mesologger.info(f"""Added annual/perennial values from POWO and WCVP""")	
	# Add native habitats and regions
	for tdwgcolumn in ['native_to','regions']:
		# Avoid list defaults in ALTER TABLE here because DuckDB can hit non-flat vector internal errors
		db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS { tdwgcolumn } VARCHAR[];""")
		for source in ['col','gbif','wcvp']:
			db.execute(f"""
				UPDATE meso
				SET { tdwgcolumn } = list_distinct(list_concat(COALESCE(meso.{ tdwgcolumn }, ARRAY[]::VARCHAR[]), COALESCE({source}.{ tdwgcolumn }, ARRAY[]::VARCHAR[])))
				FROM {source}
				WHERE meso.{source}_id = {source}.id_raw;
			""")
		db.execute(f"""UPDATE meso SET { tdwgcolumn } = NULL WHERE len({ tdwgcolumn }) = 0;""")
		sql = f"SELECT COUNT(*) FROM meso WHERE { tdwgcolumn } IS NOT NULL"
		mesologger.info(f"""Added { tdwgcolumn } to {db.execute(sql).fetchone()[0]:,} entries""")
	# Add IUCN status and assessment ID for building direct redlist links
	db.execute(f"""
		CREATE TYPE iucn_status_enum AS ENUM ('LC', 'EW', 'CR', 'VU', 'EN', 'EX', 'DD', 'NT');
		ALTER TABLE meso ADD COLUMN iucn_status iucn_status_enum;
		ALTER TABLE meso ADD COLUMN iucn_assessment UINTEGER;
		UPDATE meso SET 
			iucn_status = iucn.iucn_status,
			iucn_assessment = iucn.iucn_assessment
		FROM iucn iucn WHERE meso.iucn_id IS NOT NULL AND meso.iucn_id = iucn.id_raw;
	""")
	mesologger.info(f"""Added IUCN status to {db.execute("SELECT COUNT(iucn_status) FROM meso").fetchone()[0]:,} entries""")


# Merge and deduplicate all vernacular names
def reduce_vernacular(results: dict, db: duckdb.DuckDBPyConnection):
	mesologger.info(f"############### Reducing vernacular names ###############")
	# Fastest query as measured with plenty of A/B tests
	db.execute("""
		CREATE TEMP TABLE wikidata_vernacular AS
		SELECT m.id_meso, m.name_consensus, kv.key AS lang, kv.value AS names FROM meso m
		JOIN wikidata w ON m.wikidata_id = w.id_raw AND cardinality(w.vernacular) > 0
		CROSS JOIN LATERAL UNNEST(map_entries(w.vernacular)) AS t(kv);
	""")
	mesologger.info(f"""Loaded wikidata vernacular names for {db.execute("SELECT COUNT(*) FROM wikidata_vernacular").fetchone()[0]:,} item/language pairs""")
	# Create the table for higher quality vernacular names
	db.execute(f"""CREATE TEMP TABLE quality_vernacular (id_meso UUID, name_consensus VARCHAR, lang VARCHAR, names VARCHAR[]);""")
	current_count = 0
	# Fold in iNat, IUCN, GBIF, NCBI and CoL names, also A/B tested (the prefilter helps a lot here)
	for source in ['inaturalist','iucn','gbif','ncbi','col']:
		db.execute(f"""
			INSERT INTO quality_vernacular BY NAME 
			SELECT 
				m.id_meso,
			 	m.name_consensus,
				string_split(u.unnested, ':')[1] AS lang,
				array_agg(trim(string_split(u.unnested, ':')[2])) AS names
			FROM meso m
			JOIN {source} s ON m.{source}_id = s.id_raw AND len(s.vernacular) > 0
			CROSS JOIN LATERAL (SELECT UNNEST(s.vernacular) AS unnested) AS u
			GROUP BY m.id_meso, m.name_consensus, lang
		""")
		total_count = db.execute("SELECT COUNT(*) FROM quality_vernacular").fetchone()[0]
		mesologger.info(f"Added {int(total_count-current_count):,} vernacular names from {source}")
		current_count = total_count
	# Log
	if settings.VERBOSE:
		db.sql('SUMMARIZE wikidata_vernacular').show()
		db.sql('SELECT * FROM wikidata_vernacular').show()
		db.sql('SUMMARIZE quality_vernacular').show()
		db.sql('SELECT * FROM quality_vernacular').show()
	# Merge tables, remove name_consensus values and make all lowercase
	db.execute(f"""
		CREATE TEMP TABLE merged_vernacular AS
		SELECT id_meso, name_consensus, lang, list_filter(list_transform(flatten(array_agg(names)), x -> lower(x)), x -> x != name_consensus) AS names
		FROM (
			SELECT id_meso, name_consensus, lang, names FROM quality_vernacular
			UNION ALL
			SELECT id_meso, name_consensus, lang, names FROM wikidata_vernacular
		) combined
		GROUP BY id_meso, name_consensus, lang;
		DROP TABLE quality_vernacular;
		DROP TABLE wikidata_vernacular;
	""")
	mesologger.info(f"Merged vernacular data into {db.execute("SELECT COUNT(*) FROM merged_vernacular").fetchone()[0]:,} ID/language pairs")
	if settings.VERBOSE: db.sql("SELECT * FROM merged_vernacular ORDER BY id_meso DESC").show()
	# Delete all rows that have empty arrays (because we remove name_consensus values in the previous step and that's all they had)
	db.execute("DELETE FROM merged_vernacular WHERE len(names) = 0;")
	mesologger.info("Deleted rows with no remaining names")
	# Consolidate single unique values
	db.execute(f"""UPDATE merged_vernacular SET names =  list_value(list_any_value(names)) WHERE len(names) > 1 AND list_unique(names) = 1""")
	mesologger.info("Collapsed name lists that only had a single unique value")
	if settings.VERBOSE: db.sql("SELECT * FROM merged_vernacular ORDER BY id_meso DESC").show()
	# Start processing longer name lists
	# Add extra columns, mark and copy names
	db.execute(f"""
		-- Flag to mark which rows we need to process
		ALTER TABLE merged_vernacular ADD COLUMN process BOOLEAN DEFAULT FALSE;
		-- Current bucket of names with Levenshtein < 3	
		ALTER TABLE merged_vernacular ADD COLUMN bucket VARCHAR[];
		-- The most common name within that bucket	
		ALTER TABLE merged_vernacular ADD COLUMN most_common VARCHAR;
		-- Copy of the original name list we replace names in	
		ALTER TABLE merged_vernacular ADD COLUMN target VARCHAR[];
		UPDATE merged_vernacular SET 
			target = names,
			process = TRUE
		WHERE len(names) > 1;
	""")
	if settings.VERBOSE: db.sql("SELECT * FROM merged_vernacular WHERE process;").show()
	# Order each multiset by usage so its seed deterministically represents its nearest variants
	db.execute("""UPDATE merged_vernacular SET names = flatten(
		list_transform(
			list_sort(list_transform(list_distinct(names), x -> {'c': -len(list_filter(names, y -> y = x)), 'n': x})),
			s -> list_transform(range(len(list_filter(names, y -> y = s.n))), i -> s.n)
		)
	) WHERE process;""")
	# Count rows that need their first similarity bucket
	remaining = db.execute("SELECT COUNT(*) FROM merged_vernacular WHERE process").fetchone()[0]
	mesologger.info(f"Starting Levenshtein process for {remaining:,} rows...")
	while True:
		# Extract: Find any names that are Levenshtein distance < 3 from the first item in the (remaining) source list
		mesologger.info(f"(lev:bucket) {remaining:,} rows left to process               ", extra={'sameline': True})
		db.execute("""UPDATE merged_vernacular SET 
			-- Keep every source occurrence so the representative reflects actual usage
			bucket = list_filter(names, x -> levenshtein(names[1],x) < 3) WHERE process;""")
		# Keep the frequency-ranked seed as the representative for its similarity bucket
		mesologger.info(f"(lev:common) {remaining:,} rows left to process               ", extra={'sameline': True})
		db.execute("""UPDATE merged_vernacular SET most_common = names[1] WHERE process;""")
		# Replace: Replace their occurrence in the target list with the most common spelling variant
		mesologger.info(f"(lev:transf) {remaining:,} rows left to process               ", extra={'sameline': True})
		db.execute("""UPDATE merged_vernacular SET 
			names = list_filter(names, x -> NOT list_contains(bucket, x)),
			-- Faster if we only do it for operations where we actually need to replace multiple names with common_name 
			target = CASE WHEN len(list_distinct(bucket)) > 1 THEN list_transform(target, x -> CASE WHEN list_contains(bucket, x) THEN most_common ELSE x END)
					 ELSE target 
					 END
			WHERE process 
		""")
		# Limit and speed up next round
		mesologger.info(f"(lev:update) {remaining:,} rows left to process               ", extra={'sameline': True})
		db.execute("""UPDATE merged_vernacular SET process = FALSE WHERE process AND len(names) = 0;""")
		# See if we have anything left
		remaining = db.execute("SELECT COUNT(*) FROM merged_vernacular WHERE process").fetchone()[0]
		mesologger.info(f"(lev:counts) {remaining:,} rows left to process               ", extra={'sameline': True})
		if settings.VERBOSE: 
			db.sql("SELECT * FROM merged_vernacular WHERE process ORDER BY len(names) DESC;").show()
		# We stop early at 10 because some entries like https://www.wikidata.org/wiki/Q81602 have an unnecessary 
		# and wrong long tail we don't need and traversing long list takes long even for few rows.	
		if remaining <= 10: break
	# Log completion
	mesologger.info(f"Levenshtein complete")	
	if settings.VERBOSE: db.sql("SELECT * FROM merged_vernacular WHERE len(target) > 0 ORDER BY len(target) DESC;").show()
	# Reset names to processed values
	db.execute(f"""
		UPDATE merged_vernacular SET names = target WHERE len(target) > 0;
		ALTER TABLE merged_vernacular DROP COLUMN IF EXISTS process;
		ALTER TABLE merged_vernacular DROP COLUMN IF EXISTS bucket;
		ALTER TABLE merged_vernacular DROP COLUMN IF EXISTS most_common;
		ALTER TABLE merged_vernacular DROP COLUMN IF EXISTS target;
	""")	
	# Consolidate single unique values once more for rows that only had a single unique levenshtein < 3 value
	db.execute(f"""UPDATE merged_vernacular SET names = list_value(list_any_value(names)) WHERE len(names) > 1 AND list_unique(names) = 1""")
	mesologger.info("Collapsed name lists that only had a single unique value after Levenshtein processing")
	if settings.VERBOSE: db.sql("SELECT * FROM merged_vernacular ORDER BY id_meso DESC").show()
	# Register UDF
	vernacular_struct = duckdb.struct_type({"lang": duckdb.sqltype("VARCHAR"),"names": duckdb.list_type(duckdb.sqltype("VARCHAR"))})
	db.create_function('vernacular_udf', vernacular_udf, [vernacular_struct], 'VARCHAR[]', type='arrow')
	# Sort ties alphabetically before Polars ranks names by their occurrence counts
	db.execute(f"""UPDATE merged_vernacular SET names = vernacular_udf(struct_pack(lang := lang, names := list_sort(names))) WHERE len(names) > 1""")
	mesologger.info(f"Vernacular name processing complete")
	if settings.VERBOSE: db.sql("SELECT * FROM merged_vernacular WHERE len(names) > 1 ORDER BY id_meso DESC").show()
	# Add processed names back to main table
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS vernacular MAP(VARCHAR, VARCHAR[]);
		-- Update meso table with vernacular data
		UPDATE meso m
		SET vernacular = mv.vernacular_map
		FROM (
			SELECT 
				id_meso, 
				-- Limit sorted lists to 10 entries
				MAP_FROM_ENTRIES(ARRAY_AGG(ROW(lang, names[1:10]) ORDER BY lang)) AS vernacular_map
			FROM merged_vernacular
			GROUP BY id_meso
		) mv
		WHERE m.id_meso = mv.id_meso;
	""")
	mesologger.info(f"Added vernacular names to {db.execute("SELECT COUNT(*) FROM meso WHERE cardinality(vernacular) > 0").fetchone()[0]:,} rows")
	if settings.VERBOSE: db.sql('SELECT id_meso, name_consensus, vernacular FROM meso WHERE vernacular IS NOT NULL ORDER BY cardinality(vernacular) DESC').show()
	# Add most common english name as dedicated VARCHAR column
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN common_name VARCHAR;
		UPDATE meso SET common_name = list_mode(vernacular.en) WHERE vernacular IS NOT NULL;
	""")
	mesologger.info(f"""Added common name to {db.execute("SELECT COUNT(common_name) FROM meso").fetchone()[0]:,} rows""")

# Walk up the parentage tree to add higher ranks
def add_higher_ranks(results: dict, db: duckdb.DuckDBPyConnection):
	# Vote on parents first, but keep MyCoBank out of normal consensus because its sparse paths can overwrite richer chains
	vote(results,db,'parent_raw',[a for a in core_authorities if a != 'mycobank'])
	# Fix orphaned species by linking to their genus
	id_list = ', '.join([
		f'CAST({auth}_id AS VARCHAR)' if f'{auth}_id' in int_ids else f'{auth}_id' 
		for auth in core_authorities
	])
	mesologger.info("Linking orphaned species to their genus")
	db.execute(f"""
		UPDATE meso 
		SET parent_consensus = (
			SELECT id_meso 
			FROM meso AS parent
			WHERE parent.name_consensus = SPLIT_PART(meso.name_consensus, ' ', 1)
			-- Keep homonymous genera from crossing kingdoms, e.g. fungal Micropera attaching to orchid Micropera
			AND parent.kingdom = meso.kingdom
			AND parent.rank_consensus = 'GENUS'
			-- Only link to the canonical accepted genus row since acceptance is settled before this step
			AND parent.accepted
			ORDER BY len(list_filter([{id_list}], lambda x: x IS NOT NULL)) DESC
			LIMIT 1
		)
		WHERE meso.parent_consensus IS NULL
		AND meso.rank_consensus = 'SPECIES'
		AND array_length(str_split(meso.name_consensus, ' ')) = 2
	""")
	# Fix orphaned infraspecifics by linking to their species
	mesologger.info("Linking orphaned infraspecifics to their species")
	db.execute(f"""
		UPDATE meso 
		SET parent_consensus = (
			SELECT id_meso 
			FROM meso AS parent
			WHERE parent.name_consensus = SPLIT_PART(meso.name_consensus, ' ', 1) || ' ' || SPLIT_PART(meso.name_consensus, ' ', 2)
			-- Keep homonymous species from crossing kingdoms during name-prefix fallback
			AND parent.kingdom = meso.kingdom
			-- Keep this name-prefix fallback narrow; real source parentage can still attach sections etc when explicitly supplied
			AND parent.rank_consensus = 'SPECIES'
			-- Only link infraspecifics to the canonical accepted species row
			AND parent.accepted
			ORDER BY len(list_filter([{id_list}], lambda x: x IS NOT NULL)) DESC
			LIMIT 1
		)
		WHERE meso.parent_consensus IS NULL
		AND meso.rank_consensus IN ('SUBSPECIES', 'VARIETY', 'FORM', 'SUBVARIETY', 'SUBFORM', 'LUSUS')
		AND array_length(str_split(meso.name_consensus, ' ')) >= 3
	""")
	mesologger.info(f"Adding higher ranks")
	# Extract data from DuckDB into Polars
	taxa_df = db.execute("""SELECT id_meso, name_consensus, parent_consensus, rank_consensus FROM meso""").pl()
	mesologger.info(f"Dataframe loaded into polars")
	# Create initial ancestry dataframe (direct parent relationships)
	ancestry = (
		taxa_df.select(pl.col("id_meso").alias("child_id"),pl.col("parent_consensus").alias("parent_id"))
			   .filter(pl.col("parent_id").is_not_null())
			   .with_columns(pl.lit(1).alias("level")))
	# Store all ancestry relationships
	complete_ancestry = ancestry.clone()
	mesologger.info(f"Matched initial parent/child relationships")
	if settings.VERBOSE: mesologger.debug(complete_ancestry)
	# Iteratively build the full ancestry chain
	max_level = 20
	for level in range(2, max_level + 1):
		# Join current ancestry with taxa to get the next level of parents
		next_level = (
			ancestry.join(taxa_df.select("id_meso", "parent_consensus"),left_on="parent_id",right_on="id_meso")
				.select(pl.col("child_id"),pl.col("parent_consensus").alias("parent_id"),pl.lit(level).alias("level"))
				.filter(pl.col("parent_id").is_not_null()))
		# Stop if no more parents found
		if next_level.height == 0:
			mesologger.info(f"Max hierarchy depth {level-1} reached")
			break
		# Add to complete ancestry
		complete_ancestry = pl.concat([complete_ancestry, next_level])
		# Set up for next iteration
		ancestry = next_level
	mesologger.info(f"Expanded to complete ancestry chain")
	if settings.VERBOSE: mesologger.debug(complete_ancestry)	
	# Join with taxa data to get names and ranks of all ancestors
	mesologger.info("Adding taxonomic information")
	ancestry_with_info = (
		complete_ancestry.join(taxa_df.select("id_meso", "name_consensus", "rank_consensus"),left_on="parent_id",right_on="id_meso")
			.select(pl.col("child_id"),pl.col("parent_id"),pl.col("level"),pl.col("name_consensus")
			.alias("ancestor_name"),pl.col("rank_consensus").alias("ancestor_rank")))

	mesologger.info("Added names and ranks to ancestry chain")	
	if settings.VERBOSE: mesologger.debug(ancestry_with_info)	
	# For each requested higher rank, find the closest ancestor with that exact rank
	results = {}
	for rank in higher_ranks:
		clean_rank = rank.replace('"', '')
		rank_upper = clean_rank.upper()	
		mesologger.info(f"Processing {clean_rank}...")
		# Get all ancestors of this specific rank
		rank_ancestors = ancestry_with_info.filter(pl.col("ancestor_rank") == rank_upper)	
		# If empty, skip this rank
		if rank_ancestors.height == 0:
			mesologger.info(f"No ancestors found with rank {rank_upper}")
			continue
		# For each taxon, get the closest ancestor of this rank (lowest level number)
		rank_result = (
			rank_ancestors.group_by(["child_id"])
			.agg(pl.col("ancestor_name").first().alias(clean_rank),pl.col("level").min().alias(f"level_{clean_rank}"))
			.select(["child_id", clean_rank]))
		results[clean_rank] = rank_result
	# Create a base dataframe with all child_ids
	base_df = complete_ancestry.select("child_id").unique()
	# Join all rank results to the base dataframe
	final_df = base_df
	for rank, rank_df in results.items(): final_df = final_df.join(rank_df,on="child_id",how="left")
	# Extract genus from name_consensus for species-level taxa missing genus
	mesologger.info("Extracting genus from species names")
	final_df = final_df.join(
		taxa_df.select(["id_meso", "name_consensus", "rank_consensus"]),
		left_on="child_id", right_on="id_meso", how="left"
	).with_columns(
		pl.when(
			pl.col("genus").is_null() & 
			pl.col("rank_consensus").is_in(['SPECIES', 'SUBSPECIES', 'VARIETY', 'FORM', 'SUBVARIETY', 'SUBFORM', 'LUSUS']) &
			pl.col("name_consensus").str.contains(" ")
		)
		.then(pl.col("name_consensus").str.split(" ").list.first())
		.otherwise(pl.col("genus"))
		.alias("genus")
	).drop(["name_consensus", "rank_consensus"])
	mesologger.info("Ancestry with all ranks complete")	
	if settings.VERBOSE: mesologger.debug(final_df)	
	# Backfill missing ranks using mode of values from rows with same lower taxonomic ranks
	# Get all ranks that actually exist in DF (eg phylum is missing in debug)
	existing_ranks = [rank for rank in higher_ranks if rank.replace('"', '') in final_df.columns]
	# Log
	mesologger.info(f"Backfilling missing higher ranks { ', '.join(existing_ranks)}")
	# Process existing ranks from lowest to highest 
	backfill_order = existing_ranks.copy()
	backfill_order.reverse()  # Start with species, genus, etc.
	for i, rank in enumerate(backfill_order):
		# Skip the highest rank (no higher rank to pull from)
		if i == len(backfill_order) - 1: break  
		clean_rank = rank.replace('"', '')
		# Get most common higher rank value for each unique value in this rank
		higher_rank_values = (
			final_df
			.filter(pl.col(clean_rank).is_not_null())
			.group_by(clean_rank)
			.agg([pl.col(higher).mode().first().alias(f"src_{higher}") for higher in [r.replace('"', '') for r in backfill_order[:i]]]))
		# Join back to fill in missing higher values
		if higher_rank_values.height > 0:
			final_df = final_df.join(higher_rank_values,on=clean_rank,how="left")
			# Replace nulls with values from same-rank taxa
			for higher in [r.replace('"', '') for r in backfill_order[:i]]:
				final_df = final_df.with_columns(
					pl.when(pl.col(higher).is_null() & pl.col(f"src_{higher}").is_not_null())
					.then(pl.col(f"src_{higher}"))
					.otherwise(pl.col(higher))
					.alias(higher)
				)
			# Drop temporary columns
			final_df = final_df.drop([f"src_{higher}" for higher in [r.replace('"', '') for r in backfill_order[:i]]])
	mesologger.info("Backfilled missing ranks")	
	if settings.VERBOSE: mesologger.debug(final_df)
	# Add back to DuckDB
	db.execute(f"""
		-- Create columns (including those for which we might not have a dataframe in debug mode)
		{'; '.join([f'ALTER TABLE meso ADD COLUMN IF NOT EXISTS {rank} VARCHAR;' for rank in higher_ranks])};	
		-- Add data
			UPDATE meso SET {', '.join([f'{rank} = fd.{rank}' for rank in existing_ranks])}
			FROM final_df fd WHERE meso.id_meso = fd.child_id
	""")
	mesologger.info("Higher ranks added back to main table")
	if settings.VERBOSE: db.sql('SELECT id_meso, source::VARCHAR, name_consensus, rank_consensus::VARCHAR, kingdom, phylum, class, "order", family, genus FROM meso WHERE parent_consensus IS NOT NULL').show(max_rows=100)
	# Check for any non-kingdoms that don't have a phylum_consensus
	result = db.sql(f"""SELECT source, kingdom, phylum, class, "order", family, genus, name_consensus, rank_consensus FROM meso WHERE rank_consensus != 'KINGDOM' AND phylum IS NULL""")
	rows = result.fetchdf()
	if len(rows) > 0:
		mesologger.warning(f"Backbone still has {len(rows):,} rows without proper higher ranks")
		if settings.VERBOSE: result.show(max_rows=200)

# Decide which entries we want to create dedicated pages for
def decide_acceptance(results: dict, db: duckdb.DuckDBPyConnection):
	mesologger.info(f"############### Deciding acceptance ###############")
	# TODO: Find more authorities or way to get Tropicos data
	# DONE: See how we can handle fungi given that neither Mycobank nor Fungorum provide reliable taxon acceptance data (use CoL for now)
	authorities = {
		'plantae': 	['wcvp','powo','wfo','col','inaturalist','gbif'],
		# CoL is basically the Species Fungorum column missing in the Fungorum dataset.
		# MycoBank is intentionally NOT here: its status_clean='accepted' is permissive (maps Deleted→accepted)
		# and would inflate fungi acceptance by ~280k taxa. Use it as a soft-cascade hint below instead.
		'fungi': 	['col','inaturalist','gbif']
	}
	# Add pool and acceptance columns once before looping kingdoms
	# Must be outside loop: DuckDB 1.5.1 resets existing values when ADD COLUMN IF NOT EXISTS has a DEFAULT clause
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS accepted_by source_enum[];
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS accepted BOOLEAN DEFAULT FALSE;
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS considered_synonym source_enum[];
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS synonym BOOLEAN DEFAULT FALSE;
	""")
	# Handle kingdoms separately
	for kingdom in ['plantae','fungi']:	
		# Add to pool
		for authority in authorities[kingdom]: 
			db.execute(f""" 
				UPDATE meso SET accepted_by = list_append(accepted_by, '{authority}') FROM {authority} a 
				WHERE meso.kingdom = '{kingdom}' AND meso.{authority}_id = a.id_raw AND a.status_clean = 'accepted';
				UPDATE meso SET considered_synonym = list_append(considered_synonym, '{authority}') FROM {authority} a 
				WHERE meso.kingdom = '{kingdom}' AND meso.{authority}_id = a.id_raw AND a.status_clean = 'synonym';
			""")
		mesologger.info(f"Added {kingdom} acceptance from {len(authorities[kingdom])} authorit{'ies' if len(authorities[kingdom]) > 1 else 'y'} ({', '.join(authorities[kingdom])})")
		# Set synonym flag for all entries
		db.execute(f"""UPDATE meso SET synonym = (list_any_value(considered_synonym) IS NOT NULL) WHERE kingdom = '{kingdom}';""")
		# Also add wikidata acceptance for entries that have more than 6 wikipedia pages and aren't synonyms
		db.execute(f"""UPDATE meso SET accepted_by = list_append(accepted_by,'wikidata') WHERE kingdom = '{kingdom}' AND NOT synonym AND wikidata_pagecount > 6 AND meso.rank_consensus >= 'SPECIES'::taxon_rank_enum;""")	
		# Accept any rank where at least one authority directly voted accepted
		# Higher ranks (genus, family, order, ...) carry direct acceptance votes from CoL, GBIF, etc
		# and no longer rely on a separate cascade walk to flip the accepted flag
		db.execute(f"""UPDATE meso SET accepted = TRUE WHERE list_any_value(accepted_by) IS NOT NULL AND kingdom = '{kingdom}';""")
		# db.sql(f"SELECT id_meso, name_consensus, accepted, synonym, accepted_by, considered_synonym FROM meso WHERE kingdom = '{kingdom}' AND rank_consensus < 'SPECIES'::taxon_rank_enum AND list_any_value(accepted_by) IS NOT NULL;").show(max_rows=200)	
		# Log
		mesologger.info(f"""Accepted {db.sql(f"SELECT COUNT(*) FROM meso WHERE kingdom = '{kingdom}' AND accepted").fetchone()[0]:,} {kingdom} taxons""")
		if settings.VERBOSE: db.sql(f"""SELECT id_meso, name_consensus, accepted_by FROM meso WHERE kingdom = '{kingdom}' AND accepted ORDER BY len(accepted_by) DESC""").show(max_rows=20)
		reject_count = db.sql(f"SELECT COUNT(*) FROM meso WHERE kingdom = '{kingdom}' AND NOT accepted AND rank_consensus >= 'SPECIES'::taxon_rank_enum").fetchone()[0]
		if settings.VERBOSE:
			mesologger.info(f"""Most popular of {reject_count:,} {kingdom} rejects:""")
			db.sql(f"""SELECT id_meso, source, name_consensus, rank_consensus, accepted_by, wikidata_id, wikidata_pagecount FROM meso WHERE kingdom = '{kingdom}' AND NOT accepted AND rank_consensus >= 'SPECIES'::taxon_rank_enum ORDER BY wikidata_pagecount DESC LIMIT 20""").show(max_rows=20)
		else: mesologger.info(f"""Rejected {reject_count:,} {kingdom} taxons""")

# Make sure we don't have any name duplicates, and consolidate all column data to the accepted taxon
def dedupe_names(results: dict, db: duckdb.DuckDBPyConnection, rerun: bool = False):
	if not rerun: mesologger.info(f"############### De-Duplicating Names ###############")
	"""
			Duplicate names are a source dataset quality issue, for example:
			- WFO thinks there are multiple accepted 'Nepenthes distillatoria' or 'Andromeda oleifolia'
	"""
	# Mark first
	db.execute(f""" 
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS dedupe BOOLEAN DEFAULT FALSE;	
		-- Mark duplicate IDs
		UPDATE meso SET dedupe = TRUE 
		WHERE accepted AND name_consensus IN (SELECT name_consensus FROM meso WHERE accepted GROUP BY name_consensus HAVING COUNT(*) > 1);
	""")
	initial_count = db.sql(f"SELECT COUNT(*) FROM meso WHERE accepted").fetchone()[0]	
	mesologger.info(f"Deduplicating {db.sql(f"SELECT COUNT(*) FROM meso WHERE dedupe").fetchone()[0]:,} non unique names")
	if settings.VERBOSE: 
		db.sql(f"""
			SELECT array_agg(id_meso), name_consensus, array_agg(source), {', '.join([f'array_agg({id}_id)' for id in core_authorities])}, array_agg(accepted_by), array_agg(considered_synonym), COUNT(*) as count 
			FROM meso WHERE dedupe GROUP BY name_consensus HAVING COUNT(*) > 1 ORDER BY count DESC
		""").show(max_rows=100)
	# Pick row to use
	db.execute(f"""
		-- Create duplicate table 
		CREATE TEMP TABLE dupes AS SELECT *, len(accepted_by) AS acceptance_count, len(considered_synonym) AS synonym_count FROM meso WHERE dedupe;
		-- Grab the row with the most items in accepted_by, least considered being a synonym, or earliest year
		CREATE TEMP TABLE preferred_rows AS SELECT id_meso FROM dupes d
		WHERE d.id_meso IN (SELECT id_meso FROM dupes d2 WHERE d2.name_consensus = d.name_consensus 
		ORDER BY acceptance_count DESC, synonym_count ASC, year_consensus ASC LIMIT 1);
	""")
	mesologger.info(f"Picked {db.sql(f"SELECT COUNT(*) FROM preferred_rows").fetchone()[0]:,} rows to use")
	# Create backfill values
	extra_mode_values = ['wikipedia_page','iucn_status','common_name','wikicommons','wikispecies','bhl_page','qauthor'] + higher_ranks
	db.execute(f"""
		CREATE TEMP TABLE backfill_values AS 
		SELECT 
			name_consensus,
			mode(source) AS source,
			-- All core IDs
			{', '.join([f'mode({id}_id) AS {id}_id' for id in core_authorities])},
			-- Extra IDs
			{', '.join([f'mode({id}) AS {id}' for id in additional_ids])},
			-- Votes
			{', '.join([f'mode({consensus}) AS {consensus}' for consensus in votes_held])},
			-- Flags (find any true values)
			{', '.join([f'list_contains(array_agg({flag}), true) AS {flag}' for flag in wikidata_flags])},
			-- Other values we have at this point
			{', '.join([f'mode({v}) AS {v}' for v in extra_mode_values])},
			max(wikidata_pagecount) AS wikidata_pagecount,
			first(vernacular ORDER BY cardinality(vernacular) DESC) AS vernacular,
			-- Aggregate list values
			list_distinct(flatten(array_agg(accepted_by))) AS accepted_by,
			list_distinct(flatten(array_agg(considered_synonym))) AS considered_synonym,
			list_distinct(flatten(array_agg(native_to))) AS native_to,
			list_distinct(flatten(array_agg(regions))) AS regions
		FROM dupes GROUP BY name_consensus;
	""")
	mesologger.info(f"Collected backfill values")
	if settings.VERBOSE: db.sql("SELECT * FROM backfill_values").show(max_rows=50)
	# Unset accepted for all and then reset for preferred rows and consolidate values
	db.execute(f"""
		-- Unset accepted
		UPDATE meso SET accepted = false WHERE dedupe;	
		-- Update preferred_rows with values from backfill
		UPDATE meso SET
			-- Set accepted flag to true for preferred rows
			accepted = TRUE,
			-- All core IDs
			{', '.join([f'{id}_id = COALESCE(meso.{id}_id, b.{id}_id)' for id in core_authorities])},
			-- Extra IDs
			{', '.join([f'{id} = COALESCE(meso.{id}, b.{id})' for id in additional_ids])},
			-- Votes
			{', '.join([f'{consensus} = COALESCE(meso.{consensus}, b.{consensus})' for consensus in votes_held])},
			-- Flags
			{', '.join([f'{flag} = list_contains([meso.{flag}, b.{flag}],true)' for flag in wikidata_flags])},
			-- One off columns
			wikidata_pagecount = greatest(meso.wikidata_pagecount, b.wikidata_pagecount),
			vernacular = CASE 
				WHEN meso.vernacular IS NULL THEN b.vernacular
				WHEN b.vernacular IS NULL THEN meso.vernacular
				WHEN cardinality(meso.vernacular) >= cardinality(b.vernacular) THEN meso.vernacular
				ELSE b.vernacular
			END,
			common_name = COALESCE(meso.common_name, b.common_name),
			-- Aggregate list values
			accepted_by = array_distinct(list_concat(meso.accepted_by, b.accepted_by)),
			considered_synonym = array_distinct(list_concat(meso.considered_synonym, b.considered_synonym)),
			native_to = array_distinct(list_concat(meso.native_to, b.native_to)),
			regions = array_distinct(list_concat(meso.regions, b.regions))
		FROM backfill_values b, preferred_rows pr
		WHERE meso.id_meso = pr.id_meso AND meso.name_consensus = b.name_consensus;
		-- Clean up
		ALTER TABLE meso DROP COLUMN dedupe;
		DROP TABLE backfill_values;
		DROP TABLE dupes;
		DROP TABLE preferred_rows;
	""")
	reduced_name_count = db.sql(f"SELECT COUNT(*) FROM meso WHERE accepted").fetchone()[0]
	mesologger.info(f"""Reduced {initial_count:,} rows to {reduced_name_count:,} by consolidating {int(initial_count-reduced_name_count):,} name duplicates""")
	# Duplicate names with arrays of all ranks and years (including duplicates)
	rows = db.execute(f"""
		SELECT name_consensus FROM meso m WHERE accepted AND name_consensus IN (SELECT name_consensus FROM meso 
		WHERE accepted GROUP BY name_consensus HAVING COUNT(*) > 1) GROUP BY name_consensus ORDER BY COUNT(*) DESC
	""").fetchall()
	# Log any duplicates
	if len(rows) > 0: 
		mesologger.info(f"""Warning, still found duplicate names:""")
		rows.show(max_rows=40)

# Final parent hygiene now that acceptance, dedupe, and parent voting have already run.
# Direct vote at all ranks captures most of the skeleton; canonical parent dedup is obsolete
# because dedupe_names already enforces one accepted row per name. The remaining job is
# (a) a soft cascade for parents that direct vote missed but multiple accepted children still
# point at, attributed to those children's authorities so accepted_by stays meaningful,
# (b) dangling parent cleanup, and (c) child counting.
def consolidate_parents(results: dict, db: duckdb.DuckDBPyConnection):
	mesologger.info(f"############### Consolidating parents ###############")
	# Soft cascade: promote unaccepted parents that have ANY accepted children currently pointing
	# at them, attributing acceptance to the union of those children's authorities. This preserves
	# the invariant that every accepted row has non-empty accepted_by (no "cascade-only" rows) while
	# recovering parents like phytophthora that direct vote alone leaves unaccepted.
	previous_count = -1
	for _ in range(20):
		# Find unaccepted rows referenced by accepted children
		cands = db.execute(f"""
			SELECT m.id_meso, list_distinct(flatten(array_agg(c.accepted_by))) AS inherited_acc
			FROM meso m
			JOIN meso c ON c.parent_consensus = m.id_meso AND c.accepted
			WHERE NOT m.accepted AND m.rank_consensus != 'KINGDOM'
			GROUP BY m.id_meso
		""").fetchall()
		# Stop when there are no more candidates or the count is no longer shrinking
		if not cands or len(cands) == previous_count: break
		previous_count = len(cands)
		mesologger.info(f"Soft-cascade promoting {len(cands):,} parent rows from accepted children")
		# Persist candidates as a temp table for join-based update
		db.execute("""DROP TABLE IF EXISTS soft_cascade_candidates;""")
		db.execute("""CREATE TEMP TABLE soft_cascade_candidates (id_meso UUID, inherited_acc source_enum[]);""")
		db.executemany("INSERT INTO soft_cascade_candidates VALUES (?, ?)", cands)
		# Promote: union the children's accepted_by into the parent's accepted_by and flip accepted
		db.execute(f"""
			UPDATE meso SET
				accepted_by = list_distinct(list_concat(COALESCE(meso.accepted_by, ARRAY[]::source_enum[]), c.inherited_acc)),
				accepted = TRUE
			FROM soft_cascade_candidates c
			WHERE meso.id_meso = c.id_meso;
		""")
	db.execute("""DROP TABLE IF EXISTS soft_cascade_candidates;""")
	mesologger.info("Soft cascade complete")
	# Create accepted IDs / names temp table to reuse / speed up
	db.execute(f"""
		CREATE TEMP TABLE accepted_rows AS SELECT 
			m.id_meso,
			m.name_consensus,
			m.parent_consensus,
			p.name_consensus AS parent_name
		FROM meso m
		LEFT JOIN meso p ON m.parent_consensus = p.id_meso
		WHERE m.accepted;
	""")
	# Log 
	mesologger.info(f"""Built lookup table with all {db.execute("SELECT COUNT(*) FROM accepted_rows;").fetchone()[0]:,} accepted rows""")
	if settings.VERBOSE: db.sql("SELECT * FROM accepted_rows;").show()
	# Remove all dangling parents (parents whose IDs are not in our accepted taxon list)
	result = db.execute(f"""
		UPDATE meso SET parent_consensus = NULL 
		WHERE meso.accepted AND parent_consensus IS NOT NULL 
			AND parent_consensus NOT IN (SELECT id_meso FROM accepted_rows)
		RETURNING 1;
	""").fetchall()
	mesologger.info(f"Removed {len(result):,} dangling parent references")
	# Try adding missing parents via name
	result = db.execute(f"""
		UPDATE meso m SET parent_consensus = ar2.id_meso
		FROM accepted_rows ar1
		JOIN accepted_rows ar2 ON ar2.name_consensus = ar1.parent_name
		WHERE m.accepted 
		AND m.parent_consensus IS NULL
		AND ar1.id_meso = m.id_meso
		RETURNING 1;
	""").fetchall()
	mesologger.info(f"Found {len(result):,} missing parents by name match")
	# Add child count, if it's larger than a usmallint something's broken
	db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS child_count USMALLINT;""")
	db.execute(f"""
		WITH child_counts AS (SELECT parent_consensus, COUNT(*) AS child_count FROM meso WHERE accepted AND parent_consensus IS NOT NULL GROUP BY parent_consensus)
		UPDATE meso SET child_count = COALESCE(cc.child_count, 0) FROM child_counts cc WHERE meso.id_meso = cc.parent_consensus;
	""")
	# Log
	mesologger.info(f"""Counted children of {db.execute("SELECT COUNT(*) FROM meso WHERE child_count > 0;").fetchone()[0]:,} parental rows""")
	if settings.VERBOSE: db.sql("SELECT id_meso, source, name_consensus, child_count FROM meso ORDER BY child_count DESC LIMIT 20;").show()

# Carefully add any potential missing name matches from GBIF, NCBI etc, ie sources that we use for climate and research, as well as missing parents after cleaning them up
def polish(results: dict, db: duckdb.DuckDBPyConnection):
	mesologger.info(f"############### Polishing results ###############")
	# Add missing GBIF / NCBI IDs by name
	# Those two are essentials we derive obeservations and thus climate from GBIF, and all protein lookups from NCBI
	for source in ['gbif','ncbi']:
		# NCBI-specific: redirect obsolete taxids via the authoritative merged.dmp mapping before nulling anything.
		# This preserves cross-source ID links (most commonly from Wikidata) across NCBI's historical merges,
		# including cases where the merged-away taxid carried a different scientific name and name-match
		# fallback would silently lose the link.
		if source == 'ncbi':
			# merged_from is stored as UINTEGER[] on the ncbi parquet because the relationship is
			# naturally one-to-many (many old taxids redirect into one current taxid). Joining
			# directly against list_contains would force a nested loop since there is no scalar
			# equi-join predicate. Unnest in a pipelined CTE so DuckDB can hash-join on equality;
			# measured end-to-end cost drops from ~150s to well under a second at current volumes.
			result = db.execute("""
				WITH ncbi_redirects AS (
					SELECT unnest(merged_from) AS old_id, id_raw AS new_id
					FROM ncbi WHERE merged_from IS NOT NULL
				)
				UPDATE meso m SET ncbi_id = r.new_id
				FROM ncbi_redirects r
				WHERE m.accepted AND m.ncbi_id IS NOT NULL AND m.ncbi_id = r.old_id
				RETURNING 1;
			""").fetchall()
			mesologger.info(f"Redirected {len(result):,} obsolete NCBI IDs via merged.dmp")
		# Null all ids that are not in the source datasets anymore
		result = db.execute(f"""
			UPDATE meso SET {source}_id = NULL
			WHERE meso.accepted AND {source}_id IS NOT NULL
			AND {source}_id NOT IN (SELECT DISTINCT id_raw FROM {source})
			RETURNING 1;
		""").fetchall()
		mesologger.info(f"Nulled {len(result):,} obsolete {source.upper()} IDs")
		# GBIF special cases as we have accepted etc metadata (NCBI is name and ID only)
		if source == 'gbif':
			# Replace outdated GBIF IDs with current accepted ones 
			# Step 1: Create a deterministic lookup table with unique accepted name values
			db.execute(f"""
				CREATE TEMP TABLE name_accepted AS
					-- Choose one accepted GBIF ID per name so MODE cannot flip between equal candidates
					SELECT name_clean, FIRST(id_raw ORDER BY id_raw) as accepted_id
					-- Only accepted GBIF rows should replace obsolete synonym/problematic IDs
					FROM {source} WHERE status_clean = 'accepted'
					-- Keep one lookup row per cleaned name for the later hash join
					GROUP BY name_clean;
			""")
			# Step 2: Simple hash join update with COALESCE to preserve original if no accepted alternative
			result = db.execute(f"""
				UPDATE meso SET {source}_id = COALESCE(na.accepted_id, meso.{source}_id)
				FROM {source} old_rec
				LEFT JOIN name_accepted na ON na.name_clean = old_rec.name_clean
				WHERE meso.{source}_id = old_rec.id_raw
				AND meso.accepted
				AND old_rec.status_clean != 'accepted'
				RETURNING 1;
			""").fetchall()
			# Clean up
			db.execute("DROP TABLE name_accepted")
			mesologger.info(f"Updated {len(result):,} outdated {source.upper()} IDs to their accepted value")
			# Add accepted GBIF IDs to remaining NULL rows first
			result = db.execute(f"""
				UPDATE meso SET {source}_id = (
					-- Prefer accepted GBIF IDs before falling back to any same-name GBIF row
					SELECT s.id_raw FROM {source} s 
					-- Match the final consensus name rather than an earlier raw/source name
					WHERE s.name_clean = meso.name_consensus 
					-- Restrict this first pass to accepted GBIF records only
					AND s.status_clean = 'accepted'
					-- Make LIMIT 1 deterministic for names with multiple accepted GBIF rows
					ORDER BY s.id_raw
					LIMIT 1
				)
				WHERE meso.accepted 
				AND meso.{source}_id IS NULL 
				RETURNING 1;
			""").fetchall()
			mesologger.info(f"Added {len(result):,} accepted {source.upper()} IDs by name matching")
		# Prefer accepted GBIF rows when final name matching still has multiple candidates
		if source == 'gbif': source_order = "CASE WHEN s.status_clean = 'accepted' THEN 0 WHEN s.status_clean = 'synonym' THEN 1 ELSE 2 END, s.id_raw"
		# Otherwise keep final name matching deterministic by source ID
		else: source_order = "s.id_raw"
		# Try adding the remaining IDs we might have
		result = db.execute(f"""
			UPDATE meso SET {source}_id = (
				-- Fill the remaining accepted rows from same-name source records
				SELECT s.id_raw FROM {source} s 
				-- Match on final consensus name so this stays aligned with release identity
				WHERE s.name_clean = meso.name_consensus 
				-- Keep LIMIT 1 deterministic and prefer accepted GBIF records when available
				ORDER BY {source_order}
				LIMIT 1
			)
			WHERE meso.accepted 
			AND meso.{source}_id IS NULL 
			RETURNING 1;
		""").fetchall()
		mesologger.info(f"Added {len(result):,} {source.upper()} IDs by name matching")
	# Truncate WFO IDs to 14 characters
	db.execute("UPDATE meso SET wfo_id = LEFT(wfo_id, 14) WHERE LENGTH(wfo_id) > 14;")
	mesologger.info(f"Truncated WFO IDs to 14 characters")

# Final validation
def validate(results: dict, db: duckdb.DuckDBPyConnection):
	# Duplicate names with arrays of all ranks and years (including duplicates)
	rows = db.sql("""
		WITH duplicate_names AS (SELECT name_consensus FROM meso WHERE accepted GROUP BY name_consensus HAVING COUNT(*) > 1)
		SELECT 
			m.name_consensus,
			COUNT(*) AS occurrences,
			array_agg(m.source) AS source,
			array_agg(accepted_by) AS accepted_by,
			array_agg(m.rank_consensus) AS ranks,
			array_agg(m.author_consensus) AS author,
			array_agg(m.id_meso) AS id_meso
		FROM meso m JOIN duplicate_names d ON m.name_consensus = d.name_consensus WHERE m.accepted 
		GROUP BY m.name_consensus HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC, m.name_consensus
	""")
	# Log any duplicates
	if len(rows) > 0:
		mesologger.info(f"{len(rows):,} duplicates accepted")
		if settings.VERBOSE: rows.show(max_rows=40)

def package_release(results: dict, db: duckdb.DuckDBPyConnection):
	# Create a release name first, which is highest value of latest_processed_ 
	# plus a MD5 hash of all results' latest processed versions
	import hashlib, json
	# Get a hash from the most recent processed timestamps
	sorted_results = dict(sorted(results.items()))
	timestamps = ''.join([val.get('latest_processed', '').split('.')[1] for val in sorted_results.values() if val.get('latest_processed')])
	hash = hashlib.md5(timestamps.encode()).hexdigest()[:12]
	# Get most recent timestamp
	most_recent = max(int(val.get('latest_processed', '').split('.')[1]) for val in sorted_results.values()  if val.get('latest_processed', ''))
	release = f"{ most_recent }-{ hash }"
	# Start packaging release
	mesologger.info(f"############### Packaging Release ###############")
	# Check if we already have that release
	if check_release(release):
		# Abort if we're not forcing it
		if not settings.FORCE:
			mesologger.info(f"Release directory { release } already exists, use flag -f to overwrite")
			return
		# Otherwise remove the existing dir
		else:
			mesologger.info(f"Deleting existing release { release }")
			storage.rmtree(os.path.join(RELEASES_DIR, release))
	# Create release directory
	release_dir = os.path.join(RELEASES_DIR, release)
	storage.makedirs(release_dir)
	# Write our data
	write_to_disc(db, { "name": "meso" }, release_dir, release)
	# Copy processed input files to release
	for result in results: 
		processed_file = os.path.join(PROCESSED_DIR,results[result].get('latest_processed'))
		target_file = os.path.join(release_dir, results[result].get('latest_processed'))
		mesologger.info(f"Copying { processed_file } to { target_file }")
		storage.copy(processed_file, target_file)
	# Load latest run-state manifest for source metadata enrichment
	state_manifest = load_state()
	# Build merged source payload with state values and runtime fallback values
	release_sources = {}
	# Merge each source used in this release
	for source_name, source_payload in results.items():
		# Start from runtime payload so existing flow still works if state is missing
		merged = dict(source_payload)
		# Pull matching source entry from state manifest when available
		state_source = state_manifest.get('sources', {}).get(source_name, {})
		# Overlay stable source metadata from state when present
		for key in ['url', 'citation', 'latest_download', 'timestamp_download', 'timestamp_remote', 'timestamp_local', 'latest_processed', 'timestamp_processed']:
			# Keep state value only when it is explicitly available
			if state_source.get(key) is not None: merged[key] = state_source.get(key)
		# Store merged source payload under canonical source name
		release_sources[source_name] = merged
	# Create manifest
	manifest = { 'version': release, 'sources': release_sources}
	# Write manifest to disc
	storage.write_json(os.path.join(release_dir,'manifest.json'), manifest)
	# Return data for next step	
	return manifest


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#
#		Utility functions we might call at different points
#
# Load a parquet into memory tem table
def load_parquet(results: dict, db: duckdb.DuckDBPyConnection, source: str):
	parquet = db.read_parquet(storage.parquet_url(os.path.join(PROCESSED_DIR, results[source].get('latest_processed'))))
	db.execute(f"CREATE TEMP TABLE {source} AS SELECT * FROM parquet")
	mesologger.info(f"Loaded {source} data into memory / temp table")
	if settings.VERBOSE: db.sql(f"SUMMARIZE {source}").show(max_rows=40)

# Load values from multiple sources and pick the most common, with ordinality as tie breaker 
def vote(results: dict, db: duckdb.DuckDBPyConnection, column: str, authorities: list=None):
	# Get the fieldname, eg rank_raw then creates ipni_rank etc columns
	fieldname = column.split('_')[0] if '_' in column else column
	# Fall back on our default authorities
	if not authorities: authorities = core_authorities
	# Column votes we only want to apply to accepted rows to speed things up, after we checked acceptance
	limited_votes = ['phylum','class','order','family','genus','species']
	# Avoid casting for columns where all values are a non-VARCHAR type
	column_types = { 'year': 'USMALLINT', 'parent_raw': 'UUID','hybrid':'BOOLEAN', 'rank_clean': 'taxon_rank_enum', 'hybridpos': 'UTINYINT',}
	# Start forming pool and columns
	pool = []
	for authority in authorities: 
		if db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{authority}' AND column_name = '{column}'").fetchone() is not None:
			pool.append(authority)
			db.execute(f"""ALTER TABLE meso ADD COLUMN IF NOT EXISTS {fieldname}_{authority} {column_types.get(column, 'VARCHAR')};""")
	# Add pool and result columns
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS {fieldname}_pool {column_types.get(column, 'VARCHAR')}[];
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS {fieldname}_consensus {column_types.get(column, 'VARCHAR')};
	""")	
	for authority in pool: 	
		# Handle special votes
		match column:
			case 'name_clean':
				# Only add names without leftover ranks, authors etc
				db.execute(f"""
					UPDATE meso m SET {fieldname}_{authority} = a.{column} FROM {authority} a 
					WHERE m.{authority}_id = a.id_raw AND (len(string_split(a.name_clean,' ')) < 4 OR contains(a.name_clean, ''''));
				""")
				result = db.sql(f"SELECT id_raw, name_clean FROM {authority} WHERE len(string_split(name_clean,' ')) > 3 AND NOT contains(name_clean, '''');")
				rowcount = result.arrow().read_all().num_rows
				if rowcount > 0:
					mesologger.info(f"Disqualified {rowcount:,} names from {authority}")
					if settings.VERBOSE: result.show(max_rows=20)
			case 'parent_raw':
				# Resolve each authority's parent_raw to the canonical accepted meso row carrying that parent's name.
				# Acceptance and dedupe have already run, so each (kingdom, name_consensus) has at most one accepted row.
				# Routing every authority through the canonical accepted row eliminates Erica L. vs Erica Boehm.
				# style UUID flips in the parent vote without changing the consensus voting machinery itself.
				# Falls back to the raw m_parent row when no accepted canonical exists, so the soft cascade in
				# consolidate_parents can later promote useful parents like phytophthora that direct vote missed.
				db.execute(f"""
					UPDATE meso SET parent_{authority} = (
						SELECT COALESCE(canonical.id_meso, m_parent.id_meso)
						FROM {authority} AS i
						JOIN meso AS m_parent ON m_parent.{authority}_id IS NOT NULL AND i.parent_raw = m_parent.{authority}_id
						LEFT JOIN meso AS canonical ON canonical.name_consensus = m_parent.name_consensus
						                           AND canonical.kingdom = m_parent.kingdom
						                           AND canonical.accepted
						WHERE i.id_raw = meso.{authority}_id
						ORDER BY canonical.id_meso IS NULL, m_parent.id_meso::VARCHAR
						LIMIT 1
					)
					WHERE EXISTS (SELECT 1 FROM {authority} WHERE parent_raw IS NOT NULL AND id_raw = meso.{authority}_id);
				""")
				mesologger.info(f"Resolved {authority} parent IDs to canonical accepted meso rows (with raw fallback)")
			case 'rank_clean':
				# Uppercase rank values to match our enum and skip values outside enum labels
				db.execute(f"""
					UPDATE meso m SET {fieldname}_{authority} = upper(a.{column}) FROM {authority} a
					WHERE m.{authority}_id = a.id_raw
					AND a.{column} IS NOT NULL
					AND upper(a.{column}) IN (SELECT unnest(enum_range(NULL::taxon_rank_enum)));
				""")
				# Log source rows with invalid rank labels (e.g. greek rank markers) that were skipped
				result = db.sql(f"""
					SELECT id_raw, name_clean, {column}
					FROM {authority}
					WHERE {column} IS NOT NULL
					AND upper({column}) NOT IN (SELECT unnest(enum_range(NULL::taxon_rank_enum)));
				""")
				rowcount = result.arrow().read_all().num_rows
				if rowcount > 0:
					mesologger.info(f"Skipped {rowcount:,} invalid ranks from {authority}")
					if settings.VERBOSE: result.show(max_rows=20)
			case 'year':
				# Only set year values that are between 1700 and current year
				db.execute(f"""
					UPDATE meso m SET {fieldname}_{authority} = a.{column} FROM {authority} a 
					WHERE m.{authority}_id = a.id_raw AND a.{column} BETWEEN 1700 AND EXTRACT(YEAR FROM CURRENT_DATE);
				""")
				result = db.sql(f"SELECT id_raw, name_clean, {column} FROM {authority} WHERE {column} < 1700 OR {column} > EXTRACT(YEAR FROM CURRENT_DATE);")
				rowcount = result.arrow().read_all().num_rows
				if rowcount > 0:
					mesologger.info(f"Skipped {rowcount:,} invalid years from {authority}")
					if settings.VERBOSE: result.show(max_rows=20)
			case _:
				# Just add all source columns
				db.execute(f"""UPDATE meso m SET {fieldname}_{authority} = a.{column} FROM {authority} a WHERE m.{authority}_id = a.id_raw;""")
	mesologger.info(f"Candidates for {column} loaded from {len(pool)} authorities ({', '.join(pool)})")
	# Add columns to pool, the order also determines tie-breaking hierarchy (earlier values will take precedence)
	db.execute(f"""UPDATE meso SET {fieldname}_pool = [{', '.join([f'{fieldname}_{authority}' for authority in pool])}]""")
	mesologger.info(f"Built candidate pool for {column} consensus")
	# Pick winner and trim it while we're at it
	if not column_types.get(column): db.execute(f"""UPDATE meso SET {fieldname}_consensus = trim(list_mode({fieldname}_pool));""")
	# Don't try trimming USMALLINTS,BOOLEAN etc
	else: db.execute(f"""UPDATE meso SET {fieldname}_consensus = list_mode({fieldname}_pool);""")
	# Add value to votes held, used in dedupe etc later
	votes_held.append(f"{fieldname}_consensus")
	# Log results
	name_column = 'name_consensus' if db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'meso' AND column_name = 'name_consensus'").fetchone() is not None else 'name_clean'
	result = db.sql(f"""
		SELECT {name_column}, {', '.join([f'{fieldname}_{authority}' for authority in pool])}, {fieldname}_consensus 
		-- NULLS omitted, so > 1 is correct
		FROM meso WHERE len(list_distinct({fieldname}_pool)) > 1
		-- Special cases
		{ "ORDER BY array_length(regexp_split_to_array(trim(name_consensus), '\\s+')) DESC" if column == "name_clean" else ""}
		""")
	rowcount = result.arrow().read_all().num_rows
	if rowcount > 0:
		mesologger.info(f"Voted on {rowcount:,} ambiguous {column} values")
		if settings.VERBOSE: result.show(max_rows=20)
	# Cleanup 
	for authority in pool: db.execute(f"""ALTER TABLE meso DROP COLUMN IF EXISTS {fieldname}_{authority}""")
	db.execute(f"""ALTER TABLE meso DROP COLUMN IF EXISTS {fieldname}_pool;""")
	# Special vote cleanup
	match column:
		case 'name_clean':
			# In cases where we didn't find a name_consensus (mostly because all names were rejected)
			result = db.sql(f"SELECT source, {'_id, '.join([f'{authority}' for authority in pool])}_id, name_clean FROM meso WHERE name_consensus IS NULL;")
			rowcount = result.arrow().read_all().num_rows
			if rowcount > 0:
				mesologger.warning(f"Falling back on original name_clean in {rowcount:,} cases where we didn't find any consensus candidates")
				if settings.VERBOSE: result.show(max_rows=40)
				# Use existing name_clean instead
				db.execute(f"""UPDATE meso SET name_consensus = name_clean WHERE name_consensus IS NULL""")
				# Drop column
				db.execute(f"""ALTER TABLE meso DROP COLUMN IF EXISTS name_clean;""")
		case _:
			# Also delete the row we voted on, shouldn't make that much performance difference but easier to debug when it throws errors
			db.execute(f"""ALTER TABLE meso DROP COLUMN IF EXISTS {column};""")	
			db.execute(f"""ALTER TABLE meso DROP COLUMN IF EXISTS {fieldname};""")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#
#		Pyarrow functions we can load in DuckDB as vectorized UDF, this processes 2048 values 
# 		at a time instead of calling the function for every row. The internal STANDARD_VECTOR_SIZE 
# 		in DuckDB can't be changed (yet)
#
# Hash kingdom:name:author into UUID5
uuid_count = 0
def uuid_v5_udf(scalars: pa.StringArray):
	global uuid_count
	uuid_count += 2048
	mesologger.info(f"Hashed {uuid_count:,} UUIDs", extra={'sameline': True})
	return [str(uuid.uuid5(settings.HASH_NAMESPACE, s.as_buffer().to_pybytes())) for s in scalars]

# Duckdb UDF using polars zero copied from and to arrows
vernacular_count = 0
def vernacular_udf(data: pa.StructArray) -> pa.Array:
	"""
		Take arrays of vernacular names, normalize similar spelling (Levenshtein <= 2)
		and collapse into unique values sorted by frequency

		Test cases:
			["Becherprimel","Becher-Primel","Gift-Primel","Becher Primel"]
	"""
	global vernacular_count
	vernacular_count += len(data)
	# Zero-copy to Polars
	df = pl.from_arrow(data).struct.unnest()
	# Get names column
	result = df.with_columns([pl.col("names")
		# Replace list of names with list of sorted unique value counts
		.list.eval(pl.element().value_counts(sort=True))
		# Extract values of reduced order list back into list
		.list.eval(pl.element().struct[0])
	])
	mesologger.info(f"Processed {vernacular_count:,} complex rows with polars", extra={'sameline': True})
	return result["names"].to_arrow()

#
#		Catalogue of Life Extended Release
#
#		COL XR starts with COL Base and integrates additional checklists.
#		GBIF uses COL XR for occurrence taxonomy.
#

# Define the rolling COL XR source
source = {
	"name": "colxr",
	"url": "https://download.checklistbank.org/col/xr_latest_coldp.zip",
	"use_aria": 4,
	"citation": '<a href="https://www.catalogueoflife.org/" class="medium">Catalogue of Life Extended Release</a>, Catalogue of Life Partnership. Version YYYY-MM-DD. <a href="https://www.checklistbank.org/" class="medium">ChecklistBank</a>'
}

# Load project logging
from ..utils.log import mesologger
# Load source paths and runtime settings
from .. import SRC_DIR, TMP_DIR, settings
# Load archive handling
import zipfile
# Load shared source fetch logic
from ..utils.filehandlers import fetch
# Load DuckDB for ColDP processing
import duckdb
# Load standard source normalization and output helpers
from ..utils.queries import name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc, coldp_vernacular

# Download and process the current COL XR archive
async def update_colxr(session):
	# Announce the source stage
	mesologger.info("############### Updating Catalogue of Life Extended Release ###############")
	# Check whether a newer source archive is available
	update_available = await fetch(session, source)
	# Process new or explicitly forced source data
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_colxr(source)
	# Return source metadata for later pipeline stages
	return source

# Convert COL XR to the standard canopy source shape
def process_colxr(source: dict):
	# Resolve the source archive prepared by fetch
	source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
	# Open the archive and one isolated DuckDB connection
	with zipfile.ZipFile(source_path, 'r') as archive, duckdb.connect(':memory:') as db:
		# Keep DuckDB spill files in the canopy temporary directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Read usages without quote parsing because ColDP fields contain unescaped quotes
		name_tsv = db.read_csv(archive.open('NameUsage.tsv'), parallel=True, null_padding=True, delimiter='\t', quotechar='\0', dtype={'col:combinationAuthorshipYear': 'VARCHAR'})
		# Read vernacular names for the standard canopy field
		vernacular_tsv = db.read_csv(archive.open('VernacularName.tsv'), parallel=True, null_padding=True, delimiter='\t', quotechar='\0')
		# Load source values into one normalized table without relationship joins
		db.execute("""
			CREATE TABLE colxr AS
			SELECT
				-- COL XR usage identifier
				"col:ID" AS id_raw,
				-- Scientific name used by shared name cleanup
				lower("col:scientificName") AS name_clean,
				-- Hierarchy parent for accepted rows and accepted target for synonym rows
				"col:parentID" AS parent_raw,
				-- Source authorship
				"col:authorship" AS author_raw,
				-- Source rank
				"col:rank" AS rank_raw,
				-- Source classification
				lower("col:kingdom") AS kingdom,
				lower("col:phylum") AS phylum,
				lower("col:class") AS class,
				lower("col:order") AS "order",
				lower("col:family") AS family,
				lower("col:genus") AS genus,
				-- Direct source authorship year
				CAST(NULLIF(regexp_extract("col:combinationAuthorshipYear", '\\d{4}', 0), '') AS USMALLINT) AS year,
				-- Existing canopy hybrid field
				CAST("col:notho" IS NOT NULL AS BOOLEAN) AS hybrid,
				-- Detailed COL XR usage status
				"col:status" AS status_raw
			FROM name_tsv;
		""")
		# Fill the kingdom of relevant synonym-like usages from their accepted target
		db.execute("""
			WITH target_scope AS (
				SELECT id_raw, kingdom
				FROM colxr
				WHERE kingdom IN ('plantae', 'fungi')
			)
			UPDATE colxr AS usage
			SET kingdom = target.kingdom
			FROM target_scope AS target
			WHERE usage.kingdom IS NULL
				AND usage.status_raw IN ('synonym', 'ambiguous synonym', 'misapplied')
				AND usage.parent_raw = target.id_raw;
		""")
		# Remove usages outside the plant and fungi scope
		db.execute("DELETE FROM colxr WHERE kingdom NOT IN ('plantae', 'fungi') OR kingdom IS NULL")
		# Report the scoped usage count
		mesologger.info(f"Loaded {db.execute('SELECT COUNT(*) FROM colxr').fetchone()[0]:,} plant and fungi usages from colxr")
		# Normalize hybrid names and positions
		find_hybrids(db, source)
		# Apply shared name cleanup
		name_cleanup(db, source)
		# Build shared rank and status fields
		build_rank_and_status(db, source)
		# Add vernacular names with the shared ColDP helper
		coldp_vernacular(vernacular_tsv, db, source)
		# Validate the normalized source
		validate(db, source)
		# Write the processed source parquet
		write_to_disc(db, source)

#
# 		World Flora Online - Royal Botanic Garden Edinburg
#		We use it to enhance our IPNI and Tropicos data with parents, acceptance status etc
#
#		New versions of this checklist are released every six months in June and December: this is release 2024-12.
#
#		"The history of data development for the WFO taxonomic backbone is given on the WFO Plant List background page. 
# 		Taxonomic names are incorporated into WFO from nomenclators International Plant Name Index (IPNI) for vascular plants, 
# 		and Tropicos for bryophytes. Taxonomic and nomenclatural updates are incorporated from the WFO's Taxonomic Expert Networks (TENs) 
# 		and the World Checklist of Vascular Plants (WCVP), facilitated by the Royal Botanic Gardens, Kew." https://wfoplantlist.org/background
#
#		TODO: Find best permanent download URI & format as in https://zenodo.org/records/14538251
#		TODO: Import tropicosId properly, check tplID usage
#
# Internal
from ..utils.log import mesologger
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import strip_rank_from_name, name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc, publication_filter


source = {
    "name": "wfo",
    "url": "https://files.worldfloraonline.org/Files/Rhakhis_dwc/original/_uber/_uber.zip",
    "use_aria": 5,
	"citation": '<a href="https://www.worldfloraonline.org" class="medium">World Flora Online</a>, R. f. d. Almeida, G. Anderson, G. c. Andrella, M. Anguiano, H. Antonio-domingues, W. h. Ardi, H. Atkins, J. j. Atwood, X. Aubriot, W. Baker, A. p. Balan, P. Barberá, F. Bartolucci, W. g. Berendsohn, R. j. b. Bernal, M. Bonifacino, L. Borges, J. c. Brinda, G. Brown, et al.  World Flora Online Plant List YYYY-MM. <a href="https://doi.org/10.5281/zenodo.7460141" class="medium">https://doi.org/10.5281/zenodo.7460141</a>'
}

# Main function called as asyncio Task from run.py
async def update_wfo(session):
	mesologger.info(f"############### Starting World Flora Online Update  ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_wfo(source)
	# Return the source dict containing processing outcomes
	return source
    
# Process a fresh source file
def process_wfo(source: dict):
	mesologger.info(f"Starting to process { source['latest_download'] }...") 
	# Resolve local source path (already ensured by fetch in S3 mode)
	source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
	# Load zipfile and duckdb
	with zipfile.ZipFile(source_path, 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load the initial tsv files
		taxa_csv = db.read_csv(zip.open('classification.csv'),parallel=True, ignore_errors=True)
		reference_csv = db.read_csv(zip.open('references.csv'),parallel=True)
		mesologger.info(f"Extracted contents of WFO archive zip")
		# Create merged table by selecting all fields we're interested and create placeholders for further data
		# Store incoming accepted-name references as backlinks so fuse can prefer WFO concepts used by the active graph
		db.execute(f"""
			CREATE TABLE wfo AS 
			-- Extract BHL title IDs from references.csv relation URLs, <1% populated
			WITH bhl_refs AS (
				SELECT taxonID, CAST(NULLIF(REGEXP_EXTRACT(lower(relation), 'bhl\\.title\\.(\\d+)', 1),'') AS UINTEGER) AS bhl_title
				FROM reference_csv WHERE lower(relation) LIKE '%/bhl.title.%'
			),
			-- Count active WFO rows that identify each candidate as their accepted concept
			accepted_backlinks AS (
				SELECT acceptedNameUsageID AS taxonID, COUNT(*)::UINTEGER AS backlinks
				FROM taxa_csv
				WHERE acceptedNameUsageID IS NOT NULL AND doNotProcess_reason IS NULL
				GROUP BY acceptedNameUsageID
			)
			SELECT 
				-- classification.csv taxonID (wfo-NNNN), ~1.7M rows after doNotProcess filter
				t.taxonID as id_raw,
				t.scientificName as name_raw,
				lower(t.scientificName) AS name_clean,
				-- 25 clean ranks: species ~1.2M, variety ~275k, subspecies ~85k
				t.taxonRank as rank_raw,
				CAST(NULL AS VARCHAR) AS rank_clean,
				-- ~734 families, 98% populated
			 	lower(t.family) AS family,
				-- ~45k genera, 99% populated
			 	lower(t.genus) AS genus,
				-- parent wfo-ID, only 26% populated
			 	t.parentNameUsageID AS parent_raw,
				-- 96% populated
				trim(t.scientificNameAuthorship) as author_raw,
				-- Synonym ~1M, Accepted ~450k, Unchecked ~220k
			 	t.taxonomicStatus AS status_raw,
				-- incoming accepted-name references distinguish active concepts from orphaned duplicate WFO rows
				COALESCE(ab.backlinks, 0) AS backlinks,
				-- scientificNameID holds IPNI LSIDs or Tropicos numeric IDs, parsed in wfo_external_ids
			 	t.scientificNameID AS links_raw,
				-- year from namePublishedIn parenthesized pattern, 71% populated
				CAST(NULLIF(regexp_extract(t.namePublishedIn, '\\((\\d{{4}})\\)', 1),'') AS USMALLINT) AS year,
				-- publication snippet from namePublishedIn, 95% populated
			 	trim(regexp_replace(REGEXP_EXTRACT(REGEXP_REPLACE(t.namePublishedIn, '^(in: |in )', ''), { publication_filter }, 1), '\\s+:\\s*$', '')) AS publication_short,
				r.bhl_title AS bhl_title,
				-- tropicosId column, only 8% populated
				CAST(t.tropicosId AS UINTEGER) AS tropicos_id
			FROM taxa_csv t
			LEFT JOIN bhl_refs r ON t.taxonID = r.taxonID
			LEFT JOIN accepted_backlinks ab ON t.taxonID = ab.taxonID
			-- doNotProcess rows are duplicates or junk non-alphanumeric names
			WHERE t.doNotProcess_reason IS NULL;
		""")
		# db.sql(f"""SELECT DISTINCT  list_distinct(list(doNotProcess_reason)), COUNT(doNotProcess_reason) FROM taxa_csv WHERE NOT starts_with(doNotProcess_reason,'Duplicate of wfo-') GROUP BY doNotProcess_reason ORDER BY COUNT(doNotProcess_reason) DESC""").show(max_rows=75)
		# db.sql("SELECT scientificName FROM taxa_csv WHERE NOT starts_with(doNotProcess_reason, 'Duplicate of wfo-')").show(max_rows=200)
		# Log
		mesologger.info(f"Loaded {db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} entries from { source['name'] } csv")
		# Remove ranks
		strip_rank_from_name(db,source)  
		# Find hybrids
		find_hybrids(db,source)  
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Get external IDs
		wfo_external_ids(db,source)
		# Delete weird edge cases
		wfo_cleanup(db,source)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)

# Extract IPNI and Tropicos IDs from scientificNameID (stored as links_raw)
# IPNI ~81% populated via LSID URN, Tropicos ~8% via bare numeric IDs
def wfo_external_ids(db: duckdb.DuckDBPyConnection, source: dict):
	db.execute(f"""
	ALTER TABLE wfo ADD COLUMN IF NOT EXISTS ipni_id VARCHAR;
	UPDATE wfo SET
		-- IPNI LSID URN pattern, note: synonyms can share the same IPNI ID as their accepted name
 		ipni_id = NULLIF(REGEXP_EXTRACT(links_raw, 'urn:lsid:ipni\\.org:names:([0-9]+-[0-9]+)', 1), ''),
		-- bare numeric IDs are Tropicos, backfill if tropicosId column was empty
		tropicos_id = COALESCE(tropicos_id,TRY_CAST(links_raw AS UINTEGER))
	WHERE links_raw LIKE 'urn:lsid:ipni.org:names:%' OR TRY_CAST(links_raw AS UINTEGER) IS NOT NULL;
	""")
	# db.sql(f"SELECT DISTINCT list_distinct(list(links_raw)), COUNT(links_raw) FROM wfo GROUP BY links_raw ORDER BY COUNT(links_raw) DESC;").show(max_rows=75)	
	
# WFO data quality: delete #NAME? Excel errors and #VALUE! author artifacts
def wfo_cleanup(db: duckdb.DuckDBPyConnection, source: dict):
	broken_names = len(db.execute("DELETE FROM wfo WHERE lower(name_raw) LIKE '#name%' RETURNING 1").fetchall())
	if broken_names > 0: mesologger.info(f"""Deleted {broken_names} rows with name #NAME? from WFO""")
	missing_authors = len(db.execute("UPDATE wfo SET author_raw = NULL WHERE author_raw LIKE '#%' RETURNING 1").fetchall())
	if missing_authors > 0: mesologger.info(f"""Removed {missing_authors} authors like #VALUE! from WFO""")

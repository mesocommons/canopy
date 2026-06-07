#	
#		Biodiversity Heritage Library
#		Main source for historic mentions and illustrations
#			
#		The BHL portal provides free access to hundreds of thousands of volumes, comprising over 
# 		60 million pages, from the 15th-21st centuries. Headquartered at the Smithsonian Libraries and 
# 		Archives in Washington, D.C., BHL operates as a worldwide consortium of natural history, botanical, 
# 		research, and national libraries working together to address this challenge by digitizing the 
# 		natural history literature held in their collections and making it freely available for open access as 
# 		part of a global “biodiversity community.”
#
# Internal
from ..utils.log import mesologger
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import gzip, os, ssl, zipfile
import aiohttp
from ..utils.filehandlers import get_file
# Load run-state helper to persist latest manual BHL download/process metadata
from ..utils.state import update_source_state
from ..utils.downloader import get_local_source_file, get_timestamp_remote
# Load shared storage proxy for local/S3 transparent file operations
from ..utils.s3 import storage

# DB
import duckdb
from ..utils.queries import name_cleanup, find_hybrids, validate, write_to_disc

# S3 base for BHL open data (original site moved behind Cloudflare)
BHL_S3 = "https://s3.us-east-2.amazonaws.com/bhl-open-data/data"
# Files we need from the S3 bucket
BHL_FILES = ['page.txt', 'pagename.txt', 'item.txt', 'title.txt', 'creator.txt']

source = {
    "name": "bhl",
    # Timestamp via S3 Last-Modified header on first data file
    "url": f"{BHL_S3}/page.txt.gz",
}

# Custom update since BHL moved behind Cloudflare but publishes data on S3 as individual gz files
async def update_bhl(session):
	mesologger.info(f"############### Starting Biodiversity Heritage Library Update  ###############")
	# Look for an existing local bhl zip
	local = get_local_source_file(source['name'])
	# Populate source dict with local version info if we have a previous download
	if local:
		# Store the local filename for later comparison
		source['latest_download'] = local
		# Extract YYYYMMDD timestamp from the filename
		source['timestamp_download'] = int(local.split('.')[1])
		# Ensure local processing path exists even when canonical copy lives in S3
		source['local_path'] = storage.ensure_local(os.path.join(SRC_DIR, local), SRC_DIR)
		mesologger.info(f"Latest local version of bhl is from {source['timestamp_download']}")
	# Fetch the remote timestamp from S3 Last-Modified header if downloads are enabled
	source['timestamp_remote'] = await get_timestamp_remote(source) if settings.CHECK_FOR_DOWNLOADS else 0
	# Log remote timestamp comparison like other datasets do
	if not source['timestamp_remote']:
		mesologger.warning(f"Unable to get a valid remote timestamp for bhl")
	elif source.get('timestamp_download') and source['timestamp_remote'] == source['timestamp_download']:
		mesologger.info(f"Latest remote version {source['timestamp_remote']} of bhl matches local {source['timestamp_download']}")
	elif source.get('timestamp_download') and source['timestamp_remote'] > source['timestamp_download']:
		mesologger.info(f"New remote version {source['timestamp_remote']} of bhl available.")
	else:
		mesologger.info(f"Latest remote version of bhl is from {source['timestamp_remote']}")
	# Validate existing local zip isn't corrupt from a previous crashed run
	if local and local.endswith('.zip'):
		try:
			# Try opening and reading the zip directory to confirm it's valid
			with zipfile.ZipFile(source.get('local_path') or f"{SRC_DIR}/{local}", 'r') as test:
				test.namelist()
		except (zipfile.BadZipFile, Exception):
			# Remove the corrupt zip so we re-download
			mesologger.info(f"Removing corrupt {local}")
			os.remove(f"{SRC_DIR}/{local}")
			# Also clean up any leftover temp files from the crashed run
			if os.path.isdir(SRC_DIR):
				for f in os.listdir(SRC_DIR):
					if f.startswith(local) and f.endswith('.tmp'): os.remove(f"{SRC_DIR}/{f}")
			# Reset local state so we trigger a fresh download
			local = None
			source.pop('latest_download', None)
			source.pop('timestamp_download', None)
			source.pop('local_path', None)
	# Compare local and remote timestamps to decide if we need a fresh download
	need_download = source.get('timestamp_remote') and (not local or source['timestamp_remote'] > source.get('timestamp_download', 0))
	# Download individual gz files from S3 and assemble into a zip matching the old format
	if need_download:
		# Use the remote timestamp as the version hash for the zip filename
		datehash = source['timestamp_remote']
		# Target zip path following standard naming convention
		target = f"{SRC_DIR}/bhl.{datehash}.zip"
		mesologger.info(f"Assembling BHL zip from S3 open data bucket...")
		# Create zip with same Data/ prefix structure as the old biodiversitylibrary.org download
		with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as zf:
			# Open a long-lived session for all 5 file downloads
			async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3600)) as dl:
				# Download each of the 5 TSV files we need
				for filename in BHL_FILES:
					mesologger.info(f"Downloading {filename}.gz from S3...")
					# Build a temp path next to the target zip for the compressed download
					gz_path = f"{target}.{filename}.tmp"
					# Fetch the gzipped file from S3
					async with dl.get(f"{BHL_S3}/{filename}.gz", ssl=ssl.SSLContext()) as resp:
						# Open temp file for writing compressed bytes to disc instead of holding in memory
						with open(gz_path, 'wb') as tmp:
							# Stream in 1MB chunks to keep memory usage low
							async for chunk in resp.content.iter_chunked(1024 * 1024):
								# Write each chunk to the temp file
								tmp.write(chunk)
					# Open the temp gz for decompressed reading
					with gzip.open(gz_path, 'rb') as gz_in:
						# Create the zip entry matching the old Data/ prefix layout (force_zip64 for files >4GB)
						with zf.open(f"Data/{filename}", 'w', force_zip64=True) as dest:
							# Read and write in 8MB chunks to avoid loading the full decompressed file into memory
							while chunk := gz_in.read(8 * 1024 * 1024):
								# Write decompressed chunk into the zip entry
								dest.write(chunk)
					# Clean up the temp gz file after it's been added to the zip
					os.remove(gz_path)
					mesologger.info(f"Added Data/{filename}")
		# Point the source dict at the newly created zip
		source['latest_download'] = f"bhl.{datehash}.zip"
		# Store the local processing path for this run
		source['local_path'] = target
		# Upload assembled zip to S3 when backend is active
		if storage.is_s3(): storage.upload(target, f"source/{source['latest_download']}")
		# Remove local assembled zip after successful S3 upload in download-only mode
		if storage.is_s3() and settings.DOWNLOAD_ONLY and os.path.isfile(target): os.remove(target)
		# Store the download timestamp for processed-file comparison
		source['timestamp_download'] = datehash
		mesologger.info(f"Built {target}")
	# Look up the latest processed parquet to see if reprocessing is needed
	processed = get_file(source['name'])
	# Store processed file info on source dict so package_release can find it
	if processed:
		source['latest_processed'] = processed
		source['timestamp_processed'] = int(processed.split('.')[1])
	# Extract processed timestamp or default to 0 if no processed file exists
	timestamp_processed = source.get('timestamp_processed', 0)
	# Reprocess if the download is newer than the last processed version
	need_process = source.get('timestamp_download', 0) > timestamp_processed
	# Run processor if we have new data or force flag is set
	if (need_process or settings.FORCE) and source.get('latest_download') and not settings.DOWNLOAD_ONLY: process_bhl(source)
	# Persist latest BHL source metadata in shared state manifest
	update_source_state(source, 'fetch')
	# Return the source dict containing processing outcomes
	return source
    
# Process a fresh source file
def process_bhl(source: dict):
	mesologger.info(f"Starting to process { source['latest_download'] }...") 
	# Resolve local source path (already ensured in update_bhl for S3 mode)
	source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
	# Load zipfile and duckdb
	with zipfile.ZipFile(source_path, 'r') as zip, duckdb.connect(':memory:') as db:
		# Almost guarantueed to need a temp dir:
		db.execute(f"SET temp_directory = '{ TMP_DIR }'")

		# Create the enum types
		db.execute("""
			CREATE TYPE page_type_enum AS ENUM (
				'Text', 'Illustration', 'Blank', 'Cover', 'Chart', 'Title Page', 'Index', 'Table of Contents', 'Foldout', 'Appendix', 
			 	'Map', 'Issue Start', 'List of Illustrations', 'Article Start', 'Article End', 'Errata', 'Specimen');
		""")
		
		db.execute("""
			CREATE TYPE lang_enum AS ENUM (
				'ENG', 'GER', 'FRE', 'LAT', 'CHI', 'SPA', 'ITA', 'DUT', 'UND', 'POR', 'SWE', 'JPN', 'DAN', 'RUS', 'HUN', 'NOR', 'CZE', 'MUL', 'ARA', 'POL', 
				'RUM', 'CAT', 'UKR', 'FIN', 'YID', 'SCR', 'FRM', 'GLE', 'HEB', 'OTA', 'GRE', 'GRC', 'BUL', 'SCC', 'ICE', 'OJI', 'AFR', 'WEL', 'URD', 'EST', 
				'GMH', 'PER', 'SAN', 'GUJ', 'FAO', 'MLG', 'HRV', 'HIN', 'IND', 'ROH', 'GLA');
		""")
		
		db.execute("""CREATE TYPE mention_type_enum AS ENUM ('first_mention', 'first_illustration', 'first_eastern', 'first_eastern_illustration');""")

		# Load the initial tsv files
		page_tsv = db.read_csv(zip.open('Data/page.txt'), parallel=True, sample_size=50000, dtype={'Year': 'VARCHAR'})
		names_tsv = db.read_csv(zip.open('Data/pagename.txt'), parallel=True)
		mesologger.info("Opened first BHL files")

		# Initial table - pagename.txt has ~202M rows, reduced to ~112M by grouping per item+name+type
		db.execute(f"""
			CREATE TABLE bhl AS
			SELECT 
				-- earliest page ID for this name+item+type combo
			 	CAST(MIN(p.PageID) AS UINTEGER) AS id_raw,
				-- cleaned name from pagename.txt NameConfirmed
				trim(regexp_replace(lower(n.NameConfirmed), '[^a-z ×]', '', 'g')) AS name_clean,
				-- item.txt join key for title/author/year enrichment
				CAST(p.ItemID AS UINTEGER) AS item_id,
				-- page type: Text, Illustration, Map etc (17 values)
				CAST(p.PageTypeName AS page_type_enum) AS type,
				-- publication year from page.txt, ~5.6M rows after first-mention reduction
			 	ANY_VALUE(TRY_CAST(NULLIF(REGEXP_EXTRACT(p.Year, '\\d{{4}}', 0),'') AS USMALLINT)) AS year
			FROM page_tsv p
			JOIN names_tsv n ON p.PageID = n.PageID
			GROUP BY p.ItemID, n.NameConfirmed, p.PageTypeName
			{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
		""")
		mesologger.info(f"Created base table with {db.execute('SELECT COUNT(*) FROM bhl').fetchone()[0]:,} name mentions from BHL")

		# Backfill missing years from item.txt (page.txt year is often null)
		item_tsv = db.read_csv(zip.open('Data/item.txt'), parallel=True, dtype={'Year': 'VARCHAR'})
		db.execute(f"""
			ALTER TABLE bhl ADD COLUMN title_id UINTEGER;
			-- item.txt has Year and TitleID for each ItemID
			UPDATE bhl b SET year = TRY_CAST(NULLIF(REGEXP_EXTRACT(i.Year, '\\d{{4}}', 0),'') AS USMALLINT), title_id = i.TitleID
			FROM item_tsv i WHERE b.year IS NULL and i.Year IS NOT NULL AND b.item_id = i.ItemID;
		""")
		mesologger.info(f"Added missing years from item.txt")
		# Mentions without any year are useless for timeline, drop them
		db.execute(f"DELETE FROM bhl WHERE year IS NULL;")
		mesologger.info(f"Reduced BHL to {db.execute('SELECT COUNT(*) FROM bhl').fetchone()[0]:,} rows after removing mentions without year")
		# Backfill title_id for rows that already had a year from page.txt
		db.execute(f"""
			UPDATE bhl b SET title_id = i.TitleID
			FROM item_tsv i WHERE b.title_id IS NULL AND b.item_id = i.ItemID;
		""")
		mesologger.info(f"Added remaining Item IDs")

		# Mark publications in eastern languages for separate first-mention tracking, ~8% of rows
		titles_tsv = db.read_csv(zip.open('Data/title.txt'), parallel=True)
		db.execute(f"""
			ALTER TABLE bhl ADD COLUMN eastern BOOLEAN;
			UPDATE bhl b
			SET eastern = (t.LanguageCode IN ('CHI', 'JPN', 'ARA', 'HEB', 'OTA', 'URD', 'PER', 'SAN', 'GUJ', 'HIN', 'IND'))
			FROM titles_tsv t WHERE b.title_id = t.TitleID;
		""")
		mesologger.info("Identified Eastern literature in BHL")
		# Find the earliest page for each name in 4 categories, used for history timeline in distill
		db.execute("""
			CREATE TEMP TABLE first_mentions AS
			-- First overall mentions (sorted by year, then ID)
			SELECT MIN(id_raw) AS id_raw, CAST('first_mention' AS mention_type_enum) AS mention_type, name_clean 
			FROM (SELECT * FROM bhl WHERE year IS NOT NULL ORDER BY year, id_raw) sorted 
			GROUP BY name_clean

			UNION ALL

			-- First illustrations (sorted by year, then ID)
			SELECT MIN(id_raw) AS id_raw, CAST('first_illustration' AS mention_type_enum) AS mention_type, name_clean 
			FROM (SELECT * FROM bhl WHERE year IS NOT NULL AND type = 'Illustration' ORDER BY year, id_raw) sorted 
			GROUP BY name_clean

			UNION ALL

			-- First eastern mentions (sorted by year, then ID)
			SELECT MIN(id_raw) AS id_raw, CAST('first_eastern' AS mention_type_enum) AS mention_type, name_clean 
			FROM (SELECT * FROM bhl WHERE year IS NOT NULL AND eastern = TRUE ORDER BY year, id_raw) sorted 
			GROUP BY name_clean

			UNION ALL

			-- First eastern illustrations (sorted by year, then ID)
			SELECT MIN(id_raw) AS id_raw, CAST('first_eastern_illustration' AS mention_type_enum) AS mention_type, name_clean 
			FROM (SELECT * FROM bhl WHERE year IS NOT NULL AND type = 'Illustration' AND eastern = TRUE ORDER BY year, id_raw) sorted 
			GROUP BY name_clean;
		""")
		mesologger.info(f"Found {db.execute('SELECT COUNT(*) FROM first_mentions').fetchone()[0]:,} first mentions in BHL")

		# Update the mention_types in the main table using a direct join
		db.execute("""
			ALTER TABLE bhl ADD COLUMN IF NOT EXISTS mention_type mention_type_enum;
			UPDATE bhl SET mention_type = fm.mention_type FROM first_mentions fm WHERE bhl.id_raw = fm.id_raw AND bhl.name_clean = fm.name_clean;
		""")
		mesologger.info(f"Added first mentions to main BHL table")
		db.execute("DROP TABLE first_mentions;")

		# Drop all non-first-mention rows — reduces ~112M to ~5.6M for the final output
		db.execute(f"DELETE FROM bhl WHERE mention_type IS NULL;")
		mesologger.info(f"Reduced BHL to {db.execute('SELECT COUNT(*) FROM bhl').fetchone()[0]:,} rows after removing non-first mentions")

		# Do our standard cleanup, hybrids first
		find_hybrids(db,source)
		# Generic cleanup
		name_cleanup(db,source)

		# Dedup after name_cleanup: keep one row per (name_clean, mention_type), prefer earliest year
		db.execute(f"""
			CREATE TEMP TABLE rows_to_keep AS
			SELECT DISTINCT ON (name_clean, mention_type) id_raw, name_clean, mention_type
			FROM bhl WHERE mention_type IS NOT NULL
			ORDER BY name_clean, mention_type, year, id_raw;
			DELETE FROM bhl 
			WHERE NOT EXISTS (
				SELECT 1 FROM rows_to_keep r
				WHERE bhl.id_raw = r.id_raw AND bhl.name_clean = r.name_clean AND bhl.mention_type = r.mention_type
			);
			DROP TABLE rows_to_keep;
		""")
		mesologger.info(f"Removed final duplicates from BHL")

		# Add publication title from title.txt, prefer ShortTitle, max 100 chars
		db.execute("""
			ALTER TABLE bhl ADD COLUMN IF NOT EXISTS title VARCHAR; 
			UPDATE bhl SET title = substring(COALESCE(t.ShortTitle, t.FullTitle),1,100)
			FROM titles_tsv t WHERE bhl.title_id = t.TitleID;
		""")
		mesologger.info(f"Added Book/Journal Titles to BHL data")
		# Aggregate creator names per title, ~91% of final rows get authors
		creator_tsv = db.read_csv(zip.open('Data/creator.txt'), parallel=True)
		db.execute("""
			ALTER TABLE bhl ADD COLUMN IF NOT EXISTS author_raw VARCHAR;
			WITH author_lookup AS (
				SELECT TitleID, SUBSTRING(STRING_AGG(CreatorName, ', '), 1, 100) AS authors
				FROM creator_tsv GROUP BY TitleID
			)
			UPDATE bhl SET author_raw = author_lookup.authors
			FROM author_lookup WHERE bhl.title_id = author_lookup.TitleID;
		""")
		mesologger.info(f"Added BHL authors")
		
		# Log
		mesologger.info(f"Loaded & enriched {db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} name mentions from { source['name'] } tsv")
		if settings.VERBOSE:
			db.sql(f"SELECT * FROM bhl WHERE mention_type = 'first_eastern'").show(max_rows=100)
			db.sql(f"DESCRIBE bhl").show(max_rows=20)		
			db.sql(f"SUMMARIZE bhl").show(max_rows=20)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)

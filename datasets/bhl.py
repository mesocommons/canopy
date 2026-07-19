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
		
		db.execute("""CREATE TYPE mention_type_enum AS ENUM ('first_mention', 'first_illustration', 'first_eastern', 'first_eastern_illustration', 'popular_illustration');""")

		# Load page and name streams without extracting their 13 GB of TSV payloads
		page_tsv = db.read_csv(zip.open('Data/page.txt'), parallel=True, sample_size=50000, dtype={'PageID': 'UINTEGER', 'ItemID': 'UINTEGER', 'Year': 'VARCHAR', 'PageTypeName': 'VARCHAR'})
		names_tsv = db.read_csv(zip.open('Data/pagename.txt'), parallel=True, dtype={'NameConfirmed': 'VARCHAR', 'PageID': 'UINTEGER'})
		mesologger.info("Opened BHL page and name files")
		# Load compact item and title metadata before the large name join so effective years and language buckets are available during reduction
		item_tsv = db.read_csv(zip.open('Data/item.txt'), parallel=True, dtype={'ItemID': 'UINTEGER', 'TitleID': 'UINTEGER', 'Year': 'VARCHAR'})
		titles_tsv = db.read_csv(zip.open('Data/title.txt'), parallel=True, dtype={'TitleID': 'UINTEGER', 'LanguageCode': 'VARCHAR'})
		# Materialize distinct item rows because BHL links 9k items to multiple bibliography titles
		db.execute("""
			CREATE TEMP TABLE item_rows AS
			SELECT DISTINCT
				CAST(ItemID AS UINTEGER) AS item_id,
				CAST(TitleID AS UINTEGER) AS title_id,
				TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{4}', 0), '') AS USMALLINT) AS item_year
			FROM item_tsv;
		""")
		# Materialize one language classification per title so item joins cannot multiply page candidates
		db.execute("""
			CREATE TEMP TABLE title_languages AS
			SELECT DISTINCT
				CAST(TitleID AS UINTEGER) AS title_id,
				COALESCE(LanguageCode IN ('CHI', 'JPN', 'ARA', 'HEB', 'OTA', 'URD', 'PER', 'SAN', 'GUJ', 'HIN', 'IND'), FALSE) AS eastern
			FROM titles_tsv;
		""")
		# Reject conflicting fallback years because choosing one arbitrarily could change event chronology
		year_conflicts = db.execute("SELECT count() FROM (SELECT item_id FROM item_rows GROUP BY item_id HAVING count(DISTINCT struct_pack(present := item_year IS NOT NULL, year := item_year)) > 1)").fetchone()[0]
		# Reject duplicate language values for one title because they would fan out the page join
		title_conflicts = db.execute("SELECT count() FROM (SELECT title_id FROM title_languages GROUP BY title_id HAVING count() > 1)").fetchone()[0]
		# Stop before the large source scan when compact lookups are not safe to join
		if year_conflicts or title_conflicts: raise RuntimeError(f"Ambiguous BHL lookups year_conflicts={year_conflicts} title_conflicts={title_conflicts}")
		# Track expected multi-title ambiguity without allowing it to produce multiple rows per item
		multi_title_items = db.execute("SELECT count() FROM (SELECT item_id FROM item_rows GROUP BY item_id HAVING count() > 1)").fetchone()[0]
		# Select one complete item row, preferring populated metadata and then the stable lowest title ID
		db.execute("""
			CREATE TEMP TABLE item_winners AS
			SELECT
				item_id,
				arg_min(
					struct_pack(title_id := title_id, item_year := item_year),
					struct_pack(
						missing_title := title_id IS NULL,
						missing_year := item_year IS NULL,
						title_id := COALESCE(title_id, 4294967295::UINTEGER),
						year := COALESCE(item_year, 65535::USMALLINT)
					)
				) AS item
			FROM item_rows
			GROUP BY item_id;
		""")
		# Build the one-row-per-item context used by page candidates
		db.execute("""
			CREATE TEMP TABLE item_context AS
			SELECT i.item_id, i.item.title_id AS title_id, i.item.item_year AS item_year, COALESCE(t.eastern, FALSE) AS eastern
			FROM item_winners i
			LEFT JOIN title_languages t ON t.title_id = i.item.title_id;
		""")
		mesologger.info(f"Built {db.execute('SELECT count() FROM item_context').fetchone()[0]:,} BHL item contexts after resolving {multi_title_items:,} multi-title items")
		# Reduce BHL's multi-type PageID rows before one page can enter both mention and illustration buckets
		db.execute(f"""
			CREATE TEMP TABLE page_records AS
			SELECT
				CAST(PageID AS UINTEGER) AS id_raw,
				arg_min(
					struct_pack(
						item_id := CAST(ItemID AS UINTEGER),
						page_type := CAST(PageTypeName AS page_type_enum),
						page_year := TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{{4}}', 0), '') AS USMALLINT)
					),
					struct_pack(
						not_illustration := PageTypeName IS DISTINCT FROM 'Illustration',
						page_type := COALESCE(PageTypeName, ''),
						missing_year := TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{{4}}', 0), '') AS USMALLINT) IS NULL,
						page_year := COALESCE(TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{{4}}', 0), '') AS USMALLINT), 65535::USMALLINT),
						item_id := CAST(ItemID AS UINTEGER)
					)
				) AS page
			FROM page_tsv
			WHERE PageID IS NOT NULL
			GROUP BY PageID
			{ 'LIMIT ' + str(settings.BACKBONE_LOOPS) if settings.BACKBONE_LOOPS > 0 else '' };
		""")
		mesologger.info(f"Reduced BHL page classifications to {db.execute('SELECT count() FROM page_records').fetchone()[0]:,} unique pages")
		# Assign dated candidates to chronological buckets and duplicate illustrations into one bounded quality-proxy pool
		db.execute("""
			CREATE TEMP TABLE raw_winners AS
			WITH page_candidates AS (
				SELECT
					p.id_raw,
					p.page.item_id AS item_id,
					p.page.page_type AS page_type,
					COALESCE(p.page.page_year, i.item_year) AS effective_year,
					i.title_id,
					COALESCE(i.eastern, FALSE) AS eastern
				FROM page_records p
				LEFT JOIN item_context i ON i.item_id = p.page.item_id
			), categorized_base AS (
				SELECT
					n.NameConfirmed AS name_raw,
					CAST(CASE
						WHEN p.eastern AND p.page_type = 'Illustration'::page_type_enum THEN 'first_eastern_illustration'
						WHEN p.eastern THEN 'first_eastern'
						WHEN p.page_type = 'Illustration'::page_type_enum THEN 'first_illustration'
						ELSE 'first_mention'
					END AS mention_type_enum) AS mention_type,
					struct_pack(
						id_raw := p.id_raw,
						item_id := p.item_id,
						page_type := p.page_type,
						year := p.effective_year,
						title_id := p.title_id,
						eastern := p.eastern
					) AS event,
					struct_pack(year := p.effective_year, page := p.id_raw) AS event_order,
					struct_pack(page := p.id_raw, year := p.effective_year) AS popular_order
				FROM names_tsv n
				JOIN page_candidates p ON p.id_raw = CAST(n.PageID AS UINTEGER)
				WHERE n.NameConfirmed IS NOT NULL AND p.effective_year IS NOT NULL
			), categorized AS (
				SELECT name_raw, mention_type, event, event_order, popular_order FROM categorized_base
				UNION ALL
				SELECT name_raw, 'popular_illustration'::mention_type_enum, event, event_order, popular_order
				FROM categorized_base
				WHERE event.page_type = 'Illustration'::page_type_enum
			)
			SELECT
				name_raw,
				mention_type,
				CASE
					WHEN mention_type = 'popular_illustration'::mention_type_enum THEN list_value(arg_min(event, popular_order))
					WHEN mention_type IN ('first_illustration'::mention_type_enum, 'first_eastern_illustration'::mention_type_enum) THEN arg_min(event, event_order, 2)
					ELSE list_value(arg_min(event, event_order))
				END AS events
			FROM categorized
			GROUP BY name_raw, mention_type;
		""")
		mesologger.info(f"Reduced BHL associations to {db.execute('SELECT count() FROM raw_winners').fetchone()[0]:,} raw-name event buckets")
		# Unpack chronological winners plus at most three low-ID illustration candidates before name normalization
		db.execute("""
			CREATE TABLE bhl AS
			SELECT
				event.id_raw AS id_raw,
				trim(regexp_replace(lower(name_raw), '[^a-z ×]', '', 'g')) AS name_clean,
				event.item_id AS item_id,
				event.page_type AS type,
				event.year AS year,
				event.title_id AS title_id,
				event.eastern AS eastern,
				mention_type
			FROM raw_winners
			CROSS JOIN UNNEST(events) AS candidate(event);
		""")
		# Detect and normalize hybrid markers after the 205-million-row association stream is reduced
		find_hybrids(db,source)
		# Apply shared rank-fragment, punctuation and whitespace cleanup to the compact winner set
		name_cleanup(db,source)
		# Select the absolute lowest-ID illustration first so it owns any collision with chronology buckets
		db.execute("""
			CREATE TEMP TABLE popular_winners AS
			SELECT
				name_clean,
				'popular_illustration'::mention_type_enum AS mention_type,
				arg_min(
					struct_pack(id_raw := id_raw, item_id := item_id, page_type := type, year := year, title_id := title_id, eastern := eastern, hybrid := hybrid, hybridpos := hybridpos),
					struct_pack(page := id_raw, year := year, hybrid := COALESCE(hybrid, FALSE), hybridpos := COALESCE(hybridpos, 255::UTINYINT))
				) AS event
			FROM bhl
			WHERE name_clean IS NOT NULL AND name_clean != '' AND mention_type = 'popular_illustration'::mention_type_enum
			GROUP BY name_clean;
			CREATE TEMP TABLE cleaned_winners AS
			SELECT
				p.name_clean,
				p.mention_type,
				arg_min(
					struct_pack(id_raw := p.id_raw, item_id := p.item_id, page_type := p.type, year := p.year, title_id := p.title_id, eastern := p.eastern, hybrid := p.hybrid, hybridpos := p.hybridpos),
					struct_pack(year := p.year, page := p.id_raw, hybrid := COALESCE(p.hybrid, FALSE), hybridpos := COALESCE(p.hybridpos, 255::UTINYINT))
				) AS event
			FROM bhl p
			WHERE p.name_clean IS NOT NULL AND p.name_clean != ''
				AND p.mention_type != 'popular_illustration'::mention_type_enum
				AND NOT EXISTS (
					SELECT 1 FROM popular_winners w
					WHERE w.name_clean = p.name_clean
						AND p.mention_type IN ('first_illustration'::mention_type_enum, 'first_eastern_illustration'::mention_type_enum)
						AND w.event.id_raw = p.id_raw
				)
			GROUP BY p.name_clean, p.mention_type;
			INSERT INTO cleaned_winners BY NAME SELECT * FROM popular_winners;
			CREATE OR REPLACE TABLE bhl AS
			SELECT
				event.id_raw AS id_raw,
				name_clean,
				event.item_id AS item_id,
				event.page_type AS type,
				event.year AS year,
				event.title_id AS title_id,
				event.eastern AS eastern,
				mention_type,
				event.hybrid AS hybrid,
				event.hybridpos AS hybridpos
			FROM cleaned_winners;
		""")
		mesologger.info(f"Reduced BHL to {db.execute('SELECT count() FROM bhl').fetchone()[0]:,} cleaned-name event buckets")

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
				SELECT TitleID, SUBSTRING(STRING_AGG(CreatorName, ', ' ORDER BY CreatorName), 1, 100) AS authors
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

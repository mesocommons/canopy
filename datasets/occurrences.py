#
# 		Occurrence Geo Data
#
#		The big one: 200GB zipped parquet file but the parquet files are neatly broken down in chunks
#		which we can easily fetch from the zip file and extract.
#
#		TODO: Surface elevation data
#		TODO: Add eventdate as first observation signal alongside BHL first mention
#
# Basics
from ..utils.log import mesologger
import os, io, aiohttp, json, asyncio
from datetime import datetime, timezone, timedelta
# Internal
from .. import TMP_DIR, GEO_DIR, PROCESSED_DIR, settings
from ..utils.downloader import aria_download
from ..utils.filehandlers import get_file
# Load shared storage proxy for local/S3 transparent file operations
from ..utils.s3 import storage
# File handling & DB
import duckdb
import zipfile
import polars as pl
# Auth
from aiohttp import BasicAuth
# Default user agent for GBIF
user_agent = 'Meso Plant Database/1.0 (bruno@meso.cloud)'
# URL settings
GBIF_API_HOST = 'https://api.gbif.org/v1/'
# Seconds between pending-file readiness probes
GBIF_RETRY_SECONDS = 20
# Maximum number of pending-file readiness probes before giving up this run
GBIF_FILE_READY_ATTEMPTS = 60

# Main function 
async def update_occurrences():
	# Resolve path to rolling occurrence parquet
	file = os.path.join(GEO_DIR, f"occurrences.parquet")
	# Ensure local rolling parquet exists when canonical copy is stored in S3
	if storage.is_s3() and storage.exists(file) and not os.path.isfile(file): storage.ensure_local(file, GEO_DIR)
	# Skip GBIF API update when credentials are missing
	if not settings.GBIF_USER or not settings.GBIF_PASSWORD:
		# Soft-skip when we already have a local occurrence baseline
		if storage.exists(file):
			mesologger.info('GBIF credentials missing, skipping occurrence update and reusing local occurrences.parquet')
			return
		# Hard-stop bootstrap when there is no local baseline at all
		mesologger.info('GBIF credentials missing, skipping occurrence bootstrap')
		mesologger.info('Register credentials at https://www.gbif.org/user/profile and set them in canopy/secrets.py')
		raise RuntimeError('No local GBIF occurrences.parquet and no GBIF credentials available')
	# Bootstrap when no processed occurrence parquet exists yet
	if not storage.exists(file):
		# Log bootstrap path
		mesologger.info(f"No processed GBIF occurrence dataset found")
		# Resolve the reusable bootstrap archive path
		bootstrap_zip = os.path.join(TMP_DIR, 'occurrences.zip')
		# Ignore stale or corrupt bootstrap archives from interrupted downloads
		if os.path.isfile(bootstrap_zip) and not is_valid_zip(bootstrap_zip):
			mesologger.info('Existing occurrences.zip is invalid, deleting and re-downloading')
			os.remove(bootstrap_zip)
		# Request a bootstrap archive only when no valid local copy remains
		if not os.path.isfile(bootstrap_zip):
			# Log API bootstrap request path
			mesologger.info('Requesting initial GBIF occurrences export via API (plants and fungi only)')
			# Get auth for entire session
			auth = BasicAuth(settings.GBIF_USER, settings.GBIF_PASSWORD)
			# Spawn async http session for bootstrap request polling and download
			async with aiohttp.ClientSession(auth=auth, headers={"User-Agent": user_agent}) as session:
				# Load or initialize occurrence manifest
				manifest = get_manifest() or {}
				# Request bootstrap export when there is no pending key yet
				if not manifest.get('current_download_key'):
					# Start a full initial export limited to required kingdoms and coordinate quality
					await request_update_from_gbif(session, manifest, initial=True)
				# Resolve the pending request key
				pending_key = manifest.get('current_download_key')
				# Hard-fail bootstrap if GBIF did not accept request creation
				if not pending_key: raise RuntimeError('Initial GBIF occurrence export request failed')
				# Resolve ready download URL for pending key
				url = await get_gbif_download_url(session, manifest)
				# Hard-fail bootstrap when export is still being prepared
				if not url: raise RuntimeError('Initial GBIF occurrence export not ready yet please retry in 1 to 2 hours')
				# Download pending initial export with readiness polling
				success = await download_pending_occurrence_file(session, url, 'occurrences.zip')
				# Hard-fail bootstrap when file is not ready/downloadable yet
				if not success: raise RuntimeError('Initial GBIF occurrence export not ready yet please retry in 1 to 2 hours')
		# Convert the available bootstrap archive into the rolling geoparquet baseline
		distill_occurrences()
		# Load the manifest only after the rolling parquet was written durably
		manifest = get_manifest() or {}
		# Commit the export request time when available without advancing state before processing
		manifest['latest_download'] = manifest.get('latest_download_request') or datetime.now(timezone.utc).isoformat()
		# Preserve the existing initial-bootstrap completion timestamp behavior
		manifest['initial_download'] = manifest.get('initial_download') or datetime.now(timezone.utc).isoformat()
		# Mark the successfully persisted bootstrap archive complete
		if manifest.get('current_download_key'): manifest['last_processed_download_key'] = manifest.get('current_download_key')
		# Persist all successful bootstrap state together
		save_manifest(manifest)
		# Stop after successful bootstrap
		return
	# Otherwise update existing baseline incrementally
	mesologger.info(f"Processed GBIF occurrence data found")
	# Run incremental update flow
	await get_latest_occurrences(file)

# Persist manifest JSON to disk in one place
# so all update/bootstrap paths keep state consistently

def save_manifest(manifest: dict):
	# Write manifest atomically from in-memory dict
	storage.write_json(os.path.join(GEO_DIR,'manifest.json'), manifest)

# Kick off and wait for an occurrence update
async def get_latest_occurrences(file):
	# Get auth for entire session
	auth = BasicAuth(settings.GBIF_USER, settings.GBIF_PASSWORD)
	# Spawn async http session
	async with aiohttp.ClientSession(auth=auth,headers={"User-Agent": user_agent}) as session:
		# Get current manifest
		manifest = get_manifest()
		# Soft-skip incremental updates if manifest is missing on existing local baseline
		if not manifest:
			mesologger.info(f"No GBIF occurrence manifest found, skipping incremental update")
			return
		# Check if we have any reason to update, either because we never requested a download yet or it's older than 24h
		if 'latest_download_request' in manifest and datetime.now(timezone.utc) - datetime.fromisoformat(manifest['latest_download_request']) <= timedelta(days=1):
			mesologger.info(f"Already requested occurrence update { manifest.get('current_download_key') } within the last 24 hours")
		# Otherwise request a new dataset download
		else: await request_update_from_gbif(session, manifest, initial=False)
		# Check if we have a pending update that hasn't been processed yet
		if 'last_processed_download_key' not in manifest or manifest.get('current_download_key') != manifest.get('last_processed_download_key'):
			# Try processing a pending update
			url = await get_gbif_download_url(session, manifest)
			# Soft-skip when GBIF is still preparing incremental export
			if not url:
				mesologger.info('Incremental GBIF occurrence export not ready yet, will retry in next run')
				return
			# Set a filename for pending incremental export
			filename = f"occurrence_update.{ manifest.get('current_download_key') }.zip"
			# Download with readiness checks (or reuse existing file)
			success = await download_pending_occurrence_file(session, url, filename)
			# Soft-skip when file is still not ready
			if not success:
				mesologger.info('Incremental GBIF occurrence export file not ready yet, will retry in next run')
				return
			# Process successful incremental zip into rolling geoparquet
			process_incremental_update(manifest, filename)

# Start a new GBIF batch job
async def request_update_from_gbif(session, manifest, initial=False):
	# Log request mode
	mesologger.info(f"Requesting {'initial' if initial else 'latest'} occurrences from GBIF...")
	# Build shared predicate base for both initial and incremental runs
	predicates = [
		{
			"type": "not",
			"predicate": {
				"type": "equals",
				"key": "HAS_GEOSPATIAL_ISSUE",
				"value": True
			}
		},
		{
			"type": "in",
			"key": "KINGDOM_KEY",
			"values": [0, 5, 6]
		},
		{
			"type": "equals",
			"key": "HAS_COORDINATE",
			"value": True
		},
		{
			"type": "equals",
			"key": "OCCURRENCE_STATUS",
			"value": "PRESENT"
		}
	]
	# Add MODIFIED cutoff only for incremental updates
	if not initial:
		# Get timestamp for latest occurrence we know
		timestamp = manifest.get('latest_download') or manifest.get('initial_download')
		# Skip incremental request when no baseline timestamp exists
		if not timestamp:
			mesologger.info('No occurrence baseline timestamp in manifest, skipping incremental request')
			return
		# Prepend modified cutoff for incremental delta request
		predicates.insert(0, {
			"type": "greaterThan",
			"key": "MODIFIED",
			"value": timestamp[:10]
		})
	# Build request body
	request_body = {
		"creator": settings.GBIF_USER,
		"sendNotification": False,
		"format": "SIMPLE_PARQUET",
		"predicate": {
			"type": "and",
			"predicates": predicates
		}
	}
	# Send request to GBIF async download endpoint
	async with session.post(GBIF_API_HOST + 'occurrence/download/request', json=request_body) as resp:
		# Log and stop when GBIF rejects request creation
		if resp.status != 201:
			mesologger.error(f"{ resp.status } error requesting {'initial' if initial else 'latest'} GBIF occurrences: { await resp.text() }")
			return
		# Remember pending request key
		current_download_key = await resp.text()
		manifest['current_download_key'] = current_download_key
		manifest['latest_download_request'] = datetime.now(timezone.utc).isoformat()
		save_manifest(manifest)
		# Log success
		mesologger.info(f"Successfully requested dataset creation { current_download_key } with {'initial' if initial else 'latest'} occurrences from GBIF")

# See if GBIF spawned a URL from which we eventually can download the data
async def get_gbif_download_url(session,manifest):
	# Get ID
	pending_id = manifest.get('current_download_key')
	# Sanity
	if not pending_id:
		mesologger.error(f"ERROR No pending update id found")
		return	
	# Log
	mesologger.info(f"Fetching pending GBIF occurrence update {pending_id}")
	# Give it 15 minutes
	end_time = datetime.now() + timedelta(minutes=10)
	# Start iterating
	while datetime.now() < end_time:
		try:
			# Send request
			async with session.get(GBIF_API_HOST + 'occurrence/download/request/' + pending_id, allow_redirects=False) as response:
				# If we get a 302 response and update URL
				if response.status == 302 and response.headers.get("Location"): return response.headers.get("Location")
				# Otherwise check if GBIF is trying to tell us something
				elif response.status == 200: mesologger.info(f"GBIF incremental update status is {await response.text()}")
				# If download was cancelled, is expired etc
				else: 
					# Log
					mesologger.info(f"GBIF incremental update invalid {await response.text()}")
					# Reset manifest
					manifest.pop('current_download_key', None)
					# Stop here
					return
		except Exception as e: mesologger.error(f"Error trying to retrieve GBIF incremental update {pending_id}: {e}")
		# Show progress
		mesologger.info(f"Update not yet ready, trying again in {GBIF_RETRY_SECONDS} seconds...")
		# Wait before next request
		await asyncio.sleep(GBIF_RETRY_SECONDS)

# Check zip integrity quickly before processing
# Returns True when zip central directory can be read, False otherwise

def is_valid_zip(path: str) -> bool:
	# Sanity check file presence
	if not os.path.isfile(path): return False
	# Validate zip central directory
	try:
		with zipfile.ZipFile(path, 'r') as z: z.namelist()
		return True
	except Exception: return False

# Wait for GBIF file readiness and download with aria
# Returns True when file exists and is valid, False when still not ready/failed
async def download_pending_occurrence_file(session, url, filename):
	# Resolve local file path once
	filepath = os.path.join(TMP_DIR, filename)
	# Reuse previously downloaded file only if zip is valid
	if is_valid_zip(filepath): return True
	# Remove stale/corrupt partial zip before retrying download
	if os.path.isfile(filepath): os.remove(filepath)
	# Wait for remote file availability
	for attempt in range(GBIF_FILE_READY_ATTEMPTS):
		# Probe download URL headers
		async with session.head(url) as head_resp:
			# Check if GBIF now serves a real file with length
			if head_resp.status == 200:
				content_length = head_resp.headers.get('Content-Length')
				if content_length and int(content_length) > 1000: break
		# Wait unless this was final retry
		if attempt < (GBIF_FILE_READY_ATTEMPTS - 1):
			mesologger.info(f"File not ready, checking again in {GBIF_RETRY_SECONDS} seconds...")
			await asyncio.sleep(GBIF_RETRY_SECONDS)
	# Stop when file was never ready within wait window
	else:
		mesologger.info(f"File still not ready after 20 minutes")
		return False
	# Wait for GBIF to report a stable file size before downloading
	expected_size = 0
	verify_after_delay = False
	for size_check in range(30):
		# Probe current content length
		async with session.head(url) as size_resp:
			# Read current file size from GBIF headers
			if size_resp.status == 200 and size_resp.headers.get('Content-Length'):
				current_size = int(size_resp.headers['Content-Length'])
			else: current_size = 0
		# Skip invalid/empty size responses
		if current_size <= 1000:
			mesologger.info(f"GBIF still adding to zip (currently {current_size / (1024**3):.1f}GB), retrying in {GBIF_RETRY_SECONDS} secs...")
			await asyncio.sleep(GBIF_RETRY_SECONDS)
			continue
		# Confirm delayed verification check if we previously saw matching sizes
		if verify_after_delay:
			# Proceed only if size remained unchanged after the longer wait
			if current_size == expected_size:
				mesologger.info(f"GBIF reports stable size {expected_size:,} bytes for {filename}")
				break
			# Reset verification state when size changed again
			verify_after_delay = False
			expected_size = current_size
			mesologger.info(f"GBIF still adding to zip (currently {current_size / (1024**3):.1f}GB), retrying in {GBIF_RETRY_SECONDS} secs...")
			await asyncio.sleep(GBIF_RETRY_SECONDS)
			continue
		# First matching check: pause briefly before final confirmation
		if current_size == expected_size:
			mesologger.info(f"GBIF zip looks complete, waiting {GBIF_RETRY_SECONDS} secs to verify...")
			verify_after_delay = True
			await asyncio.sleep(GBIF_RETRY_SECONDS)
			continue
		# Track latest size and keep polling
		expected_size = current_size
		mesologger.info(f"GBIF still adding to zip (currently {current_size / (1024**3):.1f}GB), retrying in {GBIF_RETRY_SECONDS} secs...")
		await asyncio.sleep(GBIF_RETRY_SECONDS)
	# Download the file via aria
	success = await aria_download(filename, url, 4, TMP_DIR)
	# Treat invalid/corrupt zip as unsuccessful download
	if success and not is_valid_zip(filepath):
		mesologger.warning('Download completed but zip validation failed, will retry later')
		return False
	# Return download outcome without advancing the processing watermark
	return bool(success)

# Shared extraction: iterate parquet chunks in GBIF zip, filter plantae/fungi with valid coords
# Produces temp occurrences table with canonical taxon plus raw GBIF taxon key for synonym auditing
def extract_from_zip(zip: zipfile.ZipFile, db: duckdb.DuckDBPyConnection):
	# Load spatial and spawn DB outside of file loop
	db.execute("""
		INSTALL spatial;
		LOAD spatial;
		CREATE TEMP TABLE occurrences (
			id UBIGINT,
			taxon UINTEGER,
			taxon_raw UINTEGER,
			synonym_for UINTEGER,
			location GEOMETRY,
			elevation SMALLINT,
			spatial_issue BOOLEAN DEFAULT FALSE
		);
	""")
	# Logging
	counter = 1
	total = len(zip.filelist)
	# Track extracted occurrence count for progress and final summary
	occurrence_count = 0
	# Iterate through all files
	for file in zip.filelist:
		# Ignore empty files
		if file.file_size == 0: continue
		# Read file as Polars dataframe
		df = pl.read_parquet(io.BytesIO(zip.read(file.filename)))
		# Add rows while parsing malformed taxonomy keys as NULL
		db.execute("""
			INSERT INTO occurrences BY NAME
			SELECT
				-- Unique occurrence ID
				gbifid AS id,
				-- Keep raw numeric GBIF taxon key for deterministic post-extract synonym mapping
				TRY_CAST(taxonkey AS UINTEGER) AS taxon_raw,
				-- Seed canonical taxon from raw key, then overwrite only synonym rows
				TRY_CAST(taxonkey AS UINTEGER) AS taxon,
				-- Fill synonym target key after extraction when raw key is a synonym
				CAST(NULL AS UINTEGER) AS synonym_for,
				-- 3 digits gives us about ~71m lon and ~111 meters lat, which is more than enough for 1-10km grids
				-- and 30 arcsecond (~594 meters lon x  ~926 meters lat) lookups
				ST_Point(ROUND(decimallongitude, 3),ROUND(decimallatitude, 3)) AS location,
				COALESCE(elevation,depth * -1) AS elevation,
				-- Add entries with issues as fallback but we filter most in later distillation
				list_contains(issue, 'HAS_GEOSPATIAL_ISSUE') AS spatial_issue
			-- Filter for plants and fungi with valid coordinates, excluding 0/0 null island junk
			FROM df WHERE kingdom IN ('Plantae','Fungi','Incertae sedis')
			AND decimallatitude IS NOT NULL AND decimallongitude IS NOT NULL
			AND NOT ST_Equals(ST_Point(decimallongitude, decimallatitude), ST_Point(0, 0));
		""")
		# Log, but query only every 10 files
		if counter % 10 == 0 or counter == 1: occurrence_count = db.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
		mesologger.info(f"Extracted {occurrence_count:,} occurrences from {counter} of {total} files", extra={'sameline': True})
		# Iterate 
		counter += 1
	# Refresh final row and unmatched-key counts in one scan for accurate logging
	occurrence_count, unmatched_taxon_count = db.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE taxon IS NULL) FROM occurrences").fetchone()
	# Print final extraction summary on the same carriage-return line as progress output
	mesologger.info(f"Successfully extracted {occurrence_count:,} occurrences from {total} files".ljust(96), extra={'sameline': True})
	# Report rows that downstream numeric taxonomy joins cannot match
	if unmatched_taxon_count: mesologger.warning(f"Extracted {unmatched_taxon_count:,} occurrences with NULL or unparseable taxonkeys")
	# Resolve synonyms to accepted GBIF keys once during ingest so geo stage can stay simple
	resolve_occurrence_taxonkeys(db)
	# Finish progress line with newline
	

# Resolve canonical GBIF occurrence taxon keys using processed GBIF acceptedNameUsage mapping
# Synonym rows get mapped to acceptedNameUsageID, non-synonyms keep their raw key
# This keeps geo stage lightweight and avoids heavy synonym joins during habitat processing
def resolve_occurrence_taxonkeys(db: duckdb.DuckDBPyConnection):
	# Resolve latest processed GBIF backbone parquet used for synonym mapping
	gbif_file = get_file('gbif', PROCESSED_DIR)
	# Fall back to raw keys when GBIF backbone is unavailable
	if not gbif_file:
		mesologger.warning('No processed GBIF parquet found, using raw occurrence taxon keys')
		db.execute("UPDATE occurrences SET taxon = taxon_raw")
		return
	# Build GBIF parquet path and URL for DuckDB reads (local path or s3 URL)
	gbif_path = os.path.join(PROCESSED_DIR, gbif_file)
	gbif_url = storage.parquet_url(gbif_path)
	# Configure DuckDB S3 settings when reading GBIF parquet from object storage
	if storage.is_s3(): storage.configure_duckdb(db)
	# Materialize compact synonym mapping table from processed GBIF backbone
	# Fall back gracefully when older processed gbif parquets do not yet include accepted_raw
	try:
		db.execute(f"""
			CREATE TEMP TABLE gbif_synonym_map AS
			SELECT
				CAST(id_raw AS UINTEGER) AS synonym_key,
				MIN(CAST(accepted_raw AS UINTEGER)) AS accepted_key
			FROM read_parquet('{gbif_url}')
			WHERE status_clean = 'synonym' AND accepted_raw IS NOT NULL
			GROUP BY 1;
		""")
		# Update synonym rows with scalar-subquery mapping (winner in ABTEST11)
		db.execute("""
			UPDATE occurrences
			SET
				synonym_for = (
					SELECT m.accepted_key
					FROM gbif_synonym_map m
					WHERE m.synonym_key = occurrences.taxon_raw
				),
				taxon = COALESCE((
					SELECT m.accepted_key
					FROM gbif_synonym_map m
					WHERE m.synonym_key = occurrences.taxon_raw
				), taxon_raw)
			WHERE taxon_raw IN (SELECT synonym_key FROM gbif_synonym_map);
		""")
		# Optionally collect mapped row count in verbose mode only to avoid an extra full scan
		if settings.VERBOSE:
			mapped_count = db.execute("SELECT COUNT(*) FROM occurrences WHERE synonym_for IS NOT NULL").fetchone()[0]
			mesologger.info(f"Mapped {mapped_count:,} occurrence rows to accepted GBIF synonym targets")
		# Otherwise keep log lightweight for large daily runs
		else: mesologger.info("Mapped synonym occurrence keys to accepted GBIF targets")
	except Exception as error:
		# Preserve raw key behavior when synonym mapping columns are unavailable
		mesologger.warning(f"GBIF synonym mapping unavailable, using raw occurrence taxon keys {type(error).__name__}: {error}")
		db.execute("UPDATE occurrences SET taxon = taxon_raw")

# Apply incremental GBIF download: merge new/updated occurrences into existing parquet
def process_incremental_update(manifest,filename):
	# Spawn zipfile and DuckDB
	with zipfile.ZipFile(os.path.join(TMP_DIR, filename), 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory before heavy joins/updates
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Use shared function to get updates
		extract_from_zip(zip,db)
		# Check if we even have any new occurrences
		new_occurrences = db.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
		if not new_occurrences or new_occurrences == 0:
			mesologger.info(f"No new occurrences in incremental update")		
		# Otherwise process them	
		else:
			# Load existing geoparquet into DuckDB for merge
			# Ensure rolling occurrence parquet is available locally for DuckDB read
			existing_path = storage.ensure_local(os.path.join(GEO_DIR,'occurrences.parquet'), GEO_DIR)
			existing_occurrence_parquet = db.read_parquet(existing_path)
			db.execute(f"""
				INSTALL spatial;
				LOAD spatial;
				CREATE TABLE existing_occurrences AS SELECT * FROM existing_occurrence_parquet;
			""")
			# Keep incremental merge compatible with old rolling files that predate synonym-mapped key columns
			db.execute("""
				ALTER TABLE existing_occurrences ADD COLUMN IF NOT EXISTS taxon UINTEGER;
				ALTER TABLE existing_occurrences ADD COLUMN IF NOT EXISTS taxon_raw UINTEGER;
				ALTER TABLE existing_occurrences ADD COLUMN IF NOT EXISTS synonym_for UINTEGER;
			""")
			count = db.execute("SELECT COUNT(*) FROM existing_occurrences").fetchone()[0]
			mesologger.info(f"Loaded {count:,} occurrences from existing {os.path.join(GEO_DIR,'occurrences.parquet')}")
			# Count incoming rows that match existing occurrence ids
			updated_count = db.execute("""
				SELECT COUNT(*)
				FROM occurrences AS source
				WHERE EXISTS (
					SELECT 1 FROM existing_occurrences AS target WHERE target.id = source.id
				);
			""").fetchone()[0]
			# Upsert: update changed occurrences, then insert new ones
			db.execute("""
				UPDATE existing_occurrences
				SET taxon = occurrences.taxon,
					taxon_raw = occurrences.taxon_raw,
					synonym_for = occurrences.synonym_for,
					location = occurrences.location,
					elevation = occurrences.elevation,
					spatial_issue = occurrences.spatial_issue
				FROM occurrences WHERE existing_occurrences.id = occurrences.id;
			""")
			# Log how many incoming occurrences mapped to existing ids
			mesologger.info(f"Updated {updated_count:,} existing occurrences")
			# Insert occurrences not already in existing set
			db.execute("""
				INSERT INTO existing_occurrences BY NAME
				SELECT * FROM occurrences AS source
				WHERE NOT EXISTS (SELECT 1 FROM existing_occurrences AS target WHERE target.id = source.id);
			""")
			mesologger.info(f"Added {db.execute("SELECT COUNT(*) FROM existing_occurrences").fetchone()[0]-count:,} new occurrences")
			# Check data
			if settings.VERBOSE: db.sql("SELECT * FROM occurrences").show(max_rows=200)
			# Write to disc using COPY as write_parquet() doesn't do geoparquet well
			db.sql(f"""COPY existing_occurrences TO '{os.path.join(GEO_DIR, "occurrences.parquet")}';""")
			# Upload refreshed rolling occurrence parquet to S3 when backend is active
			if storage.is_s3(): storage.upload(os.path.join(GEO_DIR, 'occurrences.parquet'))
			mesologger.info(f"Wrote updated occurrences.parquet to {GEO_DIR}")
		# Commit the export request time only after extraction and any rolling parquet upload succeeded
		manifest['latest_download'] = manifest['latest_download_request']
		# Mark the successfully persisted incremental archive complete
		if manifest.get('current_download_key'): manifest['last_processed_download_key'] = manifest.get('current_download_key')
		# Persist the safe request cutoff and processed key together
		save_manifest(manifest)
		mesologger.info(f"Updated manifest, occurrence update complete")

# Initial bootstrap: extract full GBIF occurrence snapshot (~200GB zip) into geoparquet
def distill_occurrences():		
	mesologger.info(f"############### Processing GBIF occurrences ###############")
	# Pointer to zip and spawn db
	with zipfile.ZipFile(os.path.join(TMP_DIR, f"occurrences.zip"), 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory before heavy extract/mapping writes
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Use shared function
		extract_from_zip(zip,db)
		# Write to disc using COPY as write_parquet() doesn't do geoparquet well
		db.sql(f"""COPY occurrences TO '{os.path.join(GEO_DIR, "occurrences.parquet")}';""")
		# Upload freshly distilled rolling occurrence parquet to S3 when backend is active
		if storage.is_s3(): storage.upload(os.path.join(GEO_DIR, 'occurrences.parquet'))
		mesologger.info(f"Wrote occurrences.parquet to {GEO_DIR}")

# Load occurrence manifest (tracks download keys and processing state)
def get_manifest() -> dict:
	# Try fetching manifest
	try: 
		return storage.read_json(os.path.join(GEO_DIR,'manifest.json'))
	# Error logging
	except FileNotFoundError: mesologger.error(f"No GBIF occurrence manifest found")
	except json.JSONDecodeError: mesologger.error(f"GBIF occurrence manifest corrupted")	

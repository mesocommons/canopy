# Keep download-related helpers centralized for all dataset handlers

# Load async HTTP client, async file writes, filesystem, and regex helpers
from .log import mesologger
import aiohttp, aiofiles, os, re
# Load datetime parsing for Last-Modified headers
from datetime import datetime
# Load URL/path suffix extraction helpers
from pathlib import PurePath

# Load canopy source directory constant and runtime settings
from .. import SRC_DIR, settings
# Load shared storage proxy for local/S3 transparent file operations
from ..utils.s3 import storage

# Compare remote/local timestamps and download newer source file when needed
async def pull(session: aiohttp.ClientSession, source: dict) -> bool:
	# Populate local timestamp when caller did not provide one
	if not source.get('timestamp_local'):
		# Resolve newest local source file for this dataset prefix
		current = get_local_source_file(source['name'])
		# Update source state when a local file exists
		if current:
			# Keep latest local filename for downstream processing
			source['latest_download'] = current
			# Parse YYYYMMDD timestamp from filename convention
			source['timestamp_local'] = int(current.split('.')[1])
	# Populate remote timestamp when caller did not provide one
	if not source.get('timestamp_remote'):
		# Use ChecklistBank API metadata endpoint when HEAD lacks timestamp headers
		if 'api.checklistbank.org/dataset/' in source['url']:
			# Resolve timestamp and citation via ChecklistBank JSON metadata
			await get_timestamp_checklistbank(session, source)
		# Use Last-Modified (or fallback range request) for regular hosts
		else: source['timestamp_remote'] = await get_timestamp_remote(source)
	# Log when remote timestamp could not be determined
	if source['timestamp_remote'] == 0: mesologger.warning(f"Unable to get a valid remote timestamp for { source['name'] }")
	# Log comparison details when both local and remote timestamps are available
	elif all([source.get('timestamp_local'), source.get('timestamp_remote')]):
		# Report newly available remote version
		if source['timestamp_remote'] > source['timestamp_local']: mesologger.info(f"New remote version { source['timestamp_remote'] } of { source['name'] } available.")
		# Report up-to-date local cache state
		elif source['timestamp_remote'] == source['timestamp_local']: mesologger.info(f"Latest remote version { source['timestamp_remote'] } of { source['name'] } matches local { source['timestamp_local'] }")
		# Report unexpected remote rollback state
		elif source['timestamp_remote'] < source['timestamp_local']: mesologger.info(f"Latest remote version { source['timestamp_remote'] } of { source['name'] } older than local { source['timestamp_local'] }")
	# Log remote timestamp when no local baseline exists yet
	else: mesologger.info(f"Latest remote version of { source['name'] } is from { source['timestamp_remote'] }")
	# Download when no local file exists or remote version is newer
	if not source.get('latest_download') or source['timestamp_remote'] > source['timestamp_local']:
		# Start file download and capture status code
		status = await download_file(session, source)
		# Return True only when download completed successfully
		if status == 200: return True
	# Return False when no new download was needed or download failed
	return False

# Download one source file and store it as dataset.YYYYMMDD.suffix
async def download_file(session: aiohttp.ClientSession, source: dict) -> int | None:
	# Protect download workflow so one source failure does not crash the whole run
	try:
		# Import delete helper lazily to avoid circular import at module load
		from ..utils.filehandlers import delete_older_files
		# Initialize timestamp placeholder
		datehash = None
		# Unpack source metadata used for download naming and URL
		url, filename, datehash = source['url'], source['name'], source['timestamp_remote']
		# Build target filename from dataset name, timestamp, and known/derived suffix
		target_file = f"{ filename }.{ datehash }{ source.get('suffix') or PurePath(url).suffix or '.zip' }"
		# Build full destination path inside canopy source dir
		target_path = f"{ SRC_DIR }/{ target_file }"
		# Route to aria-style parallel downloads for sources with connection hints
		if 'use_aria' in source and isinstance(source['use_aria'], int):
			# Stream directly to S3 with parallel ranged workers when S3 backend is active
			if storage.is_s3():
				# Build destination key under source prefix
				target_key = f"source/{target_file}"
				# Stream HTTP response into multipart S3 upload using source parallelism hint
				await storage.stream_to_s3_parallel(url, target_key, session, source['use_aria'])
				# Delete older source versions after successful streamed upload
				delete_older_files(filename, datehash, SRC_DIR)
				# Store downloaded timestamp for downstream processing checks
				source['timestamp_download'] = datehash
				# Store downloaded filename for downstream processing paths
				source['latest_download'] = target_file
				# Return synthetic HTTP-like success status for streamed path
				return 200
			# Execute aria2 download with configured connection count in local mode
			success = await aria_download(target_file, url, source['use_aria'])
			# Abort when aria2 download failed
			if not success: return None
			# Keep placeholder branch for future aria-specific timestamp extraction
			if not datehash:
				# No-op for now because datehash is expected from remote timestamp lookup
				pass
			# Remove older local files after successful download
			delete_older_files(filename, datehash, SRC_DIR)
			# Store downloaded timestamp for downstream processing checks
			source['timestamp_download'] = datehash
			# Store downloaded filename for downstream processing paths
			source['latest_download'] = target_file
			# Return synthetic HTTP-like success status for aria path
			return 200
		# Use aiohttp direct download path for non-aria sources
		else:
			# Start HTTP GET request with permissive SSL context matching existing behavior
			# Execute request with permissive TLS verification for source compatibility
			async with session.get(url, ssl=False) as response:
				# Open destination file in binary write mode
				async with aiofiles.open(target_path, mode='wb') as file:
					# Log download start details
					mesologger.info(f"Downloading { filename } from { url }")
					# Read full response payload into memory (legacy behavior)
					data = await response.read()
					# Backfill datehash from response headers when needed
					if not datehash: datehash = get_datehash_from_response(response)
					# Log write target filename
					mesologger.info(f"Writing { target_file }")
					# Persist payload to local source file
					await file.write(data)
					# Upload completed local file to S3 when S3 backend is active
					if storage.is_s3(): storage.upload(target_path, f"source/{target_file}")
					# Remove local file after successful S3 upload in download-only mode
					if storage.is_s3() and settings.DOWNLOAD_ONLY and os.path.isfile(target_path): os.remove(target_path)
					# Remove older local files after successful write
					delete_older_files(filename, datehash, SRC_DIR)
					# Store downloaded timestamp for downstream processing checks
					source['timestamp_download'] = datehash
					# Store downloaded filename for downstream processing paths
					source['latest_download'] = target_file
					# Return HTTP response status for success/failure handling upstream
					return response.status
	# Handle missing header-related cases explicitly
	except KeyError as e: mesologger.error(f"Unable to get { e } Header, skipping download of { filename }.")
	# Catch all other download failures and keep pipeline alive
	except Exception as e:
		# Log unexpected exception details for debugging
		mesologger.error(f"Unhandled file download exception {type(e).__name__ } { e }.")

# Ensure aria-managed source files are complete before processing stage
async def aria_ready(source: dict, dir: str | None = None) -> bool:
	# Default to canopy source directory when caller did not override
	if not dir: dir = SRC_DIR
	# Abort when source state does not include latest downloaded filename
	if not source.get('latest_download'):
		# Log missing source filename state for diagnostics
		mesologger.info(f"aria_ready missing latest_download for {source.get('name', 'unknown source')}")
		# Return False so caller skips processing
		return False
	# Build expected file path
	file_path = os.path.join(dir, source['latest_download'])
	# Handle S3-backed artifacts via object existence and size checks
	if storage.is_s3():
		# Return false when remote source object is missing
		if not storage.exists(file_path): return False
		# Return false when remote source object is empty
		if storage.size(file_path) == 0: return False
		# Return true when remote source object exists with non-zero size
		return True
	# Build aria sidecar path used to track partial download state
	sidecar_path = file_path + '.aria2'
	# Resume download when aria sidecar indicates partial state
	if os.path.isfile(sidecar_path):
		# Log resume attempt for operator visibility
		mesologger.info(f"Found aria2 sidecar for {source['latest_download']}, resuming download")
		# Resume using dataset-specific connection settings
		success = await aria_download(source['latest_download'], source['url'], source.get('use_aria') or 4, dir)
		# Retry once after deleting stale sidecar when first resume fails
		if not success:
			# Log stale-sidecar recovery path
			mesologger.error(f"Resume failed for {source['latest_download']}, removing stale sidecar and retrying once")
			# Remove stale sidecar to clear broken aria state
			try: os.remove(sidecar_path)
			# Log sidecar cleanup failure but continue retry attempt
			except Exception as e: mesologger.info(f"Unable to remove stale aria2 sidecar {sidecar_path} {type(e).__name__ } { e }")
			# Retry resume once after stale sidecar cleanup
			success = await aria_download(source['latest_download'], source['url'], source.get('use_aria') or 4, dir)
			# Abort when retry still fails
			if not success: return False
	# Abort when expected source file is still missing after resume checks
	if not os.path.isfile(file_path):
		# Log missing file state for diagnostics
		mesologger.info(f"aria_ready missing source file {file_path}")
		# Return False so caller skips processing
		return False
	# Abort when source file exists but is empty
	if os.path.getsize(file_path) == 0:
		# Log zero-byte file state for diagnostics
		mesologger.info(f"aria_ready source file has 0 bytes {file_path}")
		# Return False so caller skips processing
		return False
	# Return True when aria-managed file is present and non-empty
	return True

# Download large files with aria2 (also used by occurrence update workflow)
async def aria_download(filename: str, url: str, connections: int, dir: str | None = None) -> bool:
	# Default to canopy source directory when caller did not override
	if not dir: dir = SRC_DIR
	# Log aria invocation details for operator visibility
	mesologger.info(f"Downloading { filename } from { url } using aria2 with { connections } connections to {dir}")
	# Import asyncio locally to preserve existing structure
	import asyncio
	# Build aria2 command with resume support and periodic progress summaries
	cmd = [
		'aria2c', url, f'--dir={ dir }', f'--out={ filename }', '--log-level=debug', '--stderr', '--disable-ipv6', '--file-allocation=none',
		f'--max-connection-per-server={ connections }', '--continue=true', '--console-log-level=notice', '--summary-interval=5'
	]
	# Spawn aria2 subprocess and capture stdout/stderr streams
	process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
	# Collect stderr lines for failure summaries
	error_lines = []
	# Stream stderr lines in real-time for progress and failure logs
	while True:
		# Read one stderr line from aria process
		line = await process.stderr.readline()
		# Stop loop when process closed stderr stream
		if not line: break
		# Decode stderr bytes into string line
		line_str = line.decode('utf-8').strip()
		# Keep full line history for failure diagnostics
		error_lines.append(line_str)
		# Skip empty lines
		if line_str:
			# Skip summary separator lines to keep logs compact
			if "Download Progress Summary" in line_str: continue
			# Print only progress/notice lines that help operators monitor downloads
			if ('[#' in line_str and 'DL:' in line_str and 'CN:' in line_str) or 'NOTICE' in line_str:
				# Emit aria progress line with filename context
				mesologger.info(f"ARIA2 progress for { filename } { line_str }")
	# Wait until aria process exits
	await process.wait()
	# Capture final process exit code
	exit_code = process.returncode
	# Handle aria failures with summarized stderr output
	if exit_code != 0:
		# Keep only last lines when stderr output is very large
		last_errors = error_lines[-10:] if len(error_lines) > 10 else error_lines
		# Join selected lines into readable multiline block
		error_str = "\n".join(last_errors)
		# Log aria exit code and tail of stderr output
		mesologger.error(f"aria2c failed with exit code { exit_code }:\n{ error_str }")
		# Return None/False-equivalent for failure branch compatibility
		return None
	# Sleep briefly so file buffers are fully flushed before downstream checks
	await asyncio.sleep(3)
	# Return success to caller
	return True

# Return latest local source file for a dataset prefix, deleting empty artifacts
def get_local_source_file(starts_with: str) -> str | None:
	# Track most recent timestamp found while scanning source dir
	most_recent = None
	# Track filename matching most recent timestamp
	latest_file = None
	# Iterate all files in source backend directory
	for file in storage.list_files(SRC_DIR, prefix=starts_with + '.'):
		# Build full path for size checks and cleanup
		file_path = os.path.join(SRC_DIR, file)
		# Process only files that match expected dataset prefix
		if file.startswith(starts_with + '.'):
			# Ignore interrupted download/assembly sidecars so they cannot become canonical sources
			if file.endswith('.tmp') or file.endswith('.aria2'): continue
			# Split source filename while allowing multi-dot suffixes like .sql.gz
			parts = file.split('.')
			# Ignore malformed filenames that lack dataset.timestamp.suffix shape
			if len(parts) < 3: continue
			# Ignore files where the timestamp segment is not a numeric datehash
			if not parts[1].isdigit(): continue
			# Skip zero-byte cleanup for S3-backed listings
			if storage.is_s3() and storage.size(file_path) == 0: continue
			# Process local zero-byte cleanup when using filesystem backend
			if not storage.is_s3() and os.path.isfile(file_path):
				# Remove zero-byte leftovers from interrupted downloads
				if os.path.getsize(file_path) == 0:
					# Log and delete empty file so it cannot poison timestamp selection
					mesologger.info(f"{file_path} has 0 bytes, ignoring and deleting")
					# Delete empty file artifact
					os.remove(file_path)
					# Continue scanning other candidate files
					continue
			# Parse timestamp segment from dataset filename convention
			timestamp = file.split('.')[1]
			# Seed latest-file tracking on first valid candidate
			if not all([most_recent, latest_file]):
				# Store first candidate timestamp
				most_recent = timestamp
				# Store first candidate filename
				latest_file = file
			# Update latest-file tracking when newer timestamp is found
			if most_recent < timestamp:
				# Store newer timestamp
				most_recent = timestamp
				# Store filename for newer timestamp
				latest_file = file
	# Return newest matching filename or None when nothing matched
	return latest_file

# Resolve remote YYYYMMDD timestamp from HTTP headers
async def get_timestamp_remote(source: dict) -> int:
	# Keep this request short because it only fetches metadata
	timeout = aiohttp.ClientTimeout(30)
	# Build optional user-agent header for remote timestamp probes
	headers = {'User-Agent': settings.HTTP_USER_AGENT} if settings.HTTP_USER_AGENT else None
	# Use a dedicated short-lived session for timestamp checks
	async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
		# Try HEAD request first for cheapest metadata lookup
		response = await session.head(source['url'], ssl=False, allow_redirects=True)
		# Return parsed datehash when HEAD succeeded
		if response.status == 200: return get_datehash_from_response(response)
		# Fallback to tiny ranged GET for hosts that reject HEAD
		response = await session.get(source['url'], ssl=False, allow_redirects=True, headers={'Range': 'bytes=0-0'})
		# Close body immediately so payload is not downloaded fully
		response.close()
		# Return parsed datehash when fallback returned content headers
		if response.status in (200, 206): return get_datehash_from_response(response)
		# Return 0 when no usable timestamp response could be obtained
		else: return 0

# Resolve ChecklistBank dataset timestamp/citation from metadata endpoint
async def get_timestamp_checklistbank(session: aiohttp.ClientSession, source: dict):
	# Build optional user-agent header for ChecklistBank metadata requests
	headers = {'User-Agent': settings.HTTP_USER_AGENT} if settings.HTTP_USER_AGENT else None
	# Query checklist dataset metadata endpoint derived from download URL
	async with aiohttp.ClientSession(headers=headers) as session, session.get(source['url'].rsplit('/', 1)[0]) as response:
		# Parse metadata response only when request succeeded
		if response.status == 200:
			# Decode JSON metadata payload
			json_content = await response.json()
			# Store issued date as YYYYMMDD timestamp when available
			if json_content.get('issued'): source['timestamp_remote'] = int(json_content.get('issued').replace("-", ""))
			# Store plain-text citation stripped of HTML tags when available
			if json_content.get('citation'): source['citation'] = re.sub(r'<[^>]+>', '', json_content.get('citation'))
		# Log metadata request failures for diagnostics
		else:
			# Report HTTP status for failed metadata lookup
			mesologger.error(f" Error fetching { source['name'] } ChecklistBank version XML {response.status}")

# Parse Last-Modified header into YYYYMMDD integer datehash
def get_datehash_from_response(response: aiohttp.ClientResponse) -> int:
	# Use Last-Modified header when server provides it
	if 'Last-Modified' in response.headers:
		# Parse RFC-style Last-Modified timestamp
		timestamp = datetime.strptime(response.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
		# Return compact YYYYMMDD integer used in filename versioning
		return int(timestamp.strftime('%Y%m%d'))
	# Log missing timestamp header for diagnostics
	else:
		# Report missing Last-Modified header values to help source debugging
		mesologger.info(f"Unable to find Last-Modified in Headers { response.headers }.")

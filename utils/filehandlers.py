# Load filesystem helpers, timing utilities, system access, and dynamic imports
from .log import mesologger
import os, importlib

# Load canopy path constants and runtime settings
from .. import PROCESSED_DIR, SRC_DIR, TMP_DIR, RELEASES_DIR, settings
# Load download helpers used by source fetch logic
from ..utils.downloader import pull, get_local_source_file
# Load shared storage proxy for local/S3 transparent file operations
from ..utils.s3 import storage
# Load run-state helper to persist latest source download/process metadata
from ..utils.state import update_source_state

# Fetch source metadata, optionally download updates, and decide whether processing is needed
async def fetch(session, source: dict) -> bool:
	# Fetch the latest downloaded file
	latest_download = get_local_source_file(source['name'])
	if latest_download: 
		source['latest_download'] = latest_download
		source['timestamp_download'] = int(latest_download.split('.')[1])
		mesologger.info(f"Latest local version of { source['name'] } is from { source['timestamp_download'] }")
	else: mesologger.info(f"No local { source['name'] } version available")
	# Track whether this fetch call downloaded a newer source artifact
	source['download_updated'] = False
	# Check for remote version
	if settings.CHECK_FOR_DOWNLOADS:
		# Try downloading when a newer remote source exists
		source['download_updated'] = bool(await pull(session, source))
		# Log successful source refresh for checkpoint gating visibility
		if source['download_updated']: mesologger.info(f"Successfully fetched new remote version of { source['name'] }.")
	# Resolve processed metadata before hydrating source contents so filename comparisons stay metadata-only
	if not source.get('latest_processed'):
		# Fetch the latest processed filename from the active storage backend
		latest_processed = get_file(source['name'])
		# Record the processed version when one exists
		if latest_processed:
			source['latest_processed'] = latest_processed
			source['timestamp_processed'] = int(latest_processed.split('.')[1])
	# Check if we even have something to process at this point
	# Use explicit None check so legacy datehash 0 files still count as available local sources
	if source.get('timestamp_download') is None: return False
	# Decide from versioned filenames whether source contents are needed by a processor
	needs_processing = not source.get('timestamp_processed') or source.get('timestamp_processed') < source.get('timestamp_download')
	# Prepare a local source path only when the run will consume source contents
	if source.get('latest_download'):
		# Build canonical source path under canopy source directory
		source_path = os.path.join(SRC_DIR, source['latest_download'])
		# Preserve download-only metadata without pulling canonical S3 contents back locally
		if settings.DOWNLOAD_ONLY: source['local_path'] = source_path
		# Hydrate source contents only for stale/missing processed data or explicit forced processing
		elif settings.FORCE or needs_processing: source['local_path'] = storage.ensure_local(source_path, SRC_DIR)
	# Persist latest known source metadata into shared state manifest
	# Mark fetch stage only when this run actually downloaded a newer source file
	if source.get('download_updated'): update_source_state(source, 'fetch')
	# Otherwise update metadata only and keep the previous successful stage marker
	else: update_source_state(source)
	# Report stale processed data after the metadata-only comparison
	if needs_processing:
		mesologger.info(f"Processed { source['name']} version outdated, we have { source.get('timestamp_processed') } but { source.get('timestamp_download') } is available.")
		# Let importer know that processing needs to be done
		return True
	# Skip processing when the processed filename already matches the source version
	return False

# Collect latest processed parquet per dataset for fuse fallback runs
def get_latest_processed() -> dict | None:
	sources = {} 
	datasets = []
	# Go through processed entries from active storage backend
	for file in storage.list_files(PROCESSED_DIR, suffix='.parquet'):
		# Only consider parquet files
		if not file.endswith('.parquet'): continue
		# Get the dataset name
		name = file.split('.')[0]
		# Add to list
		if not name in datasets: datasets.append(name)
	# Go through dataset
	for dataset in datasets:
		# Skip pipeline outputs before the sources insert below or they leak into the manifest
		if dataset in ['geo']: continue
		item = get_file(dataset)
		# Append it to dict
		if item: sources[dataset] = { 
			'name': dataset, 
			'latest_processed': item, 
			'timestamp_processed': item.split('.')[1]
		}
		# Import dataset module to get citation metadata for release manifest
		module = importlib.import_module(f"importer.canopy.datasets.{dataset}")
		if module: 
			# Also handle Tropicos edge cases where we have multiple sources
			source_dict = (getattr(module, "source", None) or (getattr(module, "sources", [])[:1] or [None])[0])
			if source_dict and source_dict.get('citation'): sources[dataset]['citation'] = source_dict.get('citation')
	# Return
	return sources	


# Return newest parquet matching a dataset prefix in the target directory
def get_file(starts_with: str, dir=None) -> str | None:
	# Default to processed dir
	if not dir: dir = PROCESSED_DIR
	newest_file = None
	# Go through candidate files from active storage backend
	for file in storage.list_files(dir, prefix=str(starts_with + '.'), suffix='.parquet'):
		# See if we have a match
		if file.count('.') == 2 and os.path.splitext(file)[1] == '.parquet':
			# Compare timestamps
			if not newest_file or int(file.split('.')[1]) > int(newest_file.split('.')[1]):
				# Assign the latest we found
				newest_file = file
	# Return None or our newest file
	return newest_file

# Delete older versioned files for one dataset after successful write/download
def delete_older_files(filename: str, datehash: str, dir) -> None:
	dir = dir or SRC_DIR
	# Prevent the most stupid mistakes
	if len(str(dir)) < 3:
		mesologger.warning(f"WARNING, TRIED TO DELETE FILES IN { dir }")
		return
	try:
		for file in storage.list_files(dir, prefix=filename + '.'):
			# Build full path for deletion via storage backend
			full_path = os.path.join(dir, file)
			# Ignore other files, make sure to use delimiting dot as we have wikispecies-foo etc
			if not file.startswith(filename + '.'): continue
			# Compare hashes, this shouldn't be larger but lets leave it in anyway for now
			if int(file.split('.')[1]) >= int(datehash): continue
			# Delete if we made it all the way here
			mesologger.info(f"Deleting old file { dir }/{ file }")
			storage.delete(full_path)
	except Exception as e:
		mesologger.error(f"Unable to delete { filename } {type(e).__name__ } { e }.")	

# Check whether a specific release folder exists in the target release directory
def check_release(release,dir=None):
	# Default to staging dir
	if not dir: dir = RELEASES_DIR
	# Resolve release manifest path as canonical release-exists check
	release_path = os.path.join(dir, release, 'manifest.json')
	# Return true when release manifest exists in active storage backend
	return storage.exists(release_path)

# Match valid release folder naming (YYYYMMDD-hash), shared across release lookup helpers
_release_pattern = None

# Return compiled release folder regex once per process
def _get_release_pattern():
	# Cache compiled regex to avoid re-parsing per call
	global _release_pattern
	# Build pattern on first access
	if _release_pattern is None:
		import re
		_release_pattern = re.compile(r'^\d{8}-[a-f0-9]+$')
	# Return compiled pattern for shared use
	return _release_pattern

# List valid release folder names in the given dir sorted newest-last for lex-based ordering
def _list_releases(release_dir=None):
	# Default to canopy releases dir
	if not release_dir: release_dir = RELEASES_DIR
	# Filter directory entries to valid release folder names only
	pattern = _get_release_pattern()
	# Return sorted list so callers can pick max or predecessor by index
	return sorted([entry for entry in storage.list_dirs(release_dir) if pattern.match(entry)])

# Load manifest dict for one specific release version, or None when absent/corrupted
def get_release(version, release_dir=None):
	import json
	# Abort early on missing version input
	if not version: return None
	# Default to canopy releases dir
	if not release_dir: release_dir = RELEASES_DIR
	# Build canonical manifest path for the requested version
	manifest_path = os.path.join(release_dir, version, 'manifest.json')
	# Return None when manifest does not exist in active storage backend
	if not storage.exists(manifest_path):
		mesologger.info(f"No release manifest found for {version} in {release_dir}")
		return None
	# Try reading and parsing the manifest file
	try: return storage.read_json(manifest_path)
	# Log missing manifest path as an error for visibility
	except FileNotFoundError: mesologger.error(f"No manifest found in {version}")
	# Log corrupted manifest so operators can investigate storage
	except json.JSONDecodeError: mesologger.error(f"{version} release manifest corrupted")
	# Explicit None when any read error occurred
	return None

# Load latest release manifest based on YYYYMMDD-hash release folder naming
def get_latest_release(release_dir=None):
	# List valid release folders sorted ascending so lex-max is last
	releases = _list_releases(release_dir)
	# Log and return nothing when no valid releases exist
	if not releases:
		mesologger.info(f"No release found in {release_dir or RELEASES_DIR}")
		return None
	# Delegate manifest loading to shared primitive
	return get_release(releases[-1], release_dir)

# Load manifest of the release immediately preceding current_version by lex sort
def get_previous_release(current_version, release_dir=None):
	# Abort early on missing current version input
	if not current_version: return None
	# List valid release folders sorted ascending
	releases = _list_releases(release_dir)
	# Keep only releases strictly older than current by lex sort
	older = [entry for entry in releases if entry < current_version]
	# Return None when no predecessor exists so callers can emit all-new diff
	if not older: return None
	# Delegate manifest loading to shared primitive using most recent predecessor
	return get_release(older[-1], release_dir)

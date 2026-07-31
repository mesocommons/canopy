# Load traceback helpers for readable failure output
from .utils.log import mesologger, mesologger_setup, mesologger_flush
import traceback
# Load process exit helper for non-zero failure signaling
import sys
# Load CLI argument parsing
import argparse
# Load async runtime primitives
import asyncio
# Load async HTTP session client for download handlers
import aiohttp
# Load canopy settings proxy and settings builder
from . import settings, build_settings
# Load storage proxy for startup backend status logging
from .utils.s3 import storage
# Load state checkpoint helper for minimal stage-completion timestamps
from .utils.state import set_checkpoint

# Define source execution order so fusion gets expected dependencies
sources = ['ipni', 'fungorum', 'wcvp', 'powo', 'wfo', 'col', 'colxr', 'tropicos', 'mycobank', 'bhl', 'gbif', 'wikidata', 'wikispecies', 'inaturalist', 'iucn', 'ncbi']

# Run canopy stages based on CLI flags and return release metadata when available
async def main(argv=None):
	# Initialize CLI parser for standalone canopy usage
	parser = argparse.ArgumentParser()
	# Allow scoping to a single dataset for faster iteration/debugging
	parser.add_argument('-d', '--dataset', help='Specify dataset to import')
	# Enable verbose intermediate table output for diagnostics
	parser.add_argument('-v', '--verbose', action='store_true', help='Show analytics and table status in intermediary steps')
	# Force recomputation even when timestamp checks would skip work
	parser.add_argument('-f', '--force', action='store_true', help='Force import even if data exists')
	# Write TSV sidecars in addition to parquet outputs
	parser.add_argument('--csv', action='store_true', help='Write processed files as csv, in addition to parquet')
	# Activate debug profile with reduced workload knobs
	parser.add_argument('--debug', action='store_true', help='Fast partial processing for local dev loops')
	# Download new source files without processing/fusion
	parser.add_argument('--download', action='store_true', help='Download source datasets only')
	# Execute dataset processing stage
	parser.add_argument('--process', action='store_true', help='Process source datasets')
	# Execute fusion stage from processed source outputs
	parser.add_argument('--fuse', action='store_true', help='Fuse latest processed data')
	# Execute geospatial enrichment stage
	parser.add_argument('--geo', action='store_true', help='Geospatial processing')
	# Execute API-backed enrichment stage (Wikipedia abstracts)
	parser.add_argument('--apis', action='store_true', help='Update API-backed enrichment datasets like Wikipedia abstracts')
	# Execute litmus validation stage against packaged release artifacts
	parser.add_argument('--litmus', action='store_true', help='Run litmus checks against release parquet and write summary to manifest')
	# Execute diff stage that compares current release against previous or explicit baseline
	parser.add_argument('--diff', action='store_true', help='Compute per-source diff against previous release or explicit baseline')
	# Override diff baseline with an explicit release version for admin comparisons against current_published
	parser.add_argument('--diff-against', dest='diff_against', metavar='VERSION', help='Diff against this specific release version instead of most recent predecessor')
	# Enable S3-compatible storage backend for canopy data paths
	parser.add_argument('--s3', action='store_true', help='Use configured S3 storage backend instead of local storage')
	# Parse CLI args (or injected argv from wrapper)
	args = parser.parse_args(argv)
	# Configure canopy logger for standalone and wrapper subprocess runs
	mesologger_setup('CANOPY')
	# Build runtime settings class from CLI profile/flags
	runtime = build_settings(args)
	# Mark pure download mode so dataset handlers skip processing work
	runtime.DOWNLOAD_ONLY = bool(args.download and not args.process)
	# Detect default no-flag canopy run and explicit stage-selection runs
	run_full_default = not any([args.download, args.process, args.fuse, args.geo, args.apis, args.litmus, args.diff])
	# Disable remote download checks for explicit stage runs unless download stage is requested
	runtime.CHECK_FOR_DOWNLOADS = bool(args.download) if not run_full_default else runtime.CHECK_FOR_DOWNLOADS
	# Publish runtime settings globally for canopy modules
	settings.set_config(runtime)
	# Announce canopy start for long-running logs
	mesologger.info('Starting canopy pipeline')
	# Log active storage backend mode after runtime settings are initialized
	if runtime.USE_S3 and storage.is_s3(): mesologger.info('Using S3 as storage backend')
	# Log graceful fallback when S3 was requested but config is incomplete
	elif runtime.USE_S3 and not storage.is_s3(): mesologger.warning('S3 not available, check config/secrets and run update.sh, falling back on local storage')
	# Log default local backend mode
	else: mesologger.info('Using local storage backend')
	# Set a shared request timeout for direct HTTP operations
	# Full download runs can exceed 2 hours due large source throttling (for example Wikidata)
	timeout = aiohttp.ClientTimeout(total=60 * 60 * 8)
	# Protect full pipeline execution with top-level error handling
	try:
		# Track stage-level failures so wrapper can stop on canopy errors
		had_errors = False
		# Collect per-source processing metadata for downstream fusion/diff
		results = {}
		# Run source stage when explicitly requested, during download-only mode, or in full default flow
		run_processing = args.process or args.download or run_full_default
		# Start dataset execution stage when requested
		if run_processing:
			# Build optional user-agent header set for outbound source requests
			session_headers = {'User-Agent': settings.HTTP_USER_AGENT} if settings.HTTP_USER_AGENT else None
			# Reuse one HTTP session across all dataset handlers
			async with aiohttp.ClientSession(timeout=timeout, headers=session_headers) as session:
				# Handle grouped async exceptions while keeping per-dataset tracebacks
				try:
					# Iterate in stable source order expected by later stages
					for source in sources:
						# Skip non-selected sources when dataset filter is active
						if args.dataset and args.dataset != source: continue
						# Build module path dynamically for current dataset
						module_path = f'{__package__}.datasets.{source}'
						# Build expected async update function name
						func_name = f'update_{source}'
						# Import dataset module lazily to avoid paying all import costs upfront
						module = __import__(module_path, fromlist=[func_name])
						# Resolve dataset update coroutine
						update_func = getattr(module, func_name)
						# Execute dataset update coroutine
						result = await update_func(session)
						# Keep source manifest data for fuse/diff stage input
						if result is not None: results[result['name']] = result
					# Guard against typos when dataset filter references unknown source names
					if args.dataset and args.dataset not in sources:
						# Report invalid dataset selection to the caller
						mesologger.error('Invalid dataset specified')
						# Fail fast so wrapper can stop and report invalid CLI input
						raise ValueError('Invalid dataset specified')
				# Handle cooperative cancellation cleanly
				except* asyncio.CancelledError:
					# Mark grouped cancellation as failure for wrapper orchestration
					had_errors = True
					# Log cancellation reason for operator visibility
					mesologger.warning('Tasks cancelled - shutting down...')
				# Report grouped stage exceptions without losing stack traces
				except* Exception as eg:
					# Mark grouped dataset failures so process exits non-zero
					had_errors = True
					# Announce grouped dataset failures
					mesologger.error('Some tasks failed:')
					# Print each underlying exception from the exception group
					for e in eg.exceptions:
						# Emit short exception summary
						mesologger.error(f'- {type(e).__name__}: {str(e)}')
						# Emit full traceback for root-cause debugging
						traceback.print_exception(type(e), e, e.__traceback__)
		# Exit after downloads when explicitly running download-only mode
		if runtime.DOWNLOAD_ONLY:
			# Mark authoritative download checkpoint only for full-source runs with at least one refreshed source
			if not args.dataset:
				# Count refreshed sources from fetch metadata used by orchestrator checkpoint gating
				updated_sources = [name for name, source in results.items() if source.get('download_updated')]
				# Stamp checkpoint only when at least one source downloaded a newer file
				if updated_sources:
					set_checkpoint('download')
					mesologger.info(f"Download checkpoint updated after {len(updated_sources)} refreshed sources")
				# Keep previous checkpoint unchanged when no source changed remotely
				else: mesologger.info('No source updates downloaded, download checkpoint unchanged')
			# Explain why follow-up stages are skipped
			mesologger.info('Download-only run complete, skipping process/fuse/geo/apis/litmus steps')
			# No release object is produced in download-only runs
			return None
		# Initialize release metadata container for downstream stages
		release = None
		# Run fusion stage when explicitly requested or during full default flow
		if args.fuse or run_full_default:
			# Isolate fusion errors so other diagnostics still print cleanly
			try:
				# Import fusion entrypoint lazily to avoid heavy import cost on download-only runs
				from .pipeline.fuse import fuse
				# Execute fusion with currently collected source metadata
				release = fuse(results)
			# Handle user aborts during long fusion queries
			except KeyboardInterrupt:
				# Mark cancellation as failure for wrapper orchestration
				had_errors = True
				# Confirm clean cancellation to operator logs
				mesologger.warning('Fusion cancelled by user.')
			# Surface fusion failures with full traceback context
			except Exception as e:
				# Mark fusion failure so process exits non-zero
				had_errors = True
				# Emit concise error summary for quick scanning
				mesologger.error(f'Error during fusion: {type(e).__name__}: {str(e)}')
				# Emit full traceback for debugging
				traceback.print_exception(type(e), e, e.__traceback__)
		# Abort follow-up stages immediately when earlier stages failed
		if had_errors:
			# Emit fatal stage summary before aborting pipeline run
			mesologger.critical('Canopy stage errors detected')
			# Raise to preserve non-zero process semantics
			raise RuntimeError('Canopy stage errors detected')
		# Run geospatial stage when explicitly requested or during full default flow
		if args.geo or run_full_default:
			try:
				# Import geo stage lazily to avoid optional dependency cost unless needed
				from .pipeline.geo import compute_geospatial
				# Execute geospatial enrichment against the chosen release
				await compute_geospatial(release)
			except Exception as e:
				# Mark geo failure so process exits non-zero
				had_errors = True
				# Emit concise error summary for quick scanning
				mesologger.error(f'Error during geo: {type(e).__name__}: {str(e)}')
				# Emit full traceback for debugging
				traceback.print_exception(type(e), e, e.__traceback__)
		# Run API enrichment stage when explicitly requested or during full default flow
		if args.apis or run_full_default:
			try:
				# Import Wikipedia updater lazily for optional API dependency paths
				from .datasets.wikipedia import update_wikipedia
				# Execute API refresh/update workflow
				update_wikipedia(release)
			except Exception as e:
				# Mark API stage failure so process exits non-zero
				had_errors = True
				# Emit concise error summary for quick scanning
				mesologger.error(f'Error during apis: {type(e).__name__}: {str(e)}')
				# Emit full traceback for debugging
				traceback.print_exception(type(e), e, e.__traceback__)
		# Run litmus when explicitly requested or during full default flow.
		if args.litmus or run_full_default:
			try:
				# Import litmus runner lazily to keep startup costs low.
				from .pipeline.litmus import run as run_litmus_checks
				# Execute litmus against current release or latest packaged release.
				run_litmus_checks(release)
			except Exception as e:
				# Mark litmus failure so process exits non-zero.
				had_errors = True
				# Emit concise error summary for quick scanning.
				mesologger.error(f'Error during litmus: {type(e).__name__}: {str(e)}')
				# Emit full traceback for debugging.
				traceback.print_exception(type(e), e, e.__traceback__)
		# Run diff when explicitly requested or during full default flow
		if args.diff or run_full_default:
			try:
				# Import diff runner lazily to keep startup costs low
				from .pipeline.diff import run as run_diff
				# Execute diff against current release, honoring --diff-against override when provided
				run_diff(release, diff_against=args.diff_against)
			except Exception as e:
				# Mark diff failure so process exits non-zero
				had_errors = True
				# Emit concise error summary for quick scanning
				mesologger.error(f'Error during diff: {type(e).__name__}: {str(e)}')
				# Emit full traceback for debugging
				traceback.print_exception(type(e), e, e.__traceback__)
		# Abort with non-zero exit semantics when any stage failed
		if had_errors:
			# Emit fatal stage summary before aborting pipeline run
			mesologger.critical('Canopy stage errors detected')
			# Raise to preserve non-zero process semantics
			raise RuntimeError('Canopy stage errors detected')
		# Return release metadata for wrapper orchestration and distill handoff
		return release
	# Catch any unhandled runtime failure in canopy flow
	except Exception as e:
		# Emit concise top-level failure summary
		mesologger.error(f'Unhandled exception {type(e).__name__}: {str(e)}')
		# Emit full traceback for postmortem debugging
		traceback.print_exception(type(e), e, e.__traceback__)
		# Re-raise so CLI process exits non-zero and wrapper can stop
		raise
	# Catch cancellation propagated to top-level runner
	except asyncio.exceptions.CancelledError:
		# Confirm cancellation in logs
		mesologger.warning('Pipeline cancelled.')

# Run canopy when invoked as a module script
if __name__ == '__main__':
	# Configure canopy logger before bootstrap logging
	mesologger_setup('CANOPY')
	# Wrap standalone execution with explicit shutdown/error logging
	try:
		# Start async canopy main coroutine
		asyncio.run(main())
	# Handle Ctrl+C shutdown cleanly
	except KeyboardInterrupt:
		# Confirm graceful stop in logs
		mesologger.info('Shutting down canopy...')
	# Catch any unhandled bootstrap/runtime error in standalone mode
	except Exception as e:
		# Emit concise top-level error message
		mesologger.error(f'Unhandled error {type(e).__name__}: {e}.')
		# Emit stack for debugging unexpected top-level failures
		traceback.print_stack()
		# Flush Sentry before short-lived process exit
		mesologger_flush()
		# Exit non-zero so wrapper orchestration can stop safely
		sys.exit(1)
	# Always flush Sentry logs after successful standalone runs too
	finally:
		# Flush any queued Sentry logs without changing process outcome
		mesologger_flush()

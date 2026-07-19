# Benchmark a direct early reduction for BHL history event buckets.
#
# Production currently materializes roughly 112 million item/name/type rows before reducing
# them to history events. It also combines MIN(PageID) with an unrelated ANY_VALUE(year).
# This probe instead assigns every complete page/name candidate to one disjoint bucket and
# uses arg_min over a lexicographic (effective year, page ID) key. DuckDB documents arg_min
# as its efficient top-row-per-group aggregate, while window functions buffer their full input.
#
# The default run keeps one deterministic page-ID partition so query shape can be tested
# safely. Pass --full only after the sampled path has validated cardinality and runtime.
import argparse
# Resolve the wall-clock UTC year for hard upper-bound validation.
from datetime import datetime, timezone
# Locate versioned source and baseline files without hard-coding release dates.
import glob
# Build cross-platform paths and create the dedicated spill directory.
import os
# Validate operator-provided memory limits before embedding them in DuckDB SET statements.
import re
# Poll process memory and spill files while blocking DuckDB queries execute.
import threading
# Measure every materialization so full-source regressions are visible.
import time
# Open the assembled BHL source archive without extracting its large TSV members.
import zipfile

# Run all source reductions inside DuckDB rather than materializing rows in Python.
import duckdb
# Track real process RSS because DuckDB notes that memory_limit does not cover all process memory.
import psutil

# Reuse production name normalization after the large source has been reduced.
from importer.canopy.utils.queries import find_hybrids, name_cleanup

# Resolve paths relative to the canopy package root.
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Use the latest assembled BHL source already hydrated from canonical S3 storage.
SOURCE = sorted(glob.glob(os.path.join(BASE_DIR, 'data', 'source', 'bhl.*.zip')))[-1]
# Compare full runs with the latest processed production artifact.
BASELINE = sorted(glob.glob(os.path.join(BASE_DIR, 'data', 'processed', 'bhl.*.parquet')))[-1]
# Keep large-query spill files in canopy's ignored temp tree rather than the repository root.
TMP_DIR = os.path.join(BASE_DIR, 'data', 'temp', 'bhl-event-abtest')
# Preserve the current Eastern-language proxy exactly.
EASTERN_LANGUAGES = "('CHI', 'JPN', 'ARA', 'HEB', 'OTA', 'URD', 'PER', 'SAN', 'GUJ', 'HIN', 'IND')"
# Require title-range corroboration for older chronology candidates.
YEAR_FLOOR = 1600
# Apply the requested hard wall-clock upper bound.
CURRENT_YEAR = datetime.now(timezone.utc).year
# Keep representative names visible in every report.
SENTINELS = ['quercus alba', 'amanita muscaria', 'salix alba', 'arabidopsis thaliana', 'arabis thaliana']
# Name the working table like production so shared cleanup helpers can operate unchanged.
SOURCE_INFO = {'name': 'bhl'}

# Print one clearly separated benchmark section.
def section(title):
	# Flush immediately because full scans can run for several minutes.
	print(f"\n############### {title} ###############", flush=True)

# Format elapsed wall time for one completed operation.
def elapsed(start):
	# Return seconds with one decimal place for compact comparisons.
	return f"{time.perf_counter() - start:.1f}s"

# Continuously record peak process memory and transient DuckDB spill files.
class ResourceMonitor:
	# Prepare one monitor for the current Python process and configured spill directory.
	def __init__(self, spill_dir):
		# Resolve the process once so polling does not rediscover it.
		self.process = psutil.Process()
		# Store the only directory where DuckDB is allowed to spill.
		self.spill_dir = spill_dir
		# Signal monitor shutdown without racing the query thread.
		self.stop_event = threading.Event()
		# Run polling in a daemon so an unexpected interpreter exit cannot hang.
		self.thread = threading.Thread(target=self._run, daemon=True)
		# Track peak resident process memory in bytes.
		self.peak_rss = 0
		# Track peak bytes present in DuckDB's spill directory.
		self.peak_spill = 0
		# Track peak simultaneous spill-file count.
		self.peak_spill_files = 0

	# Start sampling before DuckDB opens its in-memory database.
	def start(self):
		# Launch the background polling loop.
		self.thread.start()

	# Measure current RSS and recursively total spill files.
	def _sample(self):
		# Update the high-water process resident-set size.
		self.peak_rss = max(self.peak_rss, self.process.memory_info().rss)
		# Reset one sample's spill totals.
		spill_bytes = 0
		# Reset one sample's file count.
		spill_files = 0
		# Walk only the dedicated ignored spill directory.
		for root, _, files in os.walk(self.spill_dir):
			# Inspect every transient DuckDB file visible in this sample.
			for filename in files:
				# Build the full path for a safe size lookup.
				path = os.path.join(root, filename)
				# Tolerate files DuckDB deletes between listing and stat.
				try: spill_bytes += os.path.getsize(path)
				# Ignore only the expected transient disappearance race.
				except FileNotFoundError: continue
				# Count files that survived the size lookup.
				spill_files += 1
		# Retain spill high-water marks even when DuckDB later cleans the files.
		self.peak_spill = max(self.peak_spill, spill_bytes)
		# Retain the largest simultaneous file count.
		self.peak_spill_files = max(self.peak_spill_files, spill_files)

	# Poll twice per second while DuckDB runs blocking queries.
	def _run(self):
		# Continue until the main thread signals completion.
		while not self.stop_event.wait(0.5): self._sample()
		# Take one final sample during shutdown.
		self._sample()

	# Stop polling and wait for the final sample to finish.
	def stop(self):
		# Signal the daemon loop to exit.
		self.stop_event.set()
		# Join promptly so result reporting sees final peak values.
		self.thread.join(timeout=5)

	# Return files DuckDB failed to remove after its connection closed.
	def remaining_files(self):
		# Collect paths relative to the configured spill root.
		remaining = []
		# Walk the complete spill tree after database teardown.
		for root, _, files in os.walk(self.spill_dir):
			# Add every remaining file with its current size.
			for filename in files:
				# Build the full remaining path.
				path = os.path.join(root, filename)
				# Keep relative paths concise in reports.
				remaining.append((os.path.relpath(path, self.spill_dir), os.path.getsize(path)))
		# Return a stable path-ordered list.
		return sorted(remaining)

# Convert bytes to a compact binary-unit report.
def format_bytes(value):
	# Start from a floating-point byte count for repeated division.
	amount = float(value)
	# Walk conventional binary units from smallest to largest.
	for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
		# Return as soon as the value fits this unit.
		if amount < 1024 or unit == 'TiB': return f"{amount:.2f}{unit}"
		# Scale into the next unit.
		amount /= 1024

# Report DuckDB-managed memory and currently visible temporary storage after one phase.
def report_duckdb_resources(db, label):
	# Sum all buffer-manager categories exposed by DuckDB's documented metadata function.
	memory = db.execute("SELECT COALESCE(sum(memory_usage_bytes), 0), COALESCE(sum(temporary_storage_bytes), 0) FROM duckdb_memory()").fetchone()
	# Query DuckDB's own temporary-file registry in addition to polling the filesystem.
	temporary = db.execute("SELECT count(), COALESCE(sum(size), 0) FROM duckdb_temporary_files()").fetchone()
	# Print current managed state so retained oversized temp tables are attributable to a phase.
	print(f"resources_after={label} duckdb_memory={format_bytes(memory[0])} duckdb_temp_storage={format_bytes(memory[1])} temp_files={temporary[0]} temp_file_bytes={format_bytes(temporary[1])}", flush=True)

# Parse safe sample controls and optional full-result output.
def parse_args():
	# Describe the full scan explicitly so it is not triggered accidentally.
	parser = argparse.ArgumentParser(description='Benchmark disjoint BHL earliest-event reduction')
	# Keep one out of N page IDs before the large pagename join during exploratory runs.
	parser.add_argument('--sample-mod', type=int, default=1000, help='keep PageID hash partition 0 of N; default 1000')
	# Allow an intentional full-source run after the sampled query has succeeded.
	parser.add_argument('--full', action='store_true', help='process every BHL page/name candidate')
	# Cap DuckDB below the 128 GB production host so the benchmark cannot hide an oversized plan.
	parser.add_argument('--memory-limit', default='96GB', help='DuckDB memory_limit; default 96GB')
	# Match the production cruncher's 32-vCPU query parallelism by default.
	parser.add_argument('--threads', type=int, default=32, help='DuckDB worker threads; default 32')
	# Persist the proposed artifact only when the operator asks for a comparison file.
	parser.add_argument('--output', help='optional parquet path for the proposed final rows')
	# Return validated command-line values.
	args = parser.parse_args()
	# Reject invalid divisors before any large source member is scanned.
	if args.sample_mod < 1: parser.error('--sample-mod must be at least 1')
	# Reject unsafe or malformed memory settings before they enter SQL.
	if not re.fullmatch(r'[1-9][0-9]*(?:MB|GB)', args.memory_limit.upper()): parser.error('--memory-limit must look like 96GB or 4096MB')
	# Normalize the validated memory setting for DuckDB.
	args.memory_limit = args.memory_limit.upper()
	# Reject thread counts that cannot represent a real DuckDB worker pool.
	if args.threads < 1: parser.error('--threads must be at least 1')
	# Full mode disables the page partition filter.
	if args.full: args.sample_mod = 1
	# Hand the safe controls to the runner.
	return args

# Register only the five BHL source members needed by the production processor.
def register_sources(db, archive):
	# Read page identifiers, page types and page-level years from the 3.8 GB page export.
	page_tsv = db.read_csv(
		archive.open('Data/page.txt'),
		parallel=True,
		dtype={'PageID': 'UINTEGER', 'ItemID': 'UINTEGER', 'Year': 'VARCHAR', 'PageTypeName': 'VARCHAR'},
	)
	# Read name/page associations from the 9.3 GB pagename export.
	names_tsv = db.read_csv(
		archive.open('Data/pagename.txt'),
		parallel=True,
		dtype={'NameConfirmed': 'VARCHAR', 'PageID': 'UINTEGER'},
	)
	# Read item title keys and fallback publication years.
	item_tsv = db.read_csv(
		archive.open('Data/item.txt'),
		parallel=True,
		dtype={'ItemID': 'UINTEGER', 'TitleID': 'UINTEGER', 'Year': 'VARCHAR'},
	)
	# Read title language and display text used after event selection.
	title_tsv = db.read_csv(
		archive.open('Data/title.txt'),
		parallel=True,
		dtype={'TitleID': 'UINTEGER', 'LanguageCode': 'VARCHAR', 'StartYear': 'VARCHAR', 'EndYear': 'VARCHAR', 'ShortTitle': 'VARCHAR', 'FullTitle': 'VARCHAR'},
	)
	# Read title creators only after the heavy event reduction needs final enrichment.
	creator_tsv = db.read_csv(
		archive.open('Data/creator.txt'),
		parallel=True,
		dtype={'TitleID': 'UINTEGER', 'CreatorName': 'VARCHAR'},
	)
	# Register relations so later helper functions do not depend on Python frame replacement scans.
	db.register('page_tsv', page_tsv)
	# Register the largest association relation without copying it into a DuckDB table.
	db.register('names_tsv', names_tsv)
	# Register the compact item lookup source.
	db.register('item_tsv', item_tsv)
	# Register title metadata for language assignment and final labels.
	db.register('title_tsv', title_tsv)
	# Register creators for deterministic title-author aggregation.
	db.register('creator_tsv', creator_tsv)

# Build one-row-per-key item and title lookups before they touch the large name association scan.
def build_context_lookups(db):
	# Announce the small lookup phase separately from the heavy candidate aggregation.
	section('context lookups')
	# Start lookup timing.
	start = time.perf_counter()
	# Materialize distinct item payloads so duplicate source lines cannot multiply page candidates.
	db.execute("""
		CREATE TEMP TABLE item_rows AS
		SELECT DISTINCT
			CAST(ItemID AS UINTEGER) AS item_id,
			CAST(TitleID AS UINTEGER) AS title_id,
			TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{4}', 0), '') AS USMALLINT) AS item_year
		FROM item_tsv;
	""")
	# Materialize distinct language and publication-range context so title joins remain one-to-one.
	db.execute(f"""
		CREATE TEMP TABLE title_languages AS
		WITH parsed AS (
			SELECT DISTINCT
				CAST(TitleID AS UINTEGER) AS title_id,
				COALESCE(LanguageCode IN {EASTERN_LANGUAGES}, FALSE) AS eastern,
				TRY_CAST(NULLIF(REGEXP_EXTRACT(StartYear, '\\d{{4}}', 0), '') AS INTEGER) AS start_year,
				TRY_CAST(NULLIF(REGEXP_EXTRACT(EndYear, '\\d{{4}}', 0), '') AS INTEGER) AS end_year
			FROM title_tsv
		)
		SELECT
			title_id,
			eastern,
			CASE WHEN start_year IS NULL THEN end_year WHEN end_year IS NULL THEN start_year ELSE least(start_year, end_year) END AS range_start,
			CASE WHEN end_year IS NULL THEN start_year WHEN start_year IS NULL THEN end_year ELSE greatest(start_year, end_year) END AS range_end
		FROM parsed;
	""")
	# Count item IDs linked to multiple title records before reducing that known BHL shape.
	multi_title_items = db.execute("SELECT count() FROM (SELECT item_id FROM item_rows GROUP BY item_id HAVING count() > 1)").fetchone()[0]
	# Detect conflicting fallback years, including NULL versus populated values.
	year_conflicts = db.execute("SELECT count() FROM (SELECT item_id FROM item_rows GROUP BY item_id HAVING count(DISTINCT COALESCE(item_year, 0)) > 1)").fetchone()[0]
	# Count conflicting title-language rows before any page join can amplify them.
	title_conflicts = db.execute("SELECT count() FROM (SELECT title_id FROM title_languages GROUP BY title_id HAVING count() > 1)").fetchone()[0]
	# Measure the tiny set of multi-title items whose title records disagree on Eastern membership.
	eastern_conflicts = db.execute("""
		SELECT count() FROM (
			SELECT i.item_id
			FROM item_rows i
			LEFT JOIN title_languages t USING (title_id)
			GROUP BY i.item_id
			HAVING count(DISTINCT COALESCE(t.eastern, FALSE)) > 1
		)
	""").fetchone()[0]
	# Refuse year or title-key ambiguity because either could change chronology or multiply pages.
	if year_conflicts or title_conflicts:
		# Include exact conflict populations in the failure.
		raise RuntimeError(f'Ambiguous BHL lookups year_conflicts={year_conflicts} title_conflicts={title_conflicts}')
	# Select one complete item row, preferring populated metadata and then the stable lowest title ID.
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
	# Join the one-row-per-item winner to one-row-per-title language and date-range metadata.
	db.execute("""
		CREATE TEMP TABLE item_context AS
		SELECT i.item_id, i.item.title_id AS title_id, i.item.item_year AS item_year, COALESCE(t.eastern, FALSE) AS eastern, t.range_start, t.range_end
		FROM item_winners i
		LEFT JOIN title_languages t ON t.title_id = i.item.title_id;
	""")
	# Report lookup sizes and runtime for fanout auditing.
	counts = db.execute("SELECT (SELECT count() FROM item_context), (SELECT count() FROM title_languages)").fetchone()
	# Print the compact cardinalities and known multi-title ambiguity before the large join.
	print(f"item_context={counts[0]:,} title_languages={counts[1]:,} multi_title_items={multi_title_items:,} eastern_conflicts={eastern_conflicts:,} runtime={elapsed(start)}", flush=True)

# Reduce duplicate page classifications to one complete record before the large name join.
def build_page_records(db, sample_mod):
	# Announce the page-level reduction that prevents one page entering multiple event buckets.
	section('page classification reduction')
	# Keep the sample predicate on the 68-million-row page source before aggregation.
	sample_filter = '' if sample_mod == 1 else f'AND abs(hash(CAST(PageID AS UINTEGER))) % {sample_mod} = 0'
	# Start page reduction timing.
	start = time.perf_counter()
	# Select one complete page record, treating any Illustration classification as authoritative.
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
		WHERE PageID IS NOT NULL {sample_filter}
		GROUP BY PageID;
	""")
	# Report source classification rows versus unique pages for fanout visibility.
	metrics = db.execute("SELECT count(), count() FILTER (WHERE page.page_type = 'Illustration') FROM page_records").fetchone()
	# Print the bounded page relation that will become the small side of the name join.
	print(f"page_records={metrics[0]:,} illustration_pages={metrics[1]:,} runtime={elapsed(start)}", flush=True)

# Reduce all eligible page/name candidates directly to one complete event per raw name and bucket.
def build_raw_winners(db):
	# Announce the only operation that scans the 9.3 GB pagename member.
	section('early raw-name bucket reduction')
	# Start the heavy association scan timer.
	start = time.perf_counter()
	# Aggregate directly from the unique page relation so no 112-million-row intermediate is written.
	db.execute(f"""
		CREATE TEMP TABLE raw_winners AS
		WITH page_evidence AS (
			SELECT
				p.id_raw,
				p.page.item_id AS item_id,
				p.page.page_type AS page_type,
				p.page.page_year AS page_year,
				i.item_year,
				i.title_id,
				COALESCE(i.eastern, FALSE) AS eastern,
				i.range_start,
				i.range_end,
				CASE WHEN p.page.page_year IS NULL OR i.range_start IS NULL THEN NULL ELSE i.range_start + ((p.page.page_year % 100 - i.range_start % 100 + 100) % 100) END AS page_century,
				CASE WHEN i.item_year IS NULL OR i.range_start IS NULL THEN NULL ELSE i.range_start + ((i.item_year % 100 - i.range_start % 100 + 100) % 100) END AS item_century
			FROM page_records p
			LEFT JOIN item_context i ON i.item_id = p.page.item_id
		), page_candidates AS (
			SELECT
				id_raw,
				item_id,
				page_type,
				CAST(CASE
					WHEN page_year BETWEEN {YEAR_FLOOR} AND {CURRENT_YEAR} THEN page_year
					WHEN page_year <= {CURRENT_YEAR} AND page_year BETWEEN range_start AND range_end THEN page_year
					WHEN item_year <= {CURRENT_YEAR} AND item_year BETWEEN range_start AND range_end THEN item_year
					WHEN page_century <= {CURRENT_YEAR} AND page_century BETWEEN range_start AND range_end AND page_century + 100 > range_end THEN page_century
					WHEN item_century <= {CURRENT_YEAR} AND item_century BETWEEN range_start AND range_end AND item_century + 100 > range_end THEN item_century
					WHEN item_year BETWEEN {YEAR_FLOOR} AND {CURRENT_YEAR} THEN item_year
					ELSE NULL
				END AS USMALLINT) AS effective_year,
				title_id,
				eastern
			FROM page_evidence
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
			END AS events,
			count() AS candidate_count
		FROM categorized
		GROUP BY name_raw, mention_type;
	""")
	# Collect only bounded cardinality metrics after the reduction completes.
	metrics = db.execute("SELECT count(), count(DISTINCT name_raw), sum(candidate_count) FROM raw_winners").fetchone()
	# Show reduction ratio without materializing source rows in Python.
	print(f"raw_bucket_rows={metrics[0]:,} raw_names={metrics[1]:,} candidates={metrics[2]:,} runtime={elapsed(start)}", flush=True)

# Apply production name cleanup only after the source has collapsed to bounded rows per raw name.
def clean_and_reduce_names(db):
	# Announce the small second reduction after normalization.
	section('cleaned-name bucket reduction')
	# Start cleanup timing.
	start = time.perf_counter()
	# Unpack complete winners plus at most three popular candidates and apply production's initial name filter.
	db.execute("""
		CREATE TEMP TABLE bhl AS
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
	# Detect and normalize hybrid markers exactly as the production BHL processor does.
	find_hybrids(db, SOURCE_INFO)
	# Apply shared rank-fragment, punctuation and whitespace cleanup.
	name_cleanup(db, SOURCE_INFO)
	# Select the absolute lowest-ID illustration first so it owns any collision with chronology buckets.
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
	""")
	# Report final event and name counts before display metadata enrichment.
	metrics = db.execute("SELECT count(), count(DISTINCT name_clean) FROM cleaned_winners").fetchone()
	# Print cleanup cost separately from the large source scan.
	print(f"cleaned_bucket_rows={metrics[0]:,} cleaned_names={metrics[1]:,} runtime={elapsed(start)}", flush=True)

# Add title and author display fields only to the final reduced event set.
def enrich_winners(db):
	# Announce late enrichment to make the intended query order obvious.
	section('late title enrichment')
	# Start enrichment timing.
	start = time.perf_counter()
	# Reduce title text to one deterministic row per title ID before joining selected events.
	db.execute("""
		CREATE TEMP TABLE title_lookup AS
		SELECT
			CAST(TitleID AS UINTEGER) AS title_id,
			min(substring(COALESCE(ShortTitle, FullTitle), 1, 100)) AS title
		FROM title_tsv
		GROUP BY TitleID;
	""")
	# Aggregate all title creators in lexical order so identical inputs serialize identically.
	db.execute("""
		CREATE TEMP TABLE author_lookup AS
		SELECT
			CAST(TitleID AS UINTEGER) AS title_id,
			SUBSTRING(STRING_AGG(CreatorName, ', ' ORDER BY CreatorName), 1, 100) AS author_raw
		FROM creator_tsv
		WHERE CreatorName IS NOT NULL
		GROUP BY TitleID;
	""")
	# Materialize the proposed processed-contract rows with unchanged output column names.
	db.execute("""
		CREATE TEMP TABLE proposed AS
		SELECT
			event.id_raw AS id_raw,
			w.name_clean,
			event.item_id AS item_id,
			event.page_type AS type,
			event.year AS year,
			event.title_id AS title_id,
			event.eastern AS eastern,
			w.mention_type,
			event.hybrid AS hybrid,
			event.hybridpos AS hybridpos,
			t.title,
			a.author_raw
		FROM cleaned_winners w
		LEFT JOIN title_lookup t ON t.title_id = w.event.title_id
		LEFT JOIN author_lookup a ON a.title_id = w.event.title_id;
	""")
	# Report the late lookup cost and final row count.
	rows = db.execute("SELECT count() FROM proposed").fetchone()[0]
	# Keep output concise for full runs.
	print(f"proposed_rows={rows:,} runtime={elapsed(start)}", flush=True)

# Validate category membership, uniqueness and internally consistent output shape.
def validate_proposed(db):
	# Announce invariant checks separately from comparisons with legacy semantics.
	section('proposed invariants')
	# Count every structural violation in one bounded aggregate query.
	checks = db.execute("""
		SELECT
			count() - count(DISTINCT (name_clean, mention_type)) AS duplicate_buckets,
			count() - count(DISTINCT (name_clean, id_raw)) AS duplicate_pages,
			count() FILTER (
				WHERE mention_type = 'first_mention' AND (eastern OR type = 'Illustration')
				   OR mention_type = 'first_illustration' AND (eastern OR type != 'Illustration')
				   OR mention_type = 'first_eastern' AND (NOT eastern OR type = 'Illustration')
				   OR mention_type = 'first_eastern_illustration' AND (NOT eastern OR type != 'Illustration')
				   OR mention_type = 'popular_illustration' AND type != 'Illustration'
			) AS wrong_bucket,
			count() FILTER (WHERE year IS NULL OR id_raw IS NULL OR name_clean IS NULL) AS missing_required
		FROM proposed;
	""").fetchone()
	# Print named values so failures are obvious in logs.
	print(f"duplicate_buckets={checks[0]} duplicate_pages={checks[1]} wrong_bucket={checks[2]} missing_required={checks[3]}")
	# Fail immediately rather than allowing an invalid benchmark artifact to look successful.
	if any(checks): raise RuntimeError(f'Proposed BHL invariant failure {checks}')
	# Report per-bucket coverage for sample/full comparison.
	print(db.execute("SELECT mention_type, count() AS rows, count(DISTINCT name_clean) AS names FROM proposed GROUP BY mention_type ORDER BY mention_type").fetchdf().to_string(index=False))

# Compare changed event semantics with the existing processed artifact on full runs only.
def compare_baseline(db, full):
	# Skip misleading global counts when the proposed side intentionally contains one page partition.
	if not full: return
	# Announce the legacy comparison as descriptive rather than a correctness oracle.
	section('baseline comparison')
	# Register the current processed artifact without copying its rows.
	db.execute(f"CREATE VIEW baseline AS SELECT * FROM read_parquet('{BASELINE.replace(os.sep, '/')}')")
	# Reduce known legacy physical duplicates before any one-to-one bucket comparison.
	db.execute("""
		CREATE TEMP TABLE baseline_unique AS
		SELECT
			name_clean,
			mention_type,
			arg_min(
				struct_pack(id_raw := id_raw, year := year),
				struct_pack(missing_year := year IS NULL, year := COALESCE(year, 65535::USMALLINT), page := id_raw)
			) AS event
		FROM baseline
		GROUP BY name_clean, mention_type;
	""")
	# Report contract-level counts on raw baseline, deduped baseline and proposed output.
	counts = db.execute("""
		SELECT
			(SELECT count() FROM baseline) AS baseline_rows,
			(SELECT count() FROM baseline_unique) AS baseline_unique_rows,
			(SELECT count() FROM proposed) AS proposed_rows,
			(SELECT count(DISTINCT name_clean) FROM baseline) AS baseline_names,
			(SELECT count(DISTINCT name_clean) FROM proposed) AS proposed_names,
			(SELECT count() - count(DISTINCT (name_clean, id_raw)) FROM baseline) AS baseline_duplicate_pages,
			(SELECT count() - count(DISTINCT (name_clean, id_raw)) FROM proposed) AS proposed_duplicate_pages
	""").fetchone()
	# Print compact before/after shape without hiding baseline duplicate rows.
	print(f"baseline_rows={counts[0]:,} baseline_unique_rows={counts[1]:,} proposed_rows={counts[2]:,} baseline_names={counts[3]:,} proposed_names={counts[4]:,} baseline_duplicate_pages={counts[5]:,} proposed_duplicate_pages={counts[6]:,}")
	# Compare each unique compatibility bucket once on both sides.
	changes = db.execute("""
		WITH compared AS (
			SELECT
				COALESCE(b.name_clean, p.name_clean) AS name_clean,
				COALESCE(b.mention_type, p.mention_type) AS mention_type,
				b.event.id_raw AS baseline_page,
				b.event.year AS baseline_year,
				p.id_raw AS proposed_page,
				p.year AS proposed_year
			FROM baseline_unique b
			FULL OUTER JOIN proposed p USING (name_clean, mention_type)
		)
		SELECT
			count() FILTER (WHERE baseline_page IS NOT NULL AND proposed_page IS NOT NULL) AS common_buckets,
			count() FILTER (WHERE baseline_page IS NULL) AS added_buckets,
			count() FILTER (WHERE proposed_page IS NULL) AS removed_buckets,
			count() FILTER (WHERE baseline_page IS NOT NULL AND proposed_page IS NOT NULL AND baseline_page != proposed_page) AS changed_pages,
			count() FILTER (WHERE baseline_page IS NOT NULL AND proposed_page IS NOT NULL AND baseline_year IS DISTINCT FROM proposed_year) AS changed_years
		FROM compared;
	""").fetchone()
	# Print category-selection changes without claiming old overlapping semantics are equivalent.
	print(f"common_buckets={changes[0]:,} added_buckets={changes[1]:,} removed_buckets={changes[2]:,} changed_pages={changes[3]:,} changed_years={changes[4]:,}")
	# Show category coverage side-by-side for the disjoint-bucket impact.
	print(db.execute("""
		WITH counts AS (
			SELECT 'baseline' AS side, mention_type, count() AS rows FROM baseline_unique GROUP BY mention_type
			UNION ALL
			SELECT 'proposed' AS side, mention_type, count() AS rows FROM proposed GROUP BY mention_type
		)
		SELECT * FROM counts ORDER BY mention_type, side;
	""").fetchdf().to_string(index=False))

# Print sentinel rows in chronological order for manual source review.
def show_sentinels(db):
	# Announce bounded human-readable samples.
	section('sentinels')
	# Build a safely quoted literal list from fixed source-code sentinels.
	names = ', '.join("'" + name.replace("'", "''") + "'" for name in SENTINELS)
	# Keep all fields needed to inspect category assignment and chronology.
	rows = db.execute(f"""
		SELECT name_clean, year, id_raw, mention_type, type, eastern, title, author_raw
		FROM proposed
		WHERE name_clean IN ({names})
		ORDER BY name_clean, year, id_raw;
	""").fetchdf()
	# Print without an index to keep copied evidence clean.
	print(rows.to_string(index=False))

# Persist the proposed rows only when requested by the operator.
def write_output(db, output):
	# Leave normal benchmark runs free of large persistent artifacts.
	if not output: return
	# Resolve an absolute destination and ensure its parent exists.
	path = os.path.abspath(output)
	# Create the destination directory when needed.
	os.makedirs(os.path.dirname(path), exist_ok=True)
	# Normalize path separators before embedding the trusted local path in SQL.
	sql_path = path.replace('\\', '/').replace("'", "''")
	# Write the final reduced relation for follow-up ad-hoc checks.
	db.execute(f"COPY proposed TO '{sql_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
	# Confirm the exact artifact path.
	print(f"Wrote proposed artifact to {path}", flush=True)

# Execute the sampled or full benchmark end to end.
def main():
	# Parse safe runtime controls before opening the multi-gigabyte source.
	args = parse_args()
	# Create the spill directory explicitly because the in-memory database cannot infer one safely.
	os.makedirs(TMP_DIR, exist_ok=True)
	# Refuse to mix a new benchmark with unexplained residue from an earlier process.
	initial_spill_files = [os.path.join(root, name) for root, _, files in os.walk(TMP_DIR) for name in files]
	# Preserve evidence rather than deleting unexpected files automatically.
	if initial_spill_files: raise RuntimeError(f'Spill directory is not empty before run: {initial_spill_files[:10]}')
	# Print immutable inputs and mode for reproducibility.
	print(f"source={SOURCE}")
	# Print the baseline path used only by full comparisons.
	print(f"baseline={BASELINE}")
	# Make the sampled fraction explicit in logs.
	print(f"page_partition=all" if args.full else f"page_partition=hash(PageID)%{args.sample_mod}=0")
	# Print resource limits before any query starts.
	print(f"memory_limit={args.memory_limit} threads={args.threads} spill_dir={TMP_DIR}")
	# Start high-water monitoring before DuckDB can create transient spill files.
	monitor = ResourceMonitor(TMP_DIR)
	# Begin background memory and spill sampling.
	monitor.start()
	# Ensure monitoring always stops even when a query raises.
	try:
		# Open the source archive once so every relation streams from the same local artifact.
		with zipfile.ZipFile(SOURCE, 'r') as archive, duckdb.connect(':memory:') as db:
			# Route larger-than-memory hash aggregation spill to the dedicated ignored directory.
			db.execute(f"SET temp_directory = '{TMP_DIR.replace(os.sep, '/')}'")
			# Cap DuckDB below production RAM so an inefficient plan spills or fails visibly here.
			db.execute(f"SET memory_limit = '{args.memory_limit}'")
			# Match production parallelism instead of exploiting extra workstation cores.
			db.execute(f"SET threads = {args.threads}")
			# Confirm DuckDB accepted the intended limits and location.
			configured = db.execute("SELECT current_setting('memory_limit'), current_setting('threads'), current_setting('temp_directory')").fetchone()
			# Print effective settings rather than trusting requested values.
			print(f"duckdb_memory_limit={configured[0]} duckdb_threads={configured[1]} duckdb_temp_directory={configured[2]}")
			# Match production page-type storage so enum comparisons are exercised by the AB test.
			db.execute("""
				CREATE TYPE page_type_enum AS ENUM (
					'Text', 'Illustration', 'Blank', 'Cover', 'Chart', 'Title Page', 'Index', 'Table of Contents', 'Foldout', 'Appendix',
					'Map', 'Issue Start', 'List of Illustrations', 'Article Start', 'Article End', 'Errata', 'Specimen'
				);
				CREATE TYPE mention_type_enum AS ENUM ('first_mention', 'first_illustration', 'first_eastern', 'first_eastern_illustration', 'popular_illustration');
			""")
			# Register source members lazily without extracting 13+ GB of TSV data.
			register_sources(db, archive)
			# Build and verify one-to-one small-side joins first.
			build_context_lookups(db)
			# Record retained memory after compact lookup construction.
			report_duckdb_resources(db, 'context_lookups')
			# Resolve multi-type page classifications before they can create duplicate event pages.
			build_page_records(db, args.sample_mod)
			# Attribute page-table memory before the much larger name association scan.
			report_duckdb_resources(db, 'page_records')
			# Collapse the large page/name stream immediately to raw-name bucket winners.
			build_raw_winners(db)
			# Attribute raw-winner memory and any spill created by the large group-by.
			report_duckdb_resources(db, 'raw_winners')
			# Normalize names only after the high-cardinality source has been reduced.
			clean_and_reduce_names(db)
			# Attribute cleanup and second-reduction retained memory.
			report_duckdb_resources(db, 'cleaned_winners')
			# Attach title and author fields to final winners only.
			enrich_winners(db)
			# Attribute final artifact memory before validation reads it.
			report_duckdb_resources(db, 'proposed')
			# Enforce the new mutually exclusive output contract.
			validate_proposed(db)
			# Compare against current output only when both sides cover the complete source.
			compare_baseline(db, args.full)
			# Show fixed sentinels for manual chronology review.
			show_sentinels(db)
			# Optionally retain the proposed relation for deeper analysis.
			write_output(db, args.output)
	# Stop monitoring and report cleanup even when a DuckDB query raises.
	finally:
		# Stop monitoring after DuckDB closes and removes its temporary database state.
		monitor.stop()
		# Inspect the configured directory after connection teardown as requested.
		remaining = monitor.remaining_files()
		# Report both transient high-water use and cleanup outcome.
		print(f"peak_rss={format_bytes(monitor.peak_rss)} peak_spill={format_bytes(monitor.peak_spill)} peak_spill_files={monitor.peak_spill_files} remaining_spill_files={len(remaining)}")
		# List residue without deleting it so failed cleanup remains inspectable.
		if remaining: print(f"remaining_spill={remaining[:20]}")

# Run only when invoked as an AB test script.
if __name__ == '__main__':
	# Execute the benchmark and let failures return non-zero.
	main()

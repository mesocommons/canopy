# Evaluate BHL page/item/title year reconciliation before changing production semantics.
import glob
# Build stable local paths for source and spill data.
import os
# Use the wall-clock UTC year as the requested hard upper bound.
from datetime import datetime, timezone
# Stream source members without extracting the multi-gigabyte archive.
import zipfile

# Keep all transformations in DuckDB and report only bounded aggregates.
import duckdb

# Resolve the canopy package and latest hydrated BHL source.
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Use the canonical source artifact already hydrated from S3.
SOURCE = sorted(glob.glob(os.path.join(BASE_DIR, 'data', 'source', 'bhl.*.zip')))[-1]
# Route any larger-than-memory state away from the repository root.
TMP_DIR = os.path.join(BASE_DIR, 'data', 'temp', 'bhl-year-abtest')
# Match the lower bound requested for uncorroborated publication years.
YEAR_FLOOR = 1600
# Resolve the hard upper bound dynamically for this run.
CURRENT_YEAR = datetime.now(timezone.utc).year
# Keep production-like memory and thread ceilings explicit.
MEMORY_LIMIT = '96GB'
# Match the 32-vCPU cloud cruncher.
THREADS = 32
# Pages already investigated manually and expected resolutions.
SENTINELS = [42979101, 42979272, 42979390, 45387526, 45387736, 45696292, 65015638]

# Print one compact report section.
def section(title):
	# Flush before long scans so logs show the active phase.
	print(f"\n############### {title} ###############", flush=True)

# Register only source columns needed by the page-level year probe.
def register_sources(db, archive):
	# Read page identifiers, item links, page types and page years.
	page_tsv = db.read_csv(
		archive.open('Data/page.txt'),
		parallel=True,
		dtype={'PageID': 'UINTEGER', 'ItemID': 'UINTEGER', 'PageTypeName': 'VARCHAR', 'Year': 'VARCHAR'},
	)
	# Read item title links and fallback years.
	item_tsv = db.read_csv(
		archive.open('Data/item.txt'),
		parallel=True,
		dtype={'ItemID': 'UINTEGER', 'TitleID': 'UINTEGER', 'Year': 'VARCHAR'},
	)
	# Read title ranges and display fields for diagnostics.
	title_tsv = db.read_csv(
		archive.open('Data/title.txt'),
		parallel=True,
		dtype={'TitleID': 'UINTEGER', 'StartYear': 'VARCHAR', 'EndYear': 'VARCHAR', 'ShortTitle': 'VARCHAR', 'FullTitle': 'VARCHAR'},
	)
	# Register relations so SQL does not depend on Python replacement-scan frames.
	db.register('page_tsv', page_tsv)
	# Register the compact item relation.
	db.register('item_tsv', item_tsv)
	# Register title ranges and labels.
	db.register('title_tsv', title_tsv)

# Build one deterministic item/title context per ItemID without join fanout.
def build_context(db):
	# Materialize distinct item payloads before resolving multi-title items.
	db.execute("""
		CREATE TEMP TABLE item_rows AS
		SELECT DISTINCT
			CAST(ItemID AS UINTEGER) AS item_id,
			CAST(TitleID AS UINTEGER) AS title_id,
			TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{4}', 0), '') AS INTEGER) AS item_year
		FROM item_tsv;
	""")
	# Parse title bounds once on the small 192k-row title relation.
	db.execute("""
		CREATE TEMP TABLE title_rows AS
		WITH parsed AS (
			SELECT DISTINCT
				CAST(TitleID AS UINTEGER) AS title_id,
				TRY_CAST(NULLIF(REGEXP_EXTRACT(StartYear, '\\d{4}', 0), '') AS INTEGER) AS start_year,
				TRY_CAST(NULLIF(REGEXP_EXTRACT(EndYear, '\\d{4}', 0), '') AS INTEGER) AS end_year,
				substring(COALESCE(ShortTitle, FullTitle), 1, 100) AS title
			FROM title_tsv
		)
		SELECT
			title_id,
			CASE WHEN start_year IS NULL THEN end_year WHEN end_year IS NULL THEN start_year ELSE least(start_year, end_year) END AS range_start,
			CASE WHEN end_year IS NULL THEN start_year WHEN start_year IS NULL THEN end_year ELSE greatest(start_year, end_year) END AS range_end,
			title
		FROM parsed;
	""")
	# Fail if title parsing would multiply one selected title ID.
	title_conflicts = db.execute("SELECT count() FROM (SELECT title_id FROM title_rows GROUP BY title_id HAVING count() > 1)").fetchone()[0]
	# Fail if duplicate item payloads disagree on fallback year.
	year_conflicts = db.execute("SELECT count() FROM (SELECT item_id FROM item_rows GROUP BY item_id HAVING count(DISTINCT struct_pack(present := item_year IS NOT NULL, year := item_year)) > 1)").fetchone()[0]
	# Keep ambiguity visible rather than silently selecting from it.
	if title_conflicts or year_conflicts: raise RuntimeError(f'lookup conflicts title={title_conflicts} year={year_conflicts}')
	# Preserve production's deterministic complete-row selection for multi-title items.
	db.execute("""
		CREATE TEMP TABLE item_winners AS
		SELECT
			item_id,
			arg_min(
				struct_pack(title_id := title_id, item_year := item_year),
				struct_pack(missing_title := title_id IS NULL, missing_year := item_year IS NULL, title_id := COALESCE(title_id, 4294967295::UINTEGER), year := COALESCE(item_year, 2147483647))
			) AS item
		FROM item_rows
		GROUP BY item_id;
	""")
	# Attach the selected title's date range and label.
	db.execute("""
		CREATE TEMP TABLE item_context AS
		SELECT i.item_id, i.item.title_id AS title_id, i.item.item_year AS item_year, t.range_start, t.range_end, t.title
		FROM item_winners i
		LEFT JOIN title_rows t ON t.title_id = i.item.title_id;
	""")

# Reduce multi-type page rows exactly once before evaluating year candidates.
def build_pages(db):
	# Keep one complete page row with production's Illustration-first type ordering.
	db.execute("""
		CREATE TEMP TABLE page_records AS
		SELECT
			CAST(PageID AS UINTEGER) AS page_id,
			arg_min(
				struct_pack(
					item_id := CAST(ItemID AS UINTEGER),
					page_type := CAST(PageTypeName AS VARCHAR),
					page_year := TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{4}', 0), '') AS INTEGER)
				),
				struct_pack(
					not_illustration := PageTypeName IS DISTINCT FROM 'Illustration',
					page_type := COALESCE(PageTypeName, ''),
					missing_year := TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{4}', 0), '') AS INTEGER) IS NULL,
					page_year := COALESCE(TRY_CAST(NULLIF(REGEXP_EXTRACT(Year, '\\d{4}', 0), '') AS INTEGER), 2147483647),
					item_id := CAST(ItemID AS UINTEGER)
				)
			) AS page
		FROM page_tsv
		WHERE PageID IS NOT NULL
		GROUP BY PageID;
	""")

# Resolve years from ordinary bounds, corroborated ranges, and unique century correction.
def reconcile_years(db):
	# Build scalar century candidates without expanding rows or generating ranges.
	db.execute(f"""
		CREATE TEMP TABLE reconciled AS
		WITH base AS (
			SELECT
				p.page_id,
				p.page.page_type AS page_type,
				p.page.page_year AS page_year,
				i.item_year,
				i.range_start,
				i.range_end,
				i.title,
				CASE WHEN p.page.page_year IS NULL OR i.range_start IS NULL THEN NULL ELSE i.range_start + ((p.page.page_year % 100 - i.range_start % 100 + 100) % 100) END AS page_century,
				CASE WHEN i.item_year IS NULL OR i.range_start IS NULL THEN NULL ELSE i.range_start + ((i.item_year % 100 - i.range_start % 100 + 100) % 100) END AS item_century
			FROM page_records p
			LEFT JOIN item_context i ON i.item_id = p.page.item_id
		), resolved AS (
			SELECT
				*,
				CASE
					WHEN page_year BETWEEN {YEAR_FLOOR} AND {CURRENT_YEAR} THEN page_year
					WHEN page_year <= {CURRENT_YEAR} AND page_year BETWEEN range_start AND range_end THEN page_year
					WHEN item_year <= {CURRENT_YEAR} AND item_year BETWEEN range_start AND range_end THEN item_year
					WHEN page_century <= {CURRENT_YEAR} AND page_century BETWEEN range_start AND range_end AND page_century + 100 > range_end THEN page_century
					WHEN item_century <= {CURRENT_YEAR} AND item_century BETWEEN range_start AND range_end AND item_century + 100 > range_end THEN item_century
					WHEN item_year BETWEEN {YEAR_FLOOR} AND {CURRENT_YEAR} THEN item_year
					ELSE NULL
				END AS resolved_year,
				CASE
					WHEN page_year BETWEEN {YEAR_FLOOR} AND {CURRENT_YEAR} THEN 'page_in_bounds'
					WHEN page_year <= {CURRENT_YEAR} AND page_year BETWEEN range_start AND range_end THEN 'page_in_title_range'
					WHEN item_year <= {CURRENT_YEAR} AND item_year BETWEEN range_start AND range_end THEN 'item_in_title_range'
					WHEN page_century <= {CURRENT_YEAR} AND page_century BETWEEN range_start AND range_end AND page_century + 100 > range_end THEN 'page_century_corrected'
					WHEN item_century <= {CURRENT_YEAR} AND item_century BETWEEN range_start AND range_end AND item_century + 100 > range_end THEN 'item_century_corrected'
					WHEN item_year BETWEEN {YEAR_FLOOR} AND {CURRENT_YEAR} THEN 'item_in_bounds'
					ELSE 'rejected'
				END AS resolution
			FROM base
		)
		SELECT *, COALESCE(page_year, item_year) AS previous_year
		FROM resolved;
	""")

# Report bounded full-source diagnostics and known sentinel resolutions.
def report(db):
	# Summarize each resolution path without returning page-level data.
	section('resolution paths')
	# Print counts by selected rule.
	print(db.execute("SELECT resolution, count() AS pages FROM reconciled GROUP BY resolution ORDER BY pages DESC").fetchdf().to_string(index=False))
	# Summarize changes around the requested bounds.
	section('bounds and changes')
	# Report previous outliers, retained medieval pages, corrections and rejections.
	print(db.execute(f"""
		SELECT
			count() AS pages,
			count() FILTER (WHERE previous_year < {YEAR_FLOOR}) AS previous_below_floor,
			count() FILTER (WHERE previous_year > {CURRENT_YEAR}) AS previous_above_current,
			count() FILTER (WHERE resolved_year < {YEAR_FLOOR}) AS corroborated_below_floor,
			count() FILTER (WHERE resolved_year > {CURRENT_YEAR}) AS resolved_above_current,
			count() FILTER (WHERE resolved_year IS DISTINCT FROM previous_year) AS changed_or_rejected,
			count() FILTER (WHERE resolved_year IS NULL AND previous_year IS NOT NULL) AS rejected_dated_pages
		FROM reconciled;
	""").fetchdf().to_string(index=False))
	# Show the exact manually investigated records.
	section('sentinels')
	# Build a fixed literal list from source-code IDs.
	ids = ','.join(str(value) for value in SENTINELS)
	# Print all date evidence and the selected outcome.
	print(db.execute(f"""
		SELECT page_id, page_year, item_year, range_start, range_end, page_century, item_century, previous_year, resolved_year, resolution, title
		FROM reconciled
		WHERE page_id IN ({ids})
		ORDER BY page_id;
	""").fetchdf().to_string(index=False))

# Run the focused page-level probe without touching the pagename stream.
def main():
	# Create the dedicated spill location and refuse unexplained residue.
	os.makedirs(TMP_DIR, exist_ok=True)
	# Keep prior spill evidence rather than deleting it automatically.
	residue = [os.path.join(root, name) for root, _, files in os.walk(TMP_DIR) for name in files]
	# Abort if another run left files behind.
	if residue: raise RuntimeError(f'spill directory is not empty: {residue[:10]}')
	# Print reproducible runtime controls.
	print(f"source={SOURCE}\nyear_floor={YEAR_FLOOR}\ncurrent_year={CURRENT_YEAR}\nmemory_limit={MEMORY_LIMIT}\nthreads={THREADS}\nspill_dir={TMP_DIR}")
	# Open the source once and stream its members through one DuckDB connection.
	with zipfile.ZipFile(SOURCE, 'r') as archive, duckdb.connect(':memory:') as db:
		# Route all DuckDB spill files explicitly.
		db.execute(f"SET temp_directory = '{TMP_DIR.replace(os.sep, '/')}'")
		# Cap the page-level probe below production memory.
		db.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
		# Match production parallelism.
		db.execute(f"SET threads = {THREADS}")
		# Register only page/item/title streams.
		register_sources(db, archive)
		# Resolve compact item/title context first.
		build_context(db)
		# Reduce duplicate page metadata before date logic.
		build_pages(db)
		# Apply the candidate reconciliation policy.
		reconcile_years(db)
		# Print bounded evidence.
		report(db)
	# Verify DuckDB removed every transient spill file after connection close.
	remaining = [os.path.join(root, name) for root, _, files in os.walk(TMP_DIR) for name in files]
	# Fail if cleanup was incomplete.
	if remaining: raise RuntimeError(f'spill residue after run: {remaining[:10]}')
	# Confirm clean teardown.
	print('remaining_spill_files=0')

# Execute only when invoked directly as an AB test.
if __name__ == '__main__':
	# Run the full page-level reconciliation probe.
	main()

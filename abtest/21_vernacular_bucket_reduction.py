# Compare vernacular bucket-reduction variants for correctness determinism and runtime.
#
# Background:
#	Production reduce_vernacular() collapses spelling variants with a greedy Levenshtein loop.
#	The bucket is built with list_distinct(), which flattens every occurrence count to 1, so the
#	subsequent list_mode(bucket) cannot rank by frequency and simply returns the first element of
#	DuckDB's hash-ordered set. A name seen once can therefore absorb a name seen nine times.
#
# Variants under test:
#	current   production code as-is
#	proposed  bucket keeps occurrences, mode over sorted multiset picks the winner
#	sorted    candidate list pre-sorted by occurrence, seed is by construction the winner
#
# Scope:
#	This probe isolates candidate-arrival effects inside the greedy bucket loop.
#	It rebuilds published rankings in DuckDB and intentionally does not run vernacular_udf.
#	It therefore does not measure Polars value_counts tie ordering in final production output.
#
from types import SimpleNamespace
# Load timing helpers for variant runtime comparisons.
import time
# Load DuckDB for the set-based A/B probes.
import duckdb
# Load canopy settings proxy and builder.
from importer.canopy import settings, build_settings
# Load latest processed-file discovery.
from importer.canopy.utils.filehandlers import get_latest_processed
# Load production fuse helpers so the base state matches production exactly.
from importer.canopy.pipeline import fuse as fuse_pipeline

# Names we already know flip between releases, used as sentinels in the report.
SENTINELS = ['vaccinium myrtillus', 'vaccinium vitisidaea', 'sambucus nigra']
# How many rich-vernacular taxa to print in the before/after sample.
SAMPLE_SIZE = 20
# Minimum English name count for a taxon to qualify for the sample section.
SAMPLE_MIN_NAMES = 8

# Print a compact section header.
def section(title):
	# Separate output sections in the console.
	print(f"\n############### {title} ###############", flush=True)

# Format elapsed runtime for one flow.
def elapsed(start):
	# Return seconds with one decimal place.
	return f"{time.time() - start:.1f}s"

# Initialize canopy settings for local hydrated files.
def init_runtime():
	# Build minimal CLI args for local, non-S3 analysis.
	args = SimpleNamespace(debug=False, verbose=False, force=False, csv=False, s3=False)
	# Install runtime settings into the canopy proxy.
	settings.set_config(build_settings(args))

# Build the same meso state production has immediately before reduce_vernacular().
def build_base_state(db):
	# Resolve latest hydrated processed inputs.
	results = get_latest_processed()
	# Load source parquet tables into DuckDB using production code.
	fuse_pipeline.load_map_sources(results, db)
	# Build the initial backbone rows using production code.
	fuse_pipeline.initial_backbone(results, db)
	# Add cross-source IDs because vernacular joins on every source ID column.
	fuse_pipeline.add_ids(results, db)
	# Build name consensus because vernacular filters out name_consensus matches.
	fuse_pipeline.basic_consensus(results, db)
	# Create Meso UUIDs because merged_vernacular groups by id_meso.
	fuse_pipeline.create_hashes(results, db)
	# Load IUCN and NCBI which both contribute vernacular names.
	fuse_pipeline.load_enrich_sources(results, db)
	# Apply enrichment exactly as production does before the vernacular stage.
	fuse_pipeline.enrich(results, db)
	# Hand back the resolved inputs for reuse.
	return results

# Reproduce the production merge that feeds the Levenshtein loop, stopping before any bucketing.
# This is the shared input for every variant, so differences can only come from the loop itself.
def build_merged_vernacular(db):
	# Unnest the Wikidata vernacular map into one row per item and language.
	db.execute("""
		CREATE TEMP TABLE wikidata_vernacular AS
		SELECT m.id_meso, m.name_consensus, kv.key AS lang, kv.value AS names FROM meso m
		JOIN wikidata w ON m.wikidata_id = w.id_raw AND cardinality(w.vernacular) > 0
		CROSS JOIN LATERAL UNNEST(map_entries(w.vernacular)) AS t(kv);
	""")
	# Collect the higher quality sources into one table using the lang:name encoding.
	db.execute("""CREATE TEMP TABLE quality_vernacular (id_meso UUID, name_consensus VARCHAR, lang VARCHAR, names VARCHAR[]);""")
	# Fold each remaining source in with the same prefilter production uses.
	for source in ['inaturalist', 'iucn', 'gbif', 'ncbi', 'col']:
		# Split the lang:name payload and group the names per item and language.
		db.execute(f"""
			INSERT INTO quality_vernacular BY NAME
			SELECT
				m.id_meso,
				m.name_consensus,
				string_split(u.unnested, ':')[1] AS lang,
				array_agg(trim(string_split(u.unnested, ':')[2])) AS names
			FROM meso m
			JOIN {source} s ON m.{source}_id = s.id_raw AND len(s.vernacular) > 0
			CROSS JOIN LATERAL (SELECT UNNEST(s.vernacular) AS unnested) AS u
			GROUP BY m.id_meso, m.name_consensus, lang
		""")
	# Merge both pools, lowercase everything and drop names identical to the scientific name.
	db.execute("""
		CREATE TEMP TABLE base_vernacular AS
		SELECT id_meso, name_consensus, lang, list_filter(list_transform(flatten(array_agg(names)), x -> lower(x)), x -> x != name_consensus) AS names
		FROM (
			SELECT id_meso, name_consensus, lang, names FROM quality_vernacular
			UNION ALL
			SELECT id_meso, name_consensus, lang, names FROM wikidata_vernacular
		) combined
		GROUP BY id_meso, name_consensus, lang;
		DROP TABLE quality_vernacular;
		DROP TABLE wikidata_vernacular;
	""")
	# Drop rows whose only name was the scientific name itself.
	db.execute("DELETE FROM base_vernacular WHERE len(names) = 0;")
	# Report the shared input size once.
	rows = db.execute("SELECT COUNT(*) FROM base_vernacular").fetchone()[0]
	# Show how many item/language pairs every variant will process.
	print(f"Shared input: {rows:,} id/language pairs", flush=True)

# Materialize one working copy of the shared input under a chosen candidate-list ordering.
# The ordering emulates the arrival order that array_agg() produces under different thread counts.
def make_working_copy(db, table, ordering):
	# Start from the untouched merged names.
	db.execute(f"DROP TABLE IF EXISTS {table}")
	# Original keeps the list exactly as the merge produced it.
	if ordering == 'original': expr = "names"
	# Reversed emulates a different morsel arrival order at the aggregate.
	elif ordering == 'reversed': expr = "list_reverse(names)"
	# Rotated emulates yet another arrival order without changing the multiset.
	# Integer division is mandatory here because DuckDB's / is float division and a fractional
	# slice bound silently drops an element, which would change the multiset instead of reordering it.
	else: expr = "list_concat(names[len(names)//2 + 1:], names[1:len(names)//2])"
	# Build the working table with the original multiset preserved for correctness scoring.
	db.execute(f"""
		CREATE TEMP TABLE {table} AS
		SELECT
			id_meso,
			name_consensus,
			lang,
			-- Candidate list the loop consumes, in the ordering under test
			{expr} AS names,
			-- Untouched copy of the raw multiset used to score correctness afterwards
			names AS orig,
			-- Target list the loop rewrites in place
			{expr} AS target,
			-- Bucket of the current seed and its near neighbours
			CAST(NULL AS VARCHAR[]) AS bucket,
			-- Winner of the current bucket
			CAST(NULL AS VARCHAR) AS most_common,
			-- Rows still needing work
			len(names) > 1 AS process
		FROM base_vernacular;
	""")

# Run the greedy Levenshtein loop for one variant and return its wall-clock runtime.
def run_variant(db, table, variant):
	# Collapse lists that only ever held one distinct value, exactly as production does first.
	db.execute(f"UPDATE {table} SET names = list_value(list_any_value(names)), target = list_value(list_any_value(names)), process = FALSE WHERE len(names) > 1 AND list_unique(names) = 1")
	# Start the clock before the pre-sort so the sorted variant is charged for its own setup cost.
	start = time.time()
	# The sorted variant pays a one-time cost to order candidates by descending occurrence.
	if variant == 'sorted':
		# Sort distinct names by occurrence descending then alphabetically, and re-expand to a multiset.
		db.execute(f"""
			UPDATE {table} SET names = flatten(
				list_transform(
					-- Order distinct names by negative count so the most frequent leads, alphabetical on ties
					list_sort(list_transform(list_distinct(names), x -> {{'c': -len(list_filter(names, y -> y = x)), 'n': x}})),
					-- Re-expand each distinct name back to its original number of occurrences
					s -> list_transform(range(len(list_filter(names, y -> y = s.n))), i -> s.n)
				)
			) WHERE process
		""")
	# Track how many rows still need processing.
	remaining = db.execute(f"SELECT COUNT(*) FROM {table} WHERE process").fetchone()[0]
	# Loop until the tail is short enough, mirroring the production stop condition.
	while True:
		# Current production builds the bucket with list_distinct, which destroys occurrence counts.
		if variant == 'current':
			# Seed the bucket and dedupe it, losing every count in the process.
			db.execute(f"UPDATE {table} SET bucket = list_distinct([names[1]] || list_filter(names, x -> levenshtein(names[1],x) < 3)) WHERE process")
			# Pick the winner from a list where every candidate now has count one.
			db.execute(f"UPDATE {table} SET most_common = list_mode(bucket) WHERE process")
			# Guard the rewrite on raw bucket length as production does.
			guard = "len(bucket) > 1"
		# Proposed keeps occurrences in the bucket so mode can actually rank by frequency.
		else:
			# Keep every occurrence, and rely on levenshtein(seed,seed)=0 to include the seed itself.
			db.execute(f"UPDATE {table} SET bucket = list_filter(names, x -> levenshtein(names[1],x) < 3) WHERE process")
			# The sorted variant's seed is already the bucket's most frequent member, so no mode is needed.
			if variant == 'sorted': db.execute(f"UPDATE {table} SET most_common = names[1] WHERE process")
			# Otherwise take the mode of the sorted multiset, which breaks frequency ties alphabetically.
			else: db.execute(f"UPDATE {table} SET most_common = list_mode(list_sort(bucket)) WHERE process")
			# Guard on distinct names because the bucket now carries duplicates.
			guard = "len(list_distinct(bucket)) > 1"
		# Consume the bucket and rewrite its members in the target list to the winner.
		db.execute(f"""UPDATE {table} SET
			names = list_filter(names, x -> NOT list_contains(bucket, x)),
			target = CASE WHEN {guard} THEN list_transform(target, x -> CASE WHEN list_contains(bucket, x) THEN most_common ELSE x END)
					 ELSE target
					 END
			WHERE process
		""")
		# Retire rows whose candidate list is exhausted.
		db.execute(f"UPDATE {table} SET process = FALSE WHERE process AND len(names) = 0")
		# Recount the rows still in flight.
		remaining = db.execute(f"SELECT COUNT(*) FROM {table} WHERE process").fetchone()[0]
		# Show progress on one line to keep the log readable.
		print(f"({variant}) {remaining:,} rows left to process          ", end='\r', flush=True)
		# Stop on the same short tail production stops on.
		if remaining <= 10: break
	# Fold the rewritten target list back into names, as production does after the loop.
	db.execute(f"UPDATE {table} SET names = target WHERE len(target) > 0")
	# Return the loop runtime for the timing table.
	return time.time() - start

# Score one finished variant for correctness against the untouched raw multiset.
def score_variant(db, table):
	# Compute per-row raw and final winners plus tie fragility in one pass.
	return db.execute(f"""
		WITH scored AS (
			SELECT
				id_meso,
				lang,
				-- Most frequent raw name, alphabetical on ties, because list_mode returns the first max in list order
				list_mode(list_sort(orig)) AS raw_top,
				-- Most frequent name after reduction, scored the same way
				list_mode(list_sort(names)) AS final_top,
				-- Names still present after reduction
				names AS final_names,
				-- Raw multiset for occurrence lookups
				orig
			FROM {table}
			WHERE len(orig) > 1
		), counted AS (
			SELECT
				*,
				-- Occurrences of the winning name in the final list
				len(list_filter(final_names, y -> y = final_top)) AS top_count,
				-- Did the most frequent raw name survive the rewrite at all
				list_contains(final_names, raw_top) AS raw_top_survives
			FROM scored
		)
		SELECT
			COUNT(*) AS rows_scored,
			-- The reported bug: the dominant raw name was rewritten away into a rarer variant
			COUNT(*) FILTER (WHERE NOT raw_top_survives AND final_top != raw_top) AS raw_top_lost,
			-- Softer failure: the dominant raw name survived but no longer ranks first
			COUNT(*) FILTER (WHERE raw_top_survives AND final_top != raw_top) AS raw_top_demoted,
			-- Fragility: rank one is a dead heat, so a single extra occurrence upstream flips it
			COUNT(*) FILTER (WHERE len(list_filter(list_distinct(final_names), x -> len(list_filter(final_names, y -> y = x)) = top_count)) > 1) AS top_is_tied
		FROM counted;
	""").fetchone()

# Compare two finished runs of the same variant to measure order-dependence.
def score_stability(db, left, right):
	# Count rows whose top-ten list or rank-one name differ between the two orderings.
	return db.execute(f"""
		SELECT
			COUNT(*) AS rows_compared,
			-- Rank one changed purely because the candidate list arrived in a different order
			COUNT(*) FILTER (WHERE l.top != r.top) AS top1_differs,
			-- Any visible change in the ten names we actually publish
			COUNT(*) FILTER (WHERE l.top10 != r.top10) AS top10_differs
		FROM (
			SELECT id_meso, lang, list_mode(list_sort(names)) AS top,
				-- Rebuild the published shape: unique names ranked by occurrence, capped at ten
				list_transform(list_sort(list_transform(list_distinct(names), x -> {{'c': -len(list_filter(names, y -> y = x)), 'n': x}})), s -> s.n)[1:10] AS top10
			FROM {left}
		) l
		JOIN (
			SELECT id_meso, lang, list_mode(list_sort(names)) AS top,
				list_transform(list_sort(list_transform(list_distinct(names), x -> {{'c': -len(list_filter(names, y -> y = x)), 'n': x}})), s -> s.n)[1:10] AS top10
			FROM {right}
		) r USING (id_meso, lang);
	""").fetchone()

# Pick the genus with the richest English vernacular coverage so the sample is not mostly empty rows.
def pick_sample_genus(db):
	# Rank genera by how many taxa carry a substantial English name list.
	return db.execute(f"""
		SELECT split_part(name_consensus, ' ', 1) AS genus, COUNT(*) AS taxa
		FROM base_vernacular
		WHERE lang = 'en' AND len(names) >= {SAMPLE_MIN_NAMES}
		GROUP BY genus
		ORDER BY taxa DESC, genus
		LIMIT 1;
	""").fetchone()

# Print the published English name list per variant for a fixed sample of taxa.
def print_sample(db, tables, genus):
	# Draw a stable random sample from the chosen genus so every variant shows the same taxa.
	rows = db.execute(f"""
		SELECT id_meso, name_consensus
		FROM base_vernacular
		WHERE lang = 'en' AND len(names) >= {SAMPLE_MIN_NAMES} AND split_part(name_consensus, ' ', 1) = '{genus}'
		-- Hash the UUID so the sample is random but reproducible across runs
		ORDER BY hash(id_meso)
		LIMIT {SAMPLE_SIZE};
	""").fetchall()
	# Render each sampled taxon with its raw input and every variant's published output.
	for id_meso, name in rows:
		# Show which taxon we are looking at.
		print(f"\n  {name}")
		# Show the raw multiset counts that go into the reduction.
		raw = db.execute(f"""
			SELECT list_transform(list_sort(list_transform(list_distinct(names), x -> {{'c': -len(list_filter(names, y -> y = x)), 'n': x}})), s -> concat(s.n, ' x', -s.c))
			FROM base_vernacular WHERE id_meso = ? AND lang = 'en'
		""", [id_meso]).fetchone()[0]
		# Print the raw occurrence counts so the reduction can be judged against them.
		print(f"    raw      : {', '.join(raw)}")
		# Print the published top ten for each variant under test.
		for variant, table in tables.items():
			# Rebuild the exact published shape for this taxon.
			out = db.execute(f"""
				SELECT list_transform(list_sort(list_transform(list_distinct(names), x -> {{'c': -len(list_filter(names, y -> y = x)), 'n': x}})), s -> s.n)[1:10]
				FROM {table} WHERE id_meso = ? AND lang = 'en'
			""", [id_meso]).fetchone()[0]
			# Show the winner separately because it becomes common_name.
			print(f"    {variant:9s}: [{out[0]}] {', '.join(out[1:])}")

# Print the sentinel taxa we already know flip between releases.
def print_sentinels(db, tables):
	# Walk each known-unstable name.
	for name in SENTINELS:
		# A scientific name can map to several meso rows, so pin the sentinel to the richest one.
		pinned = db.execute("""
			SELECT id_meso FROM base_vernacular
			WHERE name_consensus = ? AND lang = 'en'
			-- Pick the row that actually carries the vernacular payload, not an arbitrary namesake
			ORDER BY len(names) DESC, id_meso LIMIT 1
		""", [name]).fetchone()
		# Skip sentinels that carry no English names in the current data.
		if not pinned: print(f"\n  {name}\n    (no english names)"); continue
		# Show which taxon and which meso row we are reporting.
		print(f"\n  {name}  ({pinned[0]})")
		# Show the raw occurrence counts the reduction has to work from.
		raw = db.execute("""
			SELECT list_transform(list_sort(list_transform(list_distinct(names), x -> {'c': -len(list_filter(names, y -> y = x)), 'n': x})), s -> concat(s.n, ' x', -s.c))
			FROM base_vernacular WHERE id_meso = ? AND lang = 'en'
		""", [pinned[0]]).fetchone()[0]
		# Print the raw counts so each variant's winner can be judged against them.
		print(f"    raw      : {', '.join(raw)}")
		# Report every variant's published English list for this sentinel.
		for variant, table in tables.items():
			# Fetch the published English list for the pinned row.
			row = db.execute(f"""
				SELECT list_transform(list_sort(list_transform(list_distinct(names), x -> {{'c': -len(list_filter(names, y -> y = x)), 'n': x}})), s -> s.n)[1:10]
				FROM {table} WHERE id_meso = ? AND lang = 'en'
			""", [pinned[0]]).fetchone()
			# Print the winner and the rest of the published list.
			print(f"    {variant:9s}: [{row[0][0]}] {', '.join(row[0][1:])}")

# Run the full A/B across variants, orderings, correctness and runtime.
def main():
	# Install canopy settings before touching storage or processed files.
	init_runtime()
	# Keep one in-memory DuckDB connection for the whole probe.
	with duckdb.connect(':memory:') as db:
		# Route spills to the canopy temp directory like production does.
		db.execute(f"SET temp_directory = '{fuse_pipeline.TMP_DIR}'")
		# Announce the expensive base build.
		section("Building base meso state")
		# Time the shared setup so it is not attributed to any variant.
		start = time.time()
		# Build the map and enrich phases with production code.
		build_base_state(db)
		# Report how long the shared setup took.
		print(f"Base state built in {elapsed(start)}", flush=True)
		# Build the shared vernacular merge that every variant consumes.
		section("Merging vernacular sources")
		# Produce base_vernacular once.
		build_merged_vernacular(db)
		# Run every variant under every candidate ordering.
		section("Running variants")
		# Collect runtimes keyed by variant and ordering.
		timings = {}
		# Remember the table name of the primary run of each variant for reporting.
		primary = {}
		# Test each variant against three arrival orders of the identical multiset.
		for variant in ['current', 'proposed', 'sorted']:
			# Each ordering emulates a different array_agg arrival order.
			for ordering in ['original', 'reversed', 'rotated']:
				# Name the working table for this combination.
				table = f"v_{variant}_{ordering}"
				# Materialize the working copy under this ordering.
				make_working_copy(db, table, ordering)
				# Run the loop and record its runtime.
				timings[(variant, ordering)] = run_variant(db, table, variant)
				# Keep the original ordering as the variant's primary result.
				if ordering == 'original': primary[variant] = table
				# Report the finished combination.
				print(f"{variant:9s} / {ordering:9s} -> {timings[(variant, ordering)]:6.1f}s", flush=True)
		# Report loop runtimes side by side.
		section("Runtime (levenshtein loop only)")
		# Header for the timing table.
		print(f"{'variant':10s} {'original':>10s} {'reversed':>10s} {'rotated':>10s}")
		# One row per variant.
		for variant in ['current', 'proposed', 'sorted']:
			# Print the three orderings for this variant.
			print(f"{variant:10s} {timings[(variant,'original')]:9.1f}s {timings[(variant,'reversed')]:9.1f}s {timings[(variant,'rotated')]:9.1f}s")
		# Report correctness against the raw occurrence counts.
		section("Correctness (scored against raw occurrence counts)")
		# Header for the correctness table.
		print(f"{'variant':10s} {'rows':>10s} {'top lost':>10s} {'top demoted':>12s} {'rank1 tied':>11s}")
		# Score each variant's primary run.
		for variant in ['current', 'proposed', 'sorted']:
			# Fetch the four correctness counters.
			rows, lost, demoted, tied = score_variant(db, primary[variant])
			# Print them as one row.
			print(f"{variant:10s} {rows:10,} {lost:10,} {demoted:12,} {tied:11,}")
		# Report order-dependence, which is the release-churn metric.
		section("Determinism (same data, different candidate arrival order)")
		# Header for the stability table.
		print(f"{'variant':10s} {'rows':>10s} {'rank1 differs':>14s} {'top10 differs':>14s}")
		# Compare the original ordering against the other two for each variant.
		for variant in ['current', 'proposed', 'sorted']:
			# Accumulate differences across both alternate orderings.
			total = t1 = t10 = 0
			# Compare against reversed and rotated arrival orders.
			for ordering in ['reversed', 'rotated']:
				# Score this pair.
				rows, d1, d10 = score_stability(db, primary[variant], f"v_{variant}_{ordering}")
				# Accumulate the counters.
				total, t1, t10 = total + rows, t1 + d1, t10 + d10
			# Print the accumulated instability for this variant.
			print(f"{variant:10s} {total:10,} {t1:14,} {t10:14,}")
		# Show the known-unstable taxa explicitly.
		section("Sentinels")
		# Print sentinel outputs per variant.
		print_sentinels(db, primary)
		# Show a rich sample so the change can be eyeballed.
		genus, taxa = pick_sample_genus(db)
		# Announce which genus we sampled and why.
		section(f"Sample: {SAMPLE_SIZE} random taxa from genus '{genus}' ({taxa:,} taxa with >= {SAMPLE_MIN_NAMES} english names)")
		# Print the before/after lists.
		print_sample(db, primary, genus)

# Run the probe when invoked directly.
if __name__ == '__main__':
	# Execute the full comparison.
	main()

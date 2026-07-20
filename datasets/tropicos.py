#
# 		Tropicos - Missouri Botanical Garden
#		Complementiung IPNI, especially with North and South American Ferns & Bryophytes
#
#		"The Missouri Botanical Garden’s Herbarium is one of the world’s outstanding research resources for 
# 		specimens and information on bryophytes and vascular plants. The collection is limited to these two major 
# 		groups of plants. As of 31 December 2020 the herbarium collection had 6.93 million mounted specimens 
# 		(6.33 million vascular plants and 598,000 bryophytes). This specimen dataset includes over 4.7 million records
# 		(4.3 million vascular plants and 380,000 bryophytes)."
#
#		TODO: Check pre 1700 years as in https://www.tropicos.org/name/40016386
#		TODO: Follow up re https://www.tropicos.org/name/100351882
#		TODO: Extract typeStatus from occurrence data
#
# Internal
from ..utils.log import mesologger
from .. import SRC_DIR, TMP_DIR, settings

# Get latest version
import aiohttp

# File handling
import zipfile
from ..utils.filehandlers import fetch, get_file
# Load storage helper so the combined processor can hydrate both required archives together
from ..utils.s3 import storage

# DB
import duckdb
from ..utils.queries import build_rank_and_status, strip_author_from_name, strip_rank_from_name, find_hybrids, name_cleanup, validate, write_to_disc

# Name and default URL
sources = [{
	"name": "tropicos-specimens",
	"url": "http://ipt.mobot.org:8080/ipt/archive.do?r=tropicosspecimens",
	"suffix": ".zip",
	"citation": '<a href="https://tropicos.org/home" class="medium">Tropicos</a>, Teisher, J. & Stimmel, H. (YYYY). Tropicos MO Specimen Data. Version YYYY-MM-DD. Missouri Botanical Garden. <a href="https://doi.org/10.15468/hja69f" class="medium">https://doi.org/10.15468/hja69f</a>'
},{
	"name": "tropicos-nonmo",
	"url": "http://ipt.mobot.org:8080/ipt/archive.do?r=tropicosspecimensnonmo",
	"suffix": ".zip",
	"use_aria": 4
}]

async def update_tropicos(session):
	mesologger.info(f"############### Starting Tropicos Update  ###############")
	# Fetch timestamp as server doesn't provide Last-Modified header
	await get_tropicos_timestamp(session, sources)
	# Manually get latest unified processed file timestamp
	latest_processed = get_file('tropicos')
	# Attach timestamps and filenames to both file dicts
	if latest_processed: 
		sources[0]['timestamp_processed'] = sources[1]['timestamp_processed'] = int(latest_processed.split('.')[1])
		sources[0]['latest_processed'] = sources[1]['latest_processed'] = latest_processed
	# See if we have a new remote version vs. our latest download
	for source in sources: await fetch(session, source)
	# Process if we have a new version
	if settings.FORCE or not latest_processed or sources[0]['timestamp_processed'] < sources[0]['timestamp_download'] or sources[1]['timestamp_processed'] < sources[1]['timestamp_download']:
		# Skip processing when we only want fresh downloads
		if settings.DOWNLOAD_ONLY:
			sources[0]['name'] = 'tropicos'
			return sources[0]
		# Start processing and return our single source dict
		return process_tropicos(sources)	
	# Otherwise rewrite and return whatever latest processed we have
	sources[0]['name'] = 'tropicos'
	return sources[0]

# Process both Tropicos files
def process_tropicos(sources: dict):
	# Hydrate both companion archives when either source makes the combined output stale
	for source in sources:
		# Build the canonical source path for the active storage backend
		source_path = f"{SRC_DIR}/{source['latest_download']}"
		# Reuse an existing local file or download the required companion archive from S3
		source['local_path'] = storage.ensure_local(source_path, SRC_DIR)
	# Log both concrete source versions used by the combined processor
	mesologger.info(f"Starting to process { sources[0]['latest_download'] } and { sources[1]['latest_download'] }...")  
	# Load duckdb
	with duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Resolve local source paths for both Tropicos archives
		specimen_path = sources[0].get('local_path') or f"{SRC_DIR}/{sources[0]['latest_download']}"
		nonmo_path = sources[1].get('local_path') or f"{SRC_DIR}/{sources[1]['latest_download']}"
		# Open zips seperately, the tow datasets are either plants at Missouri University or external sources (Non-MO)
		specimen = zipfile.ZipFile(specimen_path)
		nonmo = zipfile.ZipFile(nonmo_path)
		# Load occurence.txts into DuckDB
		specimen_tsv = db.read_csv(specimen.open('occurrence.txt'),parallel=True)
		nonmo_tsv = db.read_csv(nonmo.open('occurrence.txt'),parallel=True)
		mesologger.info(f"Unpacked Tropicos archives")
		# Merge both archives (specimen + non-MO), dedup by taxonID keeping earliest year
		# ~301k rows after dedup. 96% have authors, 93% have year
		db.execute(f"""
		CREATE TABLE tropicos AS SELECT DISTINCT ON (id_raw)
			CAST(id_raw AS UINTEGER) id_raw,
			name_raw,
			name_clean,
			author_raw,
			-- only Plantae kingdom in source
			lower(kingdom) AS kingdom,
			-- ~739 families, 99% populated
			lower(family) AS family,
			-- ~18k genera, 99% populated
			lower(genus) AS genus,
			-- earliest year across duplicate records for same taxonID
			MIN(year) OVER (PARTITION BY id_raw) AS year
		FROM (
			SELECT
				taxonID AS id_raw,
				scientificName AS name_raw,
				lower(scientificName) AS name_clean,
				scientificNameAuthorship AS author_raw,
			 	kingdom,
			 	family,
			 	genus,
				year
			FROM specimen_tsv
			UNION ALL
			SELECT
				taxonID AS id_raw,
				scientificName AS name_raw,
				lower(scientificName) AS name_clean,
				scientificNameAuthorship AS author_raw,
			 	kingdom,
			 	family,
			 	genus,
				year
			FROM nonmo_tsv
		) combined_data
		""")
		# Log
		mesologger.info(f"Loaded { db.execute('SELECT COUNT(*) FROM tropicos').fetchone()[0] } plant names from Tropicos")
		# Rewrite sources
		source = sources[0]
		source['name'] = 'tropicos'
		# Strip author from name
		strip_author_from_name(db, source)
		# Tropicos has no rank column — infer from name structure
		# Abbreviated ranks like "var." are extracted via regex, single-word = genus, else species
		db.execute("""ALTER TABLE tropicos ADD COLUMN rank_raw VARCHAR;""")
		db.execute("""
			UPDATE tropicos
			SET rank_raw = CASE 
				WHEN name_clean LIKE '%[unranked]%' THEN 'unranked'
				-- extract abbreviated rank terms like "var.", "subsp.", "f."
				WHEN array_length(regexp_extract_all(name_clean, '\\b([a-z]+)\\.')) > 0 
				THEN (
					SELECT list_extract(filtered_items, 1)
					FROM (SELECT list_filter(regexp_extract_all(name_clean, '\\b([a-z]+)\\.'), 
			 				-- skip "st." (saint) to avoid false rank match, eg tropicos.org/name/22600183
							x -> NOT regexp_matches(x, '^(st)\\.$')
						) AS filtered_items)
					WHERE array_length(filtered_items) > 0
				)
				WHEN array_length(string_split(name_clean, ' ')) = 1 THEN 'genus'
				ELSE 'species'
			END
		""")
		# Clean up the ranks
		build_rank_and_status(db, source)
		# Find hybrids
		find_hybrids(db, source)
		# Strip ranks from name
		strip_rank_from_name(db, source)
		# Clean up names
		name_cleanup(db, source)
        # Check integrity
		validate(db, source)
		# Write to disc
		write_to_disc(db,source)
		# Close zips
		specimen.close()
		nonmo.close()
		# Return our source
		return source

async def get_tropicos_timestamp(session: aiohttp.ClientSession, sources: dict):
    # URL for Missouri Botanical Garden IPT data
    url = "http://ipt.mobot.org:8080/ipt/inventory/dataset"
    try:
        # Make the HTTP request using the provided session
        async with session.get(url) as response:
            # Check if the request was successful
            response.raise_for_status()  
            # Parse JSON response into a dictionary
            data = (await response.json()).get("registeredResources")
            # Set sources timestamp
            sources[0]['timestamp_remote'] = int(data[0].get('lastPublished').replace("-", ""))
            sources[1]['timestamp_remote'] = int(data[1].get('lastPublished').replace("-", ""))   
    except aiohttp.ClientError as e:
        mesologger.error(f"Error fetching Tropicos timestamp: {e}")

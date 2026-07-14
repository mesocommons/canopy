# Filter concatenated gzip members in parallel without mixing source file handling concerns
# Load logging, filesystem, process, signal, and resource helpers for gzip filtering
from .log import mesologger
import atexit, multiprocessing, os, signal, subprocess, sys, time, uuid
import psutil

# Load canopy paths and runtime settings used by worker processes
from .. import SRC_DIR, TMP_DIR, settings


# See what resources (cores and RAM) we have available for large file handling
def resources() -> list[int, int]:
    try:
        # Get CPU count - use cpu_count() for Windows compatibility
        cores = os.cpu_count()
        memory = int(psutil.virtual_memory().total / 1024**3)
        mesologger.info(f"{ cores} cores and { memory }GB memory available")
        return [cores, memory]
    except Exception as e:
        mesologger.warning(f"Unable to detect system cores and RAM, using 8GB and 8 threads {e}")
        return [8,8]

# Convert filtered JSON-array records into valid NDJSON without parsing their contents
def normalize(file: str):
	# Keep the original filtered artifact intact until the complete normalized replacement is ready
	cleaned = f"{file}.cleaning"
	# Refuse to overwrite residue that may contain evidence from an interrupted normalization
	if os.path.exists(cleaned): raise RuntimeError(f"Normalization output already exists {cleaned}")
	# Stream normalized records into a separate file so failures cannot corrupt the filtered input
	with open(cleaned, "wb") as output:
		# Remove only commas at physical line endings while passing every other line through unchanged
		result = subprocess.run(
			["rg", ",$", "--replace", "", "--passthru", "--no-line-number", "--no-filename", "--binary", "--mmap", file],
			stdout=output,
			stderr=subprocess.PIPE,
		)
	# Accept ripgrep's no-match status because already-normalized files are valid passthrough inputs
	if result.returncode not in [0, 1]:
		# Preserve the temporary output for diagnosis while surfacing ripgrep's exact failure
		error = result.stderr.decode("utf-8", errors="replace")
		raise RuntimeError(f"Unable to normalize {file}: ripgrep exited {result.returncode}: {error}")
	# Reject an empty replacement so a filtering or passthrough failure cannot erase useful input
	if os.path.getsize(cleaned) == 0: raise RuntimeError(f"Normalization produced an empty file {cleaned}")
	# Atomically expose valid NDJSON at the existing filtered artifact path
	os.replace(cleaned, file)
	# Log completion without introducing another persistent pipeline artifact
	mesologger.info(f"Normalized trailing JSON array separators in {os.path.basename(file)}")
	# Return the unchanged artifact path for compact caller flow
	return file

# Takes a gzipped file and filter criteria, splits the gzip in chunks, 
# runs them through ripgrep and produces one output file in temp dir
def filter(source: dict, pattern: str):
	mesologger.info(f"Filtering large gzip file { source['latest_download']}")
	# Sanity
	file = source.get('local_path') or os.path.join(SRC_DIR, source['latest_download'])
	if not os.path.isfile(file):
		mesologger.info(f"File not found { source['latest_download']}")
		return
	# Find independently decompressible gzip-member groups
	chunkoffsets = chunks(file)
	# Abort when no valid member boundaries were found
	if not chunkoffsets: return
	# Filter gzip-member groups concurrently
	result_files = parallel(file, chunkoffsets, pattern, resources()[0] // 3)
	try:
		filtered_filename = f"{ source['name'] }.{ source['timestamp_download']}.filtered"
		with open(os.path.join(TMP_DIR, filtered_filename), "w") as outfile:
			for chunk in result_files:
				# Extract just the filtered_filename part to check if it starts with "chunk_"
				if os.path.basename(chunk).startswith("chunk_"):
					mesologger.info(f"Adding { chunk } to { filtered_filename }")
					try:
						# Use the full path when opening the file
						with open(chunk, "r") as infile: outfile.write(infile.read())
						# Delete the chunk file
						os.remove(chunk)
					except Exception as e: mesologger.error(f"Error processing {chunk}: {e}")
		mesologger.info(f"All { len(result_files) } chunks merged into { filtered_filename }")
		# Convert copied JSON-array entries into valid newline-delimited JSON before DuckDB reads them
		normalize(os.path.join(TMP_DIR, filtered_filename))
		return filtered_filename
	except Exception as e:
		mesologger.error(f"Error {e}")
		# Abort processing rather than returning a missing or partially normalized artifact
		raise
	
# Split and scan gzipped file for gzip headers
def chunks(file):
	# Static
	header_magic = b'\x1f\x8b'
	buffer_size = 8192
	# Dynamic
	memory = resources()[1]
	file_size = os.path.getsize(file)
	file_size_gb = file_size / 1024**3
	num_chunks = int(file_size_gb / (memory / 12))
	chunk_size = file_size // num_chunks
	chunk_size_gb = chunk_size // 1024**3
	# Log
	mesologger.info(f"Dividing { file } into { num_chunks } chunks of { chunk_size_gb }GB each")
	# First boundary is always 0
	boundaries = [0]
	start_time = time.time()
	
	def validheader(f, pos):
		# Save current position
		current_pos = f.tell()
		try:
			# Go to the potential header position
			f.seek(pos)
			# Read first 10 bytes (minimum gzip header size)
			header = f.read(10)
			
			# Return False if we don't have enough bytes
			if len(header) < 10:
				return False
			
			# Check magic bytes
			if header[0:2] != header_magic:
				return False
			
			# Check compression method (should be 8 for DEFLATE)
			if header[2] != 8:
				return False
			
			# Try to actually read some decompressed data to validate
			f.seek(pos)
			try:
				import gzip
				decompressor = gzip.GzipFile(fileobj=f, mode='rb')
				# Try to read a small amount of decompressed data
				test_data = decompressor.read(1024)
				return len(test_data) > 0
			except Exception:
				return False
		finally:
			# Restore position
			f.seek(current_pos)
	
	# Open file
	with open(file, 'rb') as f:
		# go through each chunk
		for i in range(1, num_chunks):
			eof = False
			# Get position
			header_pos = i * chunk_size
			# Go to position
			f.seek(header_pos)
			# Keep looping
			while True:
				# Read into buffer
				buffer = f.read(buffer_size)
				# If we reached end of file
				if not buffer:
					eof = True
					break
				# Check for magic gzip header byte
				idx = buffer.find(header_magic)
				# If we found it
				if idx != -1:
					potential_header_pos = header_pos + idx
					# Validate if it's actually a gzip header
					if validheader(f, potential_header_pos):
						# Add the position (offset plus index position)
						boundaries.append(potential_header_pos)
						mesologger.info(f"Found valid header at position {potential_header_pos/1024/1024/1024:.2f} GB")
						break
					else:
						# False positive, continue searching after this position
						header_pos += idx + 2
						f.seek(header_pos)
						continue
				# Move position forward, but back up 1 byte in case the header spans chunks
				header_pos += len(buffer) - 1
				f.seek(header_pos)
			# In case we haven't found anything
			if eof:
				mesologger.info(f"No header found in chunk { i }, after position {header_pos/1024/1024/1024:.2f} GB")
				return False
	# Add the file size as the last boundary
	boundaries.append(file_size)
	end_time = time.time()
	mesologger.info(f"Found all {len(boundaries)-1} chunk headers in {end_time - start_time:.2f} seconds")
	return boundaries

# Global flag for tracking termination
terminated = False

# List to keep track of all child processes
processesall = []

def cleanup():
	"""Kill all registered processes on exit"""
	global processesall
	for proc in processesall:
		try:
			if hasattr(proc, 'terminate'):
				proc.terminate()
			elif hasattr(proc, 'kill'):
				proc.kill()
		except:
			pass

# Register cleanup on exit
atexit.register(cleanup)

def parallel(archive_path, chunks, pattern, max_workers=2):
	"""
	Process multiple chunks of the gzipped archive in parallel.
   
	Args:
		archive_path: Path to the gzipped archive
		chunks: List of chunk offsets
		pattern: Pattern to search for using ripgrep
		max_workers: Maximum number of parallel workers (defaults to CPU count)
       
	Returns:
		List of paths to temporary files containing matches
	"""
	global terminated
	terminated = False
	
	mesologger.info(f"Starting parallel processing with {max_workers} workers")
	
	# Set up signal handler for Ctrl+C
	def sigint_handler(sig, frame):
		global terminated
		mesologger.info("Received Ctrl+C. Aborting all processes...")
		terminated = True
		
		# Call cleanup immediately
		cleanup()
		
		# Force exit on second Ctrl+C
		signal.signal(signal.SIGINT, lambda s, f: os._exit(1))
	
	original_sigint_handler = signal.getsignal(signal.SIGINT)
	signal.signal(signal.SIGINT, sigint_handler)
	
	# Prepare chunk arguments
	chunk_args = []
	for i in range(len(chunks) - 1):
		chunk_args.append((archive_path, chunks[i], pattern, chunks[i+1], i))
	
	# Process chunks in parallel using direct multiprocessing
	results = []
	processes = []
	result_queue = multiprocessing.Queue()
	
	try:
		# Start at most max_workers processes
		running_processes = 0
		next_chunk = 0
		
		# Continue loop until all chunks are assigned AND all processes are complete
		while (next_chunk < len(chunk_args) or processes) and not terminated:
			# Start processes up to max_workers
			while running_processes < max_workers and next_chunk < len(chunk_args):
				if terminated:
					break
					
				args = chunk_args[next_chunk]
				p = multiprocessing.Process(
					target=worker,
					args=(args, result_queue)
				)
				p.daemon = True  # Set as daemon so it exits when main process exits
				p.start()
				processes.append(p)
				processesall.append(p)  # Add to global list for cleanup
				running_processes += 1
				next_chunk += 1
			
			# Check for completed processes and results
			for p in list(processes):
				if not p.is_alive():
					processes.remove(p)
					running_processes -= 1
			
			# Check for results without blocking
			while not result_queue.empty():
				result = result_queue.get_nowait()
				if result is not None:
					results.append(result)
			
			# Small sleep to prevent CPU hogging
			time.sleep(0.1)
		
		# Wait for remaining processes to finish or terminate them
		if terminated:
			for p in processes:
				if p.is_alive():
					p.terminate()
		else:
			# Wait for all processes to complete
			for p in processes:
				p.join(timeout=1)
				if p.is_alive():
					p.terminate()
			
			# Get any remaining results
			while not result_queue.empty():
				result = result_queue.get_nowait()
				if result is not None:
					results.append(result)
	
	except KeyboardInterrupt:
		mesologger.info("Interrupt received in main process. Terminating all workers...")
		terminated = True
		
		# Terminate all processes
		for p in processes:
			if p.is_alive():
				p.terminate()
	
	finally:
		# Clean up
		signal.signal(signal.SIGINT, original_sigint_handler)
		
		if terminated:
			mesologger.info(f"Processing aborted. Processed {len(results)}/{len(chunk_args)} chunks before abort")
		else:
			mesologger.info(f"Parallel processing complete. Processed {len(results)}/{len(chunk_args)} chunks successfully")
	
	return results

def worker(args, result_queue):
	"""Wrapper to handle chunk and put result in queue"""
	try:
		# Set up process-specific signal handler
		def proc_sigint_handler(sig, frame):
			# Just exit the process
			sys.exit(0)
		
		signal.signal(signal.SIGINT, proc_sigint_handler)
		
		# Process the chunk
		result = chunk(*args)
		
		# Put result in queue
		result_queue.put(result)
	except KeyboardInterrupt:
		# Handle interrupt gracefully
		sys.exit(0)
	except Exception as e:
		mesologger.error(f"Error in worker process: {str(e)}")
		result_queue.put(None)
		sys.exit(1)

def chunk(archive_path, chunk_offset, pattern, next_chunk_offset, chunk_id):
	"""
	Process a single chunk of the gzipped archive starting at chunk_offset.
	Uses pigz to decompress and ripgrep to filter, writing results to a temporary file.
   
	Args:
		archive_path: Path to the gzipped archive
		chunk_offset: Byte offset of the gzip header to start from
		pattern: Pattern to search for using ripgrep
		next_chunk_offset: Byte offset of the next chunk (to limit reading)
		chunk_id: Identifier for this chunk (used for progress tracking)
	   
	Returns:
		Path to the temporary file containing matches or None if an error occurred
	"""
	global terminated, processesall
	
	# Generate a random filename for the temporary results
	temp_file = os.path.join(TMP_DIR, f"chunk_{uuid.uuid4().hex}.txt")
   
	# Calculate chunk size
	chunk_size = next_chunk_offset - chunk_offset
   
	# Buffer size optimized for 5GB chunks and multiple parallel processes
	# Using 16MB as a good balance for large chunks
	buffer_size = 64 * 1024 * 1024  # 16MB buffer
   
	# Prepare ripgrep command to filter the results with performance optimizations
	# Using --binary for explicit binary mode and mmap for faster file access
	rg_cmd = ["rg", pattern, "--no-line-number", "--no-filename", "--binary", "--mmap", "--dfa-size-limit=100M"]
   
	pigz_process = None
	rg_process = None
	
	try:
		# Start pigz process for decompression with focus on I/O optimization
		# -b 512: Use 512k block size for better throughput
		pigz_process = subprocess.Popen(["pigz", "-d", "-c", "-b", "512"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=buffer_size)
		processesall.append(pigz_process)
	   
		# Start ripgrep process for filtering with binary mode
		with open(temp_file, "wb") as output_file:
			rg_process = subprocess.Popen(rg_cmd, stdin=pigz_process.stdout, stdout=output_file, stderr=subprocess.PIPE, bufsize=buffer_size)
			processesall.append(rg_process)
		   
			# Close pigz's stdout in the parent process
			pigz_process.stdout.close()
		
		# Open the archive file and feed the chunk to pigz using memory mapping when available
		with open(archive_path, "rb") as archive_file:
			# Seek to the chunk offset
			archive_file.seek(chunk_offset)
			mesologger.info(f"Starting to process chunk { chunk_id }")			
			# Read and feed data in chunks to avoid excessive memory usage
			bytes_remaining = chunk_size
			start_time = time.time()
			total_processed = 0
			last_log_time = time.time()
			
			while bytes_remaining > 0:
				# Check for termination request
				if terminated:
					raise KeyboardInterrupt("Processing terminated")
					
				# Calculate how much to read in this iteration
				read_size = min(buffer_size, bytes_remaining)
			   
				# Read a chunk from the file
				data = archive_file.read(read_size)
				if not data:  # EOF reached
					break
				
				# Track throughput
				chunk_size_bytes = len(data)
				total_processed += chunk_size_bytes
				
				# Write to pigz's stdin
				pigz_process.stdin.write(data)
			   
				# Update remaining bytes
				bytes_remaining -= chunk_size_bytes
				
				# Only log every second to avoid overwhelming with messages
				current_time = time.time()
				if current_time - last_log_time >= 1.0:
					elapsed = current_time - start_time
					mb_per_sec = (total_processed / 1024 / 1024) / elapsed if elapsed > 0 else 0					
					mesologger.info(f"Chunk {chunk_id}: Remaining: {bytes_remaining/1024/1024:.2f} MB | Processed: {total_processed/1024/1024:.2f} MB | Speed: {mb_per_sec:.2f} MB/s")
					
					last_log_time = current_time
			
			# Final stats after processing all data
			elapsed = time.time() - start_time
			mb_per_sec = (total_processed / 1024 / 1024) / elapsed if elapsed > 0 else 0
			
			mesologger.info(f"CHUNK {chunk_id} COMPLETE Total processed: {total_processed/1024/1024:.2f} MB | Avg Speed: {mb_per_sec:.2f} MB/s")
			
			# Close pigz's stdin to signal end of input
			pigz_process.stdin.close()
	   
		# Only wait for processes if not terminating
		if not terminated:
			# Wait for processes to complete with a reasonable timeout
			pigz_exit_code = pigz_process.wait(timeout=600)  # 10 min timeout
			rg_exit_code = rg_process.wait(timeout=600)
		   
			# Check for errors - handle gzip corruption more gracefully
			if pigz_exit_code != 0:
				pigz_error = pigz_process.stderr.read().decode('utf-8', errors='replace')
				if "corrupted input" in pigz_error and bytes_remaining == 0:
					# This might be expected if we're cutting across gzip stream boundaries
					if settings.VERBOSE: mesologger.info(f"Possible gzip boundary at end of chunk, processing as much as possible")
					return temp_file
				else:
					mesologger.error(f"pigz error (code {pigz_exit_code}): {pigz_error}")
					return None
			   
			if rg_exit_code != 0 and rg_exit_code != 1:  # ripgrep returns 1 when no matches found
				rg_error = rg_process.stderr.read().decode('utf-8', errors='replace')
				mesologger.error(f"ripgrep error (code {rg_exit_code}): {rg_error}")
				return None
		   
			return temp_file
		else:
			return None
	   
	except subprocess.TimeoutExpired:
		mesologger.info(f"Timeout processing chunk at offset {chunk_offset}")
		return None
		
	except KeyboardInterrupt:
		mesologger.warning(f"Chunk {chunk_id} aborted by user")
		return None
		
	except Exception as e:
		mesologger.error(f"Error processing chunk at offset {chunk_offset}: {str(e)}")
		return None
	
	finally:
		# Kill processes in this process
		if pigz_process and hasattr(pigz_process, 'poll') and pigz_process.poll() is None:
			try: pigz_process.kill()
			except: pass
		if rg_process and hasattr(rg_process, 'poll') and rg_process.poll() is None:
			try: rg_process.kill()
			except: pass
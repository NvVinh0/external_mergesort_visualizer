# Function Parameter Docs

## `generate_input_file(path, n, seed=42, chunk_items=None, chunk_mb=64)`
- `path`: Output binary file path. Relative paths are resolved from project root.
- `n`: Number of float64 values to generate.
- `seed`: Random seed for reproducible data.
- `chunk_items`: Number of values generated/written per chunk.
- `chunk_mb`: Chunk size in MB when `chunk_items` is not set. Formula: `(chunk_mb * 1024 * 1024) // 8`.

## `suggest_memory_items(fraction_of_avail=0.6, safety_factor=0.8)`
- `fraction_of_avail`: Fraction of currently available RAM to reserve for the sorter.
- `safety_factor`: Fraction of computed `memory_items` recommended for run generation.

## `tune_io(memory_items, requested_fan_in, soft_nofile, reserve_fds=32, min_block_items=4096)`
- `memory_items`: Total in-memory float64 capacity for buffers.
- `requested_fan_in`: Requested K-way merge fan-in, or `None` for auto tuning.
- `soft_nofile`: OS soft file descriptor limit.
- `reserve_fds`: File descriptors reserved for non-merge needs.
- `min_block_items`: Minimum read/write block size in float64 items.

## `use_heap_for_k(k, c=6.0, max_argmin_k=32)`
- `k`: Number of active merge inputs.
- `c`: Heuristic multiplier in `k > c * log2(k)`.
- `max_argmin_k`: Always use heap above this `k`.

## `external_sort(input_path, output_path, memory_items=1_000_000, fan_in=None, run_generation="internal", parallel_workers=None, chunk_sort_backend="thread")`
- `input_path`: Source binary float64 file. Relative paths are resolved from project root.
- `output_path`: Destination sorted binary float64 file. Relative paths are resolved from project root.
- `memory_items`: In-memory budget in float64 items.
- `fan_in`: Merge fan-in (`None` = auto tune based on memory/FD limits).
- `run_generation`: Run generation method: `internal` or `replacement`.
- `parallel_workers`: Number of workers used for internal run generation and per-pass merge groups.
  Set `1` to force sequential execution, or `None` for automatic selection.
- `chunk_sort_backend`: Internal run-generation backend, `thread` or `process`.
  Use `process` for CPU-bound chunk sorting; if process pool setup fails, code falls back to `thread`.

## `BufferedFloatWriter.write_direct(arr)`
- `arr`: NumPy array written directly to output file.
- Behavior: flushes internal buffer first, then streams `arr` directly with `tofile(...)`.
- Intended use: large contiguous writes in merge fast paths.

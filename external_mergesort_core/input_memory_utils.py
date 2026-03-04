import os

import numpy as np

'''
RUNNING INSTRUCTIONS:
Import these helpers from `external_mergesort_core.input_memory_utils`.

PARAMETER DESCRIPTION:
This module provides utilities for available-memory estimation and
input generation for external merge sort workloads.
'''

F64_BYTES = np.dtype(np.float64).itemsize
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


def get_available_ram_bytes():
    '''
    PARAMETER DESCRIPTION:
    This helper takes no input parameters.

    RETURNS:
    Estimated available RAM in bytes.

    NOTES:
    Uses `psutil` when available, then `/proc/meminfo` fallback, then 1 GiB default.
    '''
    try:
        import psutil

        return int(psutil.virtual_memory().available)
    except Exception:
        pass

    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    parts = line.split()
                    return int(parts[1]) * 1024
    except Exception:
        pass

    return 1 * 1024**3


def suggest_memory_items(fraction_of_avail=0.6, safety_factor=0.8):
    '''
    PARAMETER DESCRIPTION:
    fraction_of_avail : Fraction of currently available RAM reserved for sorting.
    safety_factor : Additional safety multiplier applied to computed memory items.

    RETURNS:
    Dictionary with `avail_bytes`, `memory_bytes`, `memory_items`, and `run_items`.

    EXAMPLE:
    suggest_memory_items(fraction_of_avail=0.6, safety_factor=0.8)
    '''
    avail_bytes = get_available_ram_bytes()
    memory_bytes = int(avail_bytes * float(fraction_of_avail))
    memory_items = int(memory_bytes // F64_BYTES)
    run_items = int(memory_items * float(safety_factor))
    return {
        "avail_bytes": avail_bytes,
        "memory_bytes": memory_bytes,
        "memory_items": memory_items,
        "run_items": run_items,
    }


def generate_input_file(
    path: str,
    n: int,
    seed: int = 42,
    chunk_items: int | None = None,
    chunk_mb: int = 64,
):
    '''
    RUNNING INSTRUCTIONS:
    Call from Python to create an unsorted binary float64 input file.

    PARAMETER DESCRIPTION:
    path : Output file path to write generated values.
    n : Number of float64 values to generate.
    seed : Random seed for reproducible output.
    chunk_items : Number of values generated/written per iteration.
      If None, it is derived from `chunk_mb`.
    chunk_mb : Chunk size in MB used when `chunk_items` is None.
      Effective formula: `(chunk_mb * 1024 * 1024) // 8`.

    EXAMPLE:
    generate_input_file("input.bin", n=5_000_000, seed=42, chunk_mb=128)
    '''
    if chunk_items is None:
        chunk_items = max(1, (int(chunk_mb) * 1024 * 1024) // F64_BYTES)
    else:
        chunk_items = max(1, int(chunk_items))

    path = _resolve_path(path)
    rng = np.random.default_rng(seed)
    with open(path, "wb") as f:
        left = int(n)
        while left:
            k = min(left, chunk_items)
            rng.uniform(-1e9, 1e9, size=k).tofile(f)
            left -= k

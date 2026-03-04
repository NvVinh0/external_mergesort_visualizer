import os

import numpy as np

'''
RUNNING INSTRUCTIONS:
These helpers are intended to be called by executor workers.

PARAMETER DESCRIPTION:
This module provides chunk-level sort worker functions for
parallel internal run generation.
'''


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


def sort_chunk_to_run(input_path: str, start: int, end: int, out_path: str):
    '''
    PARAMETER DESCRIPTION:
    input_path : Source binary file containing float64 values.
    start : Start index (inclusive) of the chunk to sort.
    end : End index (exclusive) of the chunk to sort.
    out_path : Destination run file path for sorted chunk output.

    RETURNS:
    Tuple `(out_path, n_items_written)`.

    EXAMPLE:
    sort_chunk_to_run("input.bin", 0, 500000, "run_000000.bin")
    '''
    input_path = _resolve_path(input_path)
    out_path = _resolve_path(out_path)
    src = np.memmap(input_path, dtype=np.float64, mode="r")
    arr = np.array(src[int(start):int(end)], copy=True)
    arr.sort(kind="quicksort")
    arr.tofile(out_path)
    n = int(arr.size)
    del src
    return out_path, n


def sort_chunk_to_run_task(task):
    '''
    PARAMETER DESCRIPTION:
    task : Tuple form of `(input_path, start, end, out_path)`.

    RETURNS:
    Same return value as `sort_chunk_to_run(...)`.
    '''
    return sort_chunk_to_run(*task)

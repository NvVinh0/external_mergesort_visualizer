import math
import os
from typing import Optional

import numpy as np

'''
RUNNING INSTRUCTIONS:
Import runtime IO and merge helpers from `external_mergesort_core.merge_runtime`.

PARAMETER DESCRIPTION:
This module contains reusable low-level components for file descriptor
tuning and buffered float64 IO used by external merge sort.
'''

try:
    import resource  # Unix only
except ImportError:
    resource = None


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


def get_soft_nofile(default_soft: int = 256) -> int:
    '''
    PARAMETER DESCRIPTION:
    default_soft : Fallback soft file-descriptor limit when OS query is unavailable.

    RETURNS:
    Soft open-file limit as integer.
    '''
    if resource is None:
        return default_soft
    soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft <= 0 or soft >= 10**9:
        return default_soft
    return int(soft)


def tune_io(
    memory_items: int,
    requested_fan_in: Optional[int],
    soft_nofile: int,
    reserve_fds: int = 32,
    min_block_items: int = 4096,
) -> tuple[int, int, int]:
    '''
    PARAMETER DESCRIPTION:
    memory_items : Total in-memory capacity measured in float64 items.
    requested_fan_in : Desired merge fan-in, or None for automatic tuning.
    soft_nofile : Current soft limit for open file descriptors.
    reserve_fds : File descriptors reserved for non-merge activities.
    min_block_items : Lower bound for read/write block sizes.

    RETURNS:
    Tuple `(fan_in, read_block_items, write_block_items)`.
    '''
    memory_items = int(memory_items)
    max_fan_in_by_fd = max(2, int(soft_nofile) - int(reserve_fds))
    max_fan_in_by_mem = max(2, memory_items // min_block_items - 1)

    if requested_fan_in is None:
        fan_in = min(64, max_fan_in_by_fd, max_fan_in_by_mem)
    else:
        fan_in = max(2, min(int(requested_fan_in), max_fan_in_by_fd, max_fan_in_by_mem))

    read_block_items = max(min_block_items, memory_items // (fan_in + 1))
    write_block_items = read_block_items

    fan_in = max(2, min(fan_in, memory_items // read_block_items - 1, max_fan_in_by_fd))
    read_block_items = max(min_block_items, memory_items // (fan_in + 1))
    write_block_items = read_block_items
    return fan_in, read_block_items, write_block_items


def use_heap_for_k(k: int, c: float = 6.0, max_argmin_k: int = 32) -> bool:
    '''
    PARAMETER DESCRIPTION:
    k : Number of active runs participating in K-way merge.
    c : Threshold multiplier in heuristic `k > c * log2(k)`.
    max_argmin_k : Upper limit where argmin strategy is still considered.

    RETURNS:
    True if heap-based merge should be used, else False.
    '''
    if k <= 1:
        return False
    if k > max_argmin_k:
        return True
    return k > (math.log2(k) * c)


class BinaryFloatReader:
    '''
    PARAMETER DESCRIPTION:
    path : Binary float64 file path.
    block_items : Buffered read size in number of float64 items.

    NOTES:
    Provides chunked and scalar read helpers for merge phases.
    '''
    __slots__ = ("fp", "block_items", "buf", "i")

    def __init__(self, path: str, block_items: int):
        self.fp = open(_resolve_path(path), "rb")
        self.block_items = int(block_items)
        self.buf = np.empty(0, dtype=np.float64)
        self.i = 0

    def next(self) -> Optional[np.float64]:
        i = self.i
        buf = self.buf
        if i >= buf.size:
            buf = np.fromfile(self.fp, dtype=np.float64, count=self.block_items)
            if buf.size == 0:
                self.buf = buf
                self.i = 0
                return None
            self.buf = buf
            i = 0
        v = buf[i]
        self.i = i + 1
        return v

    def read_batch(self, batch_items: int) -> np.ndarray:
        if self.i >= self.buf.size:
            self.buf = np.fromfile(self.fp, dtype=np.float64, count=self.block_items)
            self.i = 0
            if self.buf.size == 0:
                return np.empty(0, dtype=np.float64)
        take = min(int(batch_items), self.buf.size - self.i)
        out = self.buf[self.i:self.i + take]
        self.i += take
        return out

    def read_items(self, count_items: int) -> np.ndarray:
        out = np.empty(int(count_items), dtype=np.float64)
        n = 0
        out_size = out.size
        while n < out_size:
            if self.i >= self.buf.size:
                self.buf = np.fromfile(self.fp, dtype=np.float64, count=self.block_items)
                self.i = 0
                if self.buf.size == 0:
                    break
            take = min(out_size - n, self.buf.size - self.i)
            out[n:n + take] = self.buf[self.i:self.i + take]
            self.i += take
            n += take
        return out[:n]

    def drain_tail(self) -> np.ndarray:
        if self.i < self.buf.size:
            tail = self.buf[self.i:].copy()
            self.i = self.buf.size
            return tail
        return np.empty(0, dtype=np.float64)

    def read_chunk(self) -> np.ndarray:
        return np.fromfile(self.fp, dtype=np.float64, count=self.block_items)

    def close(self) -> None:
        self.fp.close()


class RunCursor:
    '''
    PARAMETER DESCRIPTION:
    reader : `BinaryFloatReader` instance.
    mini_batch : Number of items loaded per cursor refill.

    NOTES:
    Wraps buffered readers for efficient K-way merge iteration.
    '''
    __slots__ = ("reader", "mini_batch", "batch", "idx")

    def __init__(self, reader: BinaryFloatReader, mini_batch: int):
        self.reader = reader
        self.mini_batch = int(mini_batch)
        self.batch = np.empty(0, dtype=np.float64)
        self.idx = 0

    def next(self) -> Optional[np.float64]:
        if self.idx >= self.batch.size:
            self.batch = self.reader.read_batch(self.mini_batch)
            self.idx = 0
            if self.batch.size == 0:
                return None
        v = self.batch[self.idx]
        self.idx += 1
        return v


class BufferedFloatWriter:
    '''
    PARAMETER DESCRIPTION:
    path : Output binary float64 path.
    block_items : Buffered write size in number of float64 items.

    NOTES:
    Accumulates output in memory and flushes in large contiguous blocks.
    '''
    __slots__ = ("fp", "buf", "n")

    def __init__(self, path: str, block_items: int):
        self.fp = open(_resolve_path(path), "wb")
        self.buf = np.empty(int(block_items), dtype=np.float64)
        self.n = 0

    def write(self, v: np.float64 | float) -> None:
        self.buf[self.n] = v
        self.n += 1
        if self.n == self.buf.size:
            self.flush()

    def write_array(self, arr: np.ndarray) -> None:
        j = 0
        m = arr.size
        while j < m:
            room = self.buf.size - self.n
            take = min(room, m - j)
            self.buf[self.n:self.n + take] = arr[j:j + take]
            self.n += take
            j += take
            if self.n == self.buf.size:
                self.flush()

    def write_direct(self, arr: np.ndarray) -> None:
        """
        Flush internal buffer and write arr directly to file.
        Used for large streaming writes.
        """
        if self.n:
            self.flush()
        arr.tofile(self.fp)

    def flush(self) -> None:
        if self.n:
            self.buf[:self.n].tofile(self.fp)
            self.n = 0

    def close(self) -> None:
        self.flush()
        self.fp.close()

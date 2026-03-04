import heapq
import os
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

'''
RUNNING INSTRUCTIONS:
$ python external_merge_sort.py

PARAMETER DESCRIPTION:
This script demonstrates an external merge sort for float64 binary files.
Main runtime parameters are configured in `external_sort(...)` and the
example call under `if __name__ == "__main__":`.

EXAMPLE:
$ python external_merge_sort.py

This generates `input.bin`, sorts it into `output.bin`, and prints stats.
'''

import numpy as np
from external_mergesort_core.chunk_sorting import sort_chunk_to_run_task
from external_mergesort_core.input_memory_utils import generate_input_file, get_available_ram_bytes, suggest_memory_items
from external_mergesort_core.merge_runtime import (
    BinaryFloatReader,
    BufferedFloatWriter,
    RunCursor,
    get_soft_nofile,
    tune_io,
    use_heap_for_k,
)


F64_BYTES = np.dtype(np.float64).itemsize
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


def _resolve_path(path: str) -> str:
    '''
    Resolve relative paths from the project root directory.
    '''
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


def _choose_parallel_workers(task_count: int, requested_workers, est_worker_bytes: int = 0) -> int:
    '''
    PARAMETER DESCRIPTION:
    task_count : Number of parallel tasks currently available.
    requested_workers : User-requested worker count or None for auto.
    est_worker_bytes : Estimated memory footprint per worker.

    RETURNS:
    Worker count chosen with CPU and RAM constraints.
    '''
    if task_count <= 1:
        return 1
    if requested_workers is not None:
        return max(1, min(int(requested_workers), int(task_count)))

    cpu = os.cpu_count() or 1
    workers = min(cpu, int(task_count))
    if est_worker_bytes > 0:
        # Keep a margin so parallel workers do not consume all available RAM.
        avail = get_available_ram_bytes()
        by_mem = max(1, int((avail * 0.7) // int(est_worker_bytes)))
        workers = max(1, min(workers, by_mem))
    return max(1, workers)


def create_sorted_runs_parallel(
    input_path: str,
    tmpdir: str,
    run_items: int,
    parallel_workers=None,
    chunk_sort_backend: str = "thread",
):
    '''
    PARAMETER DESCRIPTION:
    input_path : Binary input file containing float64 values.
    tmpdir : Temporary directory where run files are written.
    run_items : Number of float64 items per initial sorted run.
    parallel_workers : Worker count for parallel chunk sorting.
    chunk_sort_backend : `thread` or `process`.

    EXAMPLE:
    create_sorted_runs_parallel("input.bin", "tmp", 500000, parallel_workers=4, chunk_sort_backend="process")
    '''
    input_path = _resolve_path(input_path)
    src = np.memmap(input_path, dtype=np.float64, mode="r")
    n_items = int(src.size)
    del src
    tasks = []
    for run_id, start in enumerate(range(0, n_items, int(run_items))):
        end = min(start + int(run_items), n_items)
        out_path = os.path.join(tmpdir, f"run_{run_id:06d}.bin")
        tasks.append((input_path, start, end, out_path))

    workers = _choose_parallel_workers(
        task_count=len(tasks),
        requested_workers=parallel_workers,
        est_worker_bytes=int(run_items) * F64_BYTES * 2,
    )
    if workers == 1:
        return [sort_chunk_to_run_task(t) for t in tasks]

    if chunk_sort_backend not in ("thread", "process"):
        raise ValueError("chunk_sort_backend must be 'thread' or 'process'")

    if chunk_sort_backend == "process":
        try:
            with ProcessPoolExecutor(max_workers=workers) as ex:
                runs = list(ex.map(sort_chunk_to_run_task, tasks))
        except Exception:
            # Windows spawn/import edge-cases can break process pools in some hosts.
            # Fallback preserves correctness while keeping parallelism.
            with ThreadPoolExecutor(max_workers=workers) as ex:
                runs = list(ex.map(sort_chunk_to_run_task, tasks))
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            runs = list(ex.map(sort_chunk_to_run_task, tasks))
    runs.sort(key=lambda x: x[0])
    return runs


def replacement_selection_runs(
    input_path: str,
    tmpdir: str,
    memory_items: int,
    read_block_items: int,
    write_block_items: int,
):
    '''
    PARAMETER DESCRIPTION:
    input_path : Binary input file containing float64 values.
    tmpdir : Temporary directory where run files are written.
    memory_items : Heap capacity in float64 items for replacement selection.
    read_block_items : Buffered read block size (float64 items).
    write_block_items : Buffered write block size (float64 items).

    EXAMPLE:
    replacement_selection_runs("input.bin", "tmp", 500000, 32768, 32768)
    '''
    input_path = _resolve_path(input_path)
    runs = []
    reader = BinaryFloatReader(input_path, block_items=read_block_items)
    try:
        init = reader.read_items(memory_items)
        if init.size == 0:
            return runs

        active = init.tolist()
        frozen = []
        heapify = heapq.heapify
        heappop = heapq.heappop
        heappush = heapq.heappush
        heapify(active)

        run_idx = 0
        run_path = os.path.join(tmpdir, f"run_{run_idx:06d}.bin")
        writer = BufferedFloatWriter(run_path, block_items=write_block_items)
        write_one = writer.write
        runs.append(run_path)

        last_out = np.float64(-np.inf)
        next_value = reader.next

        while active:
            x = heappop(active)
            write_one(x)
            last_out = x

            nxt = next_value()
            if nxt is not None:
                if nxt >= last_out:
                    heappush(active, nxt)
                else:
                    frozen.append(nxt)

            if not active:
                writer.close()
                if frozen:
                    run_idx += 1
                    run_path = os.path.join(tmpdir, f"run_{run_idx:06d}.bin")
                    writer = BufferedFloatWriter(run_path, block_items=write_block_items)
                    write_one = writer.write
                    runs.append(run_path)
                    active = frozen
                    frozen = []
                    heapify(active)
                    last_out = np.float64(-np.inf)

        return runs
    finally:
        reader.close()


def _run_group_total_items(run_group):
    '''
    PARAMETER DESCRIPTION:
    run_group : List of tuples `(run_path, item_count)`.

    RETURNS:
    Total item count across all runs in the group.
    '''
    total_items = 0
    for _, n_items in run_group:
        total_items += n_items
    return total_items


def copy_binary_file(src_path: str, dst_path: str, chunk_bytes: int = 4 * 1024 * 1024):
    '''
    PARAMETER DESCRIPTION:
    src_path : Source binary file path.
    dst_path : Destination binary file path.
    chunk_bytes : Copy buffer size in bytes.

    EXAMPLE:
    copy_binary_file("run_000000.bin", "final_output.bin")
    '''
    src_path = _resolve_path(src_path)
    dst_path = _resolve_path(dst_path)
    with open(src_path, "rb") as src, open(dst_path, "wb") as dst:
        while True:
            chunk = src.read(chunk_bytes)
            if not chunk:
                break
            dst.write(chunk)


def merge_group_heap(run_group, out_path, read_block_items, write_block_items):
    '''
    PARAMETER DESCRIPTION:
    run_group : List of sorted run files to merge.
    out_path : Destination file for merged output.
    read_block_items : Per-run buffered read size.
    write_block_items : Buffered write size.

    NOTES:
    Uses heap-based K-way merge with buffered IO.
    '''
    readers = [BinaryFloatReader(path, read_block_items) for path, _ in run_group]
    mini_batch_items = max(8, min(256, int(read_block_items) // 32))
    cursors = [RunCursor(reader, mini_batch_items) for reader in readers]
    writer = BufferedFloatWriter(out_path, write_block_items)
    heap = []
    out_buf = np.empty(int(write_block_items), dtype=np.float64)
    out_n = 0

    heappop = heapq.heappop
    heapreplace = heapq.heapreplace
    heapify = heapq.heapify
    write_array = writer.write_array
    write_one = writer.write
    searchsorted = np.searchsorted

    try:
        for cursor in cursors:
            v = cursor.next()
            if v is not None:
                heap.append([v, cursor])
        heapify(heap)

        while heap:
            if len(heap) == 1:
                v, cursor = heap[0]
                if out_n:
                    write_array(out_buf[:out_n])
                    out_n = 0
                writer.write_direct(np.array((v,), dtype=np.float64))

                if cursor.idx < cursor.batch.size:
                    writer.write_direct(cursor.batch[cursor.idx:])
                tail = cursor.reader.drain_tail()
                if tail.size:
                    writer.write_direct(tail)
                read_chunk = cursor.reader.read_chunk
                while True:
                    chunk = read_chunk()
                    if chunk.size == 0:
                        break
                    writer.write_direct(chunk)
                break

            item = heap[0]
            v, cursor = item
            second = heap[1][0]
            if len(heap) > 2 and heap[2][0] < second:
                second = heap[2][0]

            batch = cursor.batch
            idx = cursor.idx
            read_batch = cursor.reader.read_batch
            mini_batch = cursor.mini_batch
            while True:
                out_buf[out_n] = v
                out_n += 1
                if out_n == out_buf.size:
                    write_array(out_buf)
                    out_n = 0

                if idx >= batch.size:
                    batch = read_batch(mini_batch)
                    idx = 0
                    if batch.size == 0:
                        cursor.batch = batch
                        cursor.idx = 0
                        heappop(heap)
                        break

                tail = batch[idx:]
                n_le = int(searchsorted(tail, second, side="right"))
                if n_le:
                    chunk = tail[:n_le]
                    room = out_buf.size - out_n
                    if n_le <= room:
                        out_buf[out_n:out_n + n_le] = chunk
                        out_n += n_le
                    else:
                        if room:
                            out_buf[out_n:] = chunk[:room]
                            write_array(out_buf)
                        rest = chunk[room:]
                        full = (rest.size // out_buf.size) * out_buf.size
                        if full:
                            write_array(rest[:full])
                        tail_rest = rest[full:]
                        out_n = tail_rest.size
                        if out_n:
                            out_buf[:out_n] = tail_rest
                        else:
                            out_n = 0
                    idx += n_le

                if idx >= batch.size:
                    continue

                nxt = batch[idx]
                idx += 1
                cursor.batch = batch
                cursor.idx = idx
                item[0] = nxt
                heapreplace(heap, item)
                break

        if out_n:
            write_array(out_buf[:out_n])
    finally:
        writer.close()
        for reader in readers:
            reader.close()

    return _run_group_total_items(run_group)


def merge_group_argmin(run_group, out_path, read_block_items, write_block_items):
    '''
    PARAMETER DESCRIPTION:
    run_group : List of sorted run files to merge.
    out_path : Destination file for merged output.
    read_block_items : Per-run buffered read size.
    write_block_items : Buffered write size.

    NOTES:
    Uses argmin over current heads for small K merge groups.
    '''
    readers = [BinaryFloatReader(path, read_block_items) for path, _ in run_group]
    writer = BufferedFloatWriter(out_path, write_block_items)
    k = len(readers)
    heads = np.empty(k, dtype=np.float64)
    next_fns = [None] * k
    drain_fns = [None] * k
    chunk_fns = [None] * k
    alive = 0
    out_buf = np.empty(int(write_block_items), dtype=np.float64)
    out_n = 0
    argmin = np.argmin
    write_array = writer.write_array
    try:
        for reader in readers:
            next_fn = reader.next
            v = next_fn()
            if v is not None:
                heads[alive] = v
                next_fns[alive] = next_fn
                drain_fns[alive] = reader.drain_tail
                chunk_fns[alive] = reader.read_chunk
                alive += 1

        while alive:
            if alive == 1:
                if out_n:
                    write_array(out_buf[:out_n])
                    out_n = 0
                writer.write_direct(np.array((heads[0],), dtype=np.float64))

                tail = drain_fns[0]()
                if tail.size:
                    writer.write_direct(tail)
                while True:
                    chunk = chunk_fns[0]()
                    if chunk.size == 0:
                        break
                    writer.write_direct(chunk)
                break

            i = int(argmin(heads[:alive]))
            out_buf[out_n] = heads[i]
            out_n += 1
            if out_n == out_buf.size:
                write_array(out_buf)
                out_n = 0

            nxt = next_fns[i]()
            if nxt is None:
                last = alive - 1
                if i != last:
                    heads[i] = heads[last]
                    next_fns[i] = next_fns[last]
                    drain_fns[i] = drain_fns[last]
                    chunk_fns[i] = chunk_fns[last]
                alive -= 1
            else:
                heads[i] = nxt

        if out_n:
            write_array(out_buf[:out_n])
    finally:
        writer.close()
        for reader in readers:
            reader.close()

    return _run_group_total_items(run_group)


def merge_group(run_group, out_path, read_block_items, write_block_items):
    '''
    PARAMETER DESCRIPTION:
    run_group : List of tuples `(run_path, item_count)` to merge.
    out_path : Destination merged run path.
    read_block_items : Per-run read block size (float64 items).
    write_block_items : Write buffer size (float64 items).

    NOTES:
    Heap merge vs argmin merge is selected internally by `use_heap_for_k`.
    '''
    if use_heap_for_k(len(run_group)):
        return merge_group_heap(run_group, out_path, read_block_items, write_block_items)
    return merge_group_argmin(run_group, out_path, read_block_items, write_block_items)


def _merge_group_worker(args):
    '''
    PARAMETER DESCRIPTION:
    args : Tuple `(run_group, out_path, read_block_items, write_block_items)`.

    RETURNS:
    Tuple `(out_path, total_items, run_group)` for merge-pass orchestration.
    '''
    run_group, out_path, read_block_items, write_block_items = args
    total_items = merge_group(
        run_group,
        out_path,
        read_block_items,
        write_block_items,
    )
    return out_path, total_items, run_group


def external_sort(
    input_path: str,
    output_path: str,
    memory_items=1_000_000,
    fan_in=None,
    run_generation="internal",
    parallel_workers=None,
    chunk_sort_backend: str = "thread",
):
    '''
    RUNNING INSTRUCTIONS:
    Call this function from your own script after generating/providing `input_path`.

    PARAMETER DESCRIPTION:
    input_path : Source binary file of float64 values.
    output_path : Sorted binary output file path.
    memory_items : In-memory capacity as number of float64 items.
    fan_in : Number of runs merged at once, or None for auto tuning.
    run_generation : `internal` or `replacement` for initial run creation.
    parallel_workers : Worker count for parallel run-generation and merge groups.
    chunk_sort_backend : `thread` or `process` backend for chunk sort tasks.

    EXAMPLE:
    external_sort(
        "input.bin",
        "output.bin",
        memory_items=500000,
        fan_in=None,
        run_generation="internal",
        parallel_workers=4,
        chunk_sort_backend="process",
    )
    '''
    input_path = _resolve_path(input_path)
    output_path = _resolve_path(output_path)
    total_start = time.perf_counter()
    soft_nofile = get_soft_nofile()
    fan_in, read_block_items, write_block_items = tune_io(memory_items, fan_in, soft_nofile)

    run_start = time.perf_counter()
    merge_passes = 0
    io_read_bytes = 0
    io_write_bytes = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        final_tmp_output = os.path.join(tmpdir, "final_output.bin")

        if run_generation == "replacement":
            runs_paths = replacement_selection_runs(
                input_path=input_path,
                tmpdir=tmpdir,
                memory_items=memory_items,
                read_block_items=read_block_items,
                write_block_items=write_block_items,
            )
            runs = []
            for p in runs_paths:
                n_items = os.path.getsize(p) // F64_BYTES
                runs.append((p, n_items))
        elif run_generation == "internal":
            runs = create_sorted_runs_parallel(
                input_path,
                tmpdir,
                run_items=memory_items,
                parallel_workers=parallel_workers,
                chunk_sort_backend=chunk_sort_backend,
            )
        else:
            raise ValueError("run_generation must be 'internal' or 'replacement'")
        run_generation_s = time.perf_counter() - run_start

        input_bytes = os.path.getsize(input_path) if os.path.exists(input_path) else 0
        io_read_bytes += input_bytes

        runs_bytes = 0
        for _, n_items in runs:
            runs_bytes += n_items * F64_BYTES
        io_write_bytes += runs_bytes

        if not runs:
            open(final_tmp_output, "wb").close()
        elif len(runs) == 1:
            src_path, n_items = runs[0]
            copy_binary_file(src_path, final_tmp_output)
            one_run_bytes = n_items * F64_BYTES
            io_read_bytes += one_run_bytes
            io_write_bytes += one_run_bytes
        else:
            cur = runs
            while len(cur) > 1:
                nxt = []
                one_group = len(cur) <= fan_in
                pass_tasks = []
                for gid, start in enumerate(range(0, len(cur), fan_in)):
                    group = cur[start:start + fan_in]
                    out = final_tmp_output if (one_group and gid == 0) else os.path.join(
                        tmpdir, f"merge_p{merge_passes:02d}_{gid:06d}.bin"
                    )
                    pass_tasks.append((group, out, read_block_items, write_block_items))

                workers = _choose_parallel_workers(
                    task_count=len(pass_tasks),
                    requested_workers=parallel_workers,
                    est_worker_bytes=int(memory_items) * F64_BYTES,
                )

                if workers == 1:
                    merged_results = [_merge_group_worker(task) for task in pass_tasks]
                else:
                    with ThreadPoolExecutor(max_workers=workers) as ex:
                        merged_results = list(ex.map(_merge_group_worker, pass_tasks))
                    merged_results.sort(key=lambda x: x[0])

                for out, total_items, group in merged_results:
                    nxt.append((out, total_items))

                    merged_bytes = total_items * F64_BYTES
                    io_read_bytes += merged_bytes
                    io_write_bytes += merged_bytes

                    for p, _ in group:
                        if os.path.dirname(p) == tmpdir and os.path.exists(p):
                            os.remove(p)
                cur = nxt
                merge_passes += 1

        os.replace(final_tmp_output, output_path)

    total_s = time.perf_counter() - total_start
    total_io_bytes = io_read_bytes + io_write_bytes
    throughput_mb_s = (total_io_bytes / total_s / 1_000_000.0) if total_s > 0 else 0.0
    return {
        "run_generation_s": run_generation_s,
        "run_generation": run_generation,
        "initial_runs": len(runs),
        "merge_passes": merge_passes,
        "total_io_bytes": total_io_bytes,
        "throughput_mb_s": throughput_mb_s,
    }


if __name__ == "__main__":
    memory_profile = suggest_memory_items(fraction_of_avail=0.2, safety_factor=0.3)
    memory_items = max(4096, int(memory_profile["run_items"]))
    print(f"Suggested memory (fraction=0.2, safety=0.3): {memory_profile}")
    print(f"Using memory_items={memory_items}")

    generate_input_file("input.bin", n=5_000_000, seed=42)
    stats = external_sort(
        "input.bin",
        "output.bin",
        memory_items=memory_items,
        fan_in=None,
        run_generation="internal",
    )
    print("Done: input.bin -> output.bin")
    print(f"Run generation: {stats['run_generation_s']:.1f}s")
    print(f"Run generation mode: {stats['run_generation']}")
    print(f"Initial runs: {stats['initial_runs']}")
    print(f"Merge passes: {stats['merge_passes']}")
    print(f"Total IO: {stats['total_io_bytes']} bytes")
    print(f"Throughput: {stats['throughput_mb_s']:.0f}MB/s")

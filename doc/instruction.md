# Instruction Guide

## Overview
This document explains how to use:

1. `index.html` (interactive visualizer)
2. `external_merge_sort.py` (Python external merge sort runtime)

## `index.html` UI Guide

### Main Purpose
`index.html` visualizes external merge sort phases:

1. Run generation (`internal` chunk sort or `replacement` selection)
2. K-way merge passes
3. Final output build

### Control Buttons

1. `Step`: execute one state transition.
2. `Play/Pause`: auto-step based on speed slider.
3. `Reset`: reinitialize with current options.

### Numeric Inputs

1. `Seed`: random seed when Data Source is `Random Generated`.
2. `Input Size`: number of generated values (range enforced in UI).
3. `Run Size` / `Heap Size`: memory window for run generation.
   `Run Size` is shown for `internal`; `Heap Size` for `replacement`.
4. `Fan-In`: number of runs merged per merge group.

### Select Options

1. `Merge Strategy`
   1. `Auto (Python-like)`: auto choose `linear` vs `heap` by active group size.
   2. `Linear Scan`: compare all run heads each step.
   3. `Min-Heap`: maintain heap of run heads.

2. `Drain Mode`
   1. `Stepwise`: emit one value per step/drain state.
   2. `All In One Step`: drains contiguous values in one transition when possible.

3. `Run Generation`
   1. `Internal Chunk Sort`: split input into fixed-size chunks and sort each chunk.
   2. `Replacement Selection`: uses active heap + frozen buffer to create longer runs.

4. `Data Source`
   1. `Random Generated`: uses `Seed` + `Input Size`.
   2. `From File`: loads values from file input.

### File Input Rules (`From File` mode)

1. Accepted formats: `.txt`, `.csv`, `.json`, `.bin`
2. Size limit: 2 MB
3. Max values: 5000
4. Values must be finite numbers
5. `.bin` must be little-endian `float64` with byte size multiple of 8

### Display Panels

1. `Input Tape`: full source sequence, faded as consumed.
2. `Replacement Selection State`: active heap, frozen buffer, current run preview.
3. `Current Pass Runs`: runs currently being merged.
4. `Merge Workspace`: candidate/heap state and active group output.
5. `Next Pass Runs`: merged runs produced for next pass.
6. `Phase/Pass/Groups Done/Runs`: current progress counters.
7. `Final Output`: sorted output values.
8. `Comparisons` + `Delay`: live operation counter and playback delay.

## `external_merge_sort.py` Guide

### Main Function

Use `external_sort(...)`:

```python
stats = external_sort(
    input_path="input.bin",
    output_path="output.bin",
    memory_items=500_000,
    fan_in=None,
    run_generation="internal",
    parallel_workers=None,
    chunk_sort_backend="thread",
)
```

### Parameters

1. `input_path`: source binary file (`float64` values).
   Absolute paths are used as-is; relative paths are resolved from the project root.
2. `output_path`: destination sorted binary file.
   Absolute paths are used as-is; relative paths are resolved from the project root.
3. `memory_items`: in-memory capacity in number of `float64` items.
4. `fan_in`: merge fan-in (`None` enables auto tuning).
5. `run_generation`: `internal` or `replacement`.
6. `parallel_workers`: worker count for run generation and merge groups.
7. `chunk_sort_backend`: `thread` or `process` (for internal chunk sorting).

### Runtime Modes

1. `internal` run generation:
   splits input into fixed chunks, sorts each chunk in parallel, writes runs.
2. `replacement` run generation:
   maintains active heap and frozen values to produce longer runs.


### Running

```bash
python external_merge_sort.py
```

Default `__main__` flow:

1. Compute suggested memory profile.
2. Generate `input.bin`.
3. Sort into `output.bin`.
4. Print run-generation and throughput stats.

# External Merge Sort (Python + Visualizer)

Live visualizer: https://nvvinh0.github.io/external_mergesort_visualizer/

## Description
This project has two components:

1. `external_merge_sort.py`: a Python external merge sort implementation for binary `float64` files.
2. `index.html`: an interactive visualizer for run generation and k-way merge behavior.

The sorter is built for datasets larger than RAM: it creates sorted runs on disk, then merges runs over one or more passes.

## Motivation
The repository combines a practical external-sort runtime with a teaching/inspection UI for CS523.Q21 - HK1/2026 - UIT. You can use the Python script for real sorting workloads and the visualizer to understand algorithm phases such as replacement selection, fan-in merge passes, and heap vs linear selection.

## Features

* Sorts large binary `float64` input files using external merge sort.
* Supports two run-generation modes:
  * `internal` chunk sort
  * `replacement` selection
* Supports k-way merge with auto strategy choice (heap vs argmin) based on merge-group size.
* Optional parallelism:
  * Run generation: thread or process backend (`chunk_sort_backend`)
  * Merge groups: thread pool
* Reports runtime stats (run generation time, merge passes, total IO bytes, throughput).
* Accepts absolute paths, and resolves relative paths from the project root.
* Interactive HTML visualizer with:
  * Step/Play controls
  * Input tape, run panels, merge workspace, and final output view
  * Configurable run generation mode, merge strategy, fan-in, run size/heap size, and speed
  * File or random data source modes

## Libraries

* Python 3
* `numpy`
* Optional: `psutil` (used when available for RAM estimation)
* Python standard library modules:
  * `heapq`
  * `os`
  * `tempfile`
  * `time`
  * `concurrent.futures`
* Local package modules under `external_mergesort_core/`

## Build Instructions

No compilation is required.

1. Create and activate a virtual environment (recommended).
2. Install dependency:

```bash
pip install numpy
```

Optional:

```bash
pip install psutil
```

3. Keep the project structure intact so imports from `external_mergesort_core` resolve correctly.

## Tests

There is no dedicated automated test suite included in this folder.

You can validate behavior by:

* Running `external_merge_sort.py` end-to-end and checking that it finishes successfully.
* Verifying output ordering from `output.bin`.
* Running the visualizer (`index.html`) and stepping through all phases to completion.

## Usage

### 1. Run the Python sorter

```bash
python external_merge_sort.py
```

By default, this will:

* Generate `input.bin`
* Sort into `output.bin`
* Print performance and merge statistics

### 2. Call `external_sort(...)` from your own script

```python
from external_merge_sort import external_sort

stats = external_sort(
    input_path="input.bin",
    output_path="output.bin",
    memory_items=500_000,
    fan_in=None,
    run_generation="internal",   # or "replacement"
    parallel_workers=4,
    chunk_sort_backend="thread", # or "process"
)
print(stats)
```

### 3. Open the visualizer

Use one of these:

* Hosted page: https://nvvinh0.github.io/external_mergesort_visualizer/
* Local file: open `index.html` in a modern browser.

Use the controls to:

* Generate random input or load from file
* Choose run-generation mode and merge strategy
* Step or autoplay the algorithm
* Observe pass/group progress and final output

## How it Works (External Merge Sort)

The sorter follows standard external merge sort stages:

1. Run Generation:
   * Read input in memory-sized chunks (`internal`) or use replacement selection (`replacement`).
   * Sort each chunk and write sorted runs to temporary files.
2. Merge Passes:
   * Group runs by `fan_in`.
   * Merge each group into new sorted runs using heap or argmin selection.
   * Repeat until one final run remains.
3. Finalization:
   * Move the final merged run to `output_path`.
   * Report timing and IO throughput statistics.

The visualizer mirrors these phases with UI state updates for phase, pass, groups completed, active runs, and output growth.

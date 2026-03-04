# External Merge Sort (Python + Visualizer)

Live visualizer: https://nvvinh0.github.io/external_mergesort_visualizer/

## Mô tả
Dự án này gồm hai thành phần:

1. `external_merge_sort.py`: một triển khai external merge sort bằng Python cho các file nhị phân `float64`.
2. `index.html`: một visualizer tương tác cho việc tạo run và hành vi merge k-way.

Bộ sắp xếp được xây dựng cho các tập dữ liệu lớn hơn RAM: nó tạo các run đã sắp xếp trên đĩa, sau đó merge các run qua một hoặc nhiều lượt.

## Động lực
Repository này kết hợp một runtime external-sort thực tế với giao diện giảng dạy/kiểm tra cho CS523.Q21 - HK1/2026 - UIT. Bạn có thể dùng script Python cho các workload sắp xếp thực tế và dùng visualizer để hiểu các pha của thuật toán như replacement selection, các lượt merge fan-in, và heap vs linear selection.

## Tính năng

* Sắp xếp các file input nhị phân `float64` lớn bằng external merge sort.
* Hỗ trợ hai chế độ tạo run:
  * `internal` chunk sort
  * `replacement` selection
* Hỗ trợ k-way merge với lựa chọn chiến lược tự động (heap vs argmin) dựa trên kích thước nhóm merge.
* Tùy chọn song song:
  * Tạo run: backend thread hoặc process (`chunk_sort_backend`)
  * Merge groups: thread pool
* Báo cáo thống kê thời gian chạy (thời gian tạo run, lượt merge, tổng byte I/O, throughput).
* Chấp nhận đường dẫn tuyệt đối, và giải quyết đường dẫn tương đối từ project root.
* Visualizer HTML tương tác với:
  * Điều khiển Step/Play
  * Input tape, run panels, merge workspace, và view output cuối cùng
  * Cấu hình chế độ tạo run, chiến lược merge, fan-in, kích thước run/heap, và tốc độ
  * Chế độ nguồn dữ liệu: từ file hoặc ngẫu nhiên

## Thư viện

* Python 3
* `numpy`
* Tùy chọn: `psutil` (dùng khi có để ước lượng RAM)
* Các module chuẩn của Python:
  * `heapq`
  * `os`
  * `tempfile`
  * `time`
  * `concurrent.futures`
* Các module cục bộ trong `external_mergesort_core/`

## Hướng dẫn build

Không cần biên dịch.

1. Tạo và kích hoạt virtual environment (khuyến nghị).
2. Cài dependency:

```bash
pip install numpy
```

Tùy chọn:

```bash
pip install psutil
```

3. Giữ nguyên cấu trúc project để các import từ `external_mergesort_core` được resolve chính xác.

## Kiểm thử

Không có bộ test tự động riêng trong thư mục này.

Bạn có thể xác thực bằng:

* Chạy `external_merge_sort.py` end-to-end và kiểm tra chương trình hoàn tất thành công.
* Kiểm tra thứ tự đầu ra trong `output.bin`.
* Chạy visualizer (`index.html`) và bước qua tất cả các pha đến khi hoàn tất.

## Cách sử dụng

### 1. Chạy Python sorter

```bash
python external_merge_sort.py
```

Mặc định, điều này sẽ:

* Tạo `input.bin`
* Sắp xếp thành `output.bin`
* In thống kê hiệu năng và merge

### 2. Gọi `external_sort(...)` từ script của bạn

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

### 3. Mở visualizer

Sử dụng một trong các cách sau:

* Trang host: https://nvvinh0.github.io/external_mergesort_visualizer/
* File cục bộ: mở `index.html` trong trình duyệt hiện đại.

Sử dụng các điều khiển để:

* Tạo input ngẫu nhiên hoặc load từ file
* Chọn chế độ tạo run và chiến lược merge
* Step hoặc autoplay thuật toán
* Quan sát tiến độ pass/group và output cuối

## Cách hoạt động (External Merge Sort)

Bộ sắp xếp theo các giai đoạn chuẩn của external merge sort:

1. Tạo Run:
   * Đọc input theo các chunk vừa với bộ nhớ (`internal`) hoặc dùng replacement selection (`replacement`).
   * Sắp xếp từng chunk và ghi các run đã sắp xếp vào file tạm.
2. Các lượt Merge:
   * Nhóm các run theo `fan_in`.
   * Merge mỗi nhóm thành các run mới sử dụng heap hoặc argmin selection.
   * Lặp lại cho đến khi chỉ còn một run cuối cùng.
3. Hoàn tất:
   * Di chuyển run hợp nhất cuối cùng tới `output_path`.
   * Báo cáo thời gian và thống kê throughput I/O.

Visualizer mô phỏng các pha này với cập nhật trạng thái UI cho phase, pass, số group đã hoàn thành, các run đang hoạt động, và sự tăng trưởng của output.

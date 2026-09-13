# MapReduce trên Hadoop Standalone – Bài 7 & Bài 14

**Môi trường:** Hadoop 3.3.6, OpenJDK 11.0.32 (Temurin), WSL2 Ubuntu 24.04 trên Windows 11.
**Chế độ chạy:** Standalone (local mode) – không dùng HDFS, không dùng YARN; toàn bộ Map và
Reduce chạy trong một JVM bằng `LocalJobRunner`, đọc/ghi thẳng trên hệ thống file cục bộ.

## Cấu trúc thư mục

```
mapreduce/
├── conf-standalone/              # Cấu hình Hadoop riêng cho chế độ Standalone
│   ├── core-site.xml             #   fs.defaultFS = file:///
│   ├── mapred-site.xml           #   mapreduce.framework.name = local
│   └── log4j.properties
├── bai07-dem-so-duy-nhat/
│   ├── src/UniqueNumberCount.java
│   ├── data/sample/numbers.txt   # Dữ liệu nhỏ, kiểm tra được bằng tay
│   ├── gen_data.py               # Sinh data/large (1.000.000 số)
│   ├── verify.py                 # Kiểm tra độc lập bằng Python
│   └── run.sh
└── bai14-dem-so-nguyen-to/
    ├── src/PrimeCount.java
    ├── data/sample/numbers.txt
    ├── gen_data.py
    ├── verify.py
    └── run.sh
```

Cụm pseudo-distributed cài trước đó **không bị ảnh hưởng**: chương trình chỉ dùng
`hadoop --config conf-standalone`, không sửa gì trong `$HADOOP_HOME/etc/hadoop`, và không cần
khởi động daemon nào.

## Cách chạy

Mở WSL (Ubuntu) rồi:

```bash
cd /mnt/c/Users/Thomm/Coding/Big_Data/mapreduce

bash bai07-dem-so-duy-nhat/run.sh sample    # hoặc: large
bash bai14-dem-so-nguyen-to/run.sh sample   # hoặc: large
```

Mỗi `run.sh` thực hiện 4 bước: biên dịch + đóng gói JAR → (sinh dữ liệu lớn nếu chưa có) →
chạy job bằng `hadoop --config ../conf-standalone jar ...` → kiểm tra kết quả bằng Python.
Kết quả nằm trong `output/<sample|large>/`.

---

## Bài 7 – Đếm số duy nhất

**Đề bài:** đọc vào một file gồm các số nguyên, in ra số lượng các số nguyên chỉ xuất hiện
đúng 1 lần.

### Thiết kế: chuỗi 2 job MapReduce

| Giai đoạn | Job 1 – Lọc số duy nhất | Job 2 – Đếm |
|---|---|---|
| Map | mỗi số `n` → `(n, 1)` | mỗi dòng output Job 1 → `("SO_DUY_NHAT", 1)` |
| Combine | cộng dồn `(n, k)` ngay tại map | cộng dồn tại map |
| Shuffle | gom mọi giá trị cùng `n` về 1 reducer | gom về 1 key duy nhất |
| Reduce | tổng số lần xuất hiện của `n`; **chỉ ghi `n` nếu tổng = 1** | `("SO_DUY_NHAT", tổng)` |

- Job 1 ghi ra `output/.../1-unique-numbers/` = danh sách các số chỉ xuất hiện 1 lần.
- Job 2 ghi ra `output/.../2-unique-count/` = đáp số.
- Các số được phân tách bởi khoảng trắng, `,` hoặc `;`; hỗ trợ số âm; `007` và `7` là cùng
  một số. Token không phải số nguyên (ví dụ `abc`) được bỏ qua và đếm vào counter
  `INVALID_TOKENS`.

### Kết quả

**Dữ liệu mẫu** (`data/sample/numbers.txt`):

```
5 3 8 3 12 7 5
-4 9 12 12 0 21
8, 15; 007 -4 100
42 abc 9 1
```

| Chỉ số | Giá trị |
|---|---|
| Số nguyên hợp lệ | 21 (`abc` bị bỏ qua) |
| Số giá trị phân biệt | 13 |
| Các số xuất hiện đúng 1 lần | `0 1 15 21 42 100` |
| **Đáp số** | **6** |

**Dữ liệu lớn** (1.000.000 số ngẫu nhiên trong [1, 1.000.000], seed 7):

| Chỉ số | Giá trị |
|---|---|
| Số giá trị phân biệt | 631.532 |
| **Đáp số** | **367.509** (lý thuyết ≈ N/e ≈ 367.879) |
| Thời gian chạy (2 job) | ≈ 6,2 s |
| Kiểm tra bằng Python | KHỚP cả số lượng lẫn danh sách |

Combiner giảm dữ liệu shuffle của Job 1 từ 1.000.000 xuống 631.532 bản ghi, và của Job 2
từ 367.510 xuống **1** bản ghi.

---

## Bài 14 – Kiểm tra số nguyên tố

**Đề bài:** đếm số lượng các số nguyên tố có trong file dữ liệu cho trước.

### Thiết kế: 1 job MapReduce

| Giai đoạn | Xử lý |
|---|---|
| Map | với mỗi số `n`: phát `("TONG_SO_LUONG_SO_NGUYEN", 1)`; nếu `n` nguyên tố thì phát thêm `("SO_LUONG_SO_NGUYEN_TO", 1)` |
| Combine | cộng dồn tại map → mỗi map task chỉ còn **2 bản ghi** gửi đi |
| Reduce | 1 reducer, cộng tổng cho từng key |

- **Kiểm tra nguyên tố:** Miller–Rabin tất định với 12 cơ sở `2, 3, 5, …, 37` – cho kết quả
  **chính xác** (không phải xác suất) với mọi số kiểu `long` (tới 9.223.372.036.854.775.807).
  Thuật toán chạy O(log n) cho mỗi số, trong khi thử chia tới √n sẽ cần tới ~3 tỷ phép chia
  với số 64-bit.
- Mỗi lần xuất hiện được tính riêng: file có `7919 7919` thì tính là 2 số nguyên tố.
- Số âm, 0 và 1 không phải số nguyên tố.

### Kết quả

**Dữ liệu mẫu** (`data/sample/numbers.txt`) – cố ý chứa các trường hợp dễ sai:

```
1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
0 -7 -2 1 97 100 561 7919 7919
2147483647 1000000007 999999999989 1000000000000 9223372036854775783 9223372036854775807
3215031751, 25326001 x12
```

| Nhóm | Số nguyên tố |
|---|---|
| 1..20 | 2, 3, 5, 7, 11, 13, 17, 19 → 8 |
| Dòng 2 | 97, 7919, 7919 → 3 (561 là số Carmichael, không nguyên tố) |
| Dòng 3 | 2³¹−1, 10⁹+7, 999999999989, 2⁶³−25 → 4 (2⁶³−1 = 7²·73·127·… không nguyên tố) |
| Dòng 4 | 0 (3215031751 và 25326001 là hợp số “giả nguyên tố mạnh” – đánh lừa Miller–Rabin nếu dùng ít cơ sở) |
| **Đáp số** | **15 số nguyên tố / 37 số nguyên** (`x12` bị bỏ qua) |

**Dữ liệu lớn** (1.000.000 số ngẫu nhiên trong [0, 10.000.000], seed 14):

| Chỉ số | Giá trị |
|---|---|
| Tổng số nguyên | 1.000.000 |
| **Số lượng số nguyên tố** | **66.561** (≈ 6,66%) |
| Thời gian chạy | ≈ 3,9 s |
| Kiểm tra (sàng Eratosthenes + lệnh `factor`) | KHỚP |

Combiner giảm 1.066.563 bản ghi đầu ra của map xuống còn **2** bản ghi phải shuffle.

---

## Kiểm chứng đang chạy ở chế độ Standalone

- Job ID có dạng `job_local…` → job được thực thi bởi `LocalJobRunner`, không phải YARN
  (trên YARN, ID sẽ có dạng `job_<timestamp>_<n>` và xuất hiện trên http://localhost:8088).
- Counter `FILE: Number of bytes read/written` thay vì `HDFS: …` → đọc/ghi file cục bộ.
- Không cần daemon nào chạy (`jps` chỉ hiển thị `Jps`).

#!/usr/bin/env python3
"""Bài 14 — Đếm số lượng số nguyên tố trong file (Apache Spark, RDD API).

Ý tưởng: mỗi số được kiểm tra độc lập bằng hàm is_prime, nên chỉ cần một phép
filter rồi count. Không cần gom nhóm theo khóa như Bài 7.

Vì các phần tử độc lập nhau, Spark có thể chia RDD thành nhiều partition và
kiểm tra song song trên các executor.

"""
import re
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

# Đường dẫn BÊN TRONG container (./data trên máy = /opt/spark/data). Phải là đường dẫn
# tuyệt đối: driver và executor chạy ở hai container khác nhau, thư mục làm việc khác nhau.
INPUT = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/bai14/sample/numbers.txt"
SHOW = 20  # số phần tử tối đa in ra màn hình

# Tách token theo khoảng trắng, "," hoặc ";" — giống bản MapReduce.
SEP = re.compile(r"[\s,;]+")

spark = SparkSession.builder.appName("BTL-Bai14-PrimeCount").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")


def is_prime(n):
    """Kiểm tra số nguyên tố bằng phép chia thử, chỉ thử đến căn bậc hai của n.

    Cơ sở: nếu n = a*b thì không thể đồng thời a > √n và b > √n, nên nếu n có
    ước thì chắc chắn tồn tại một ước <= √n.

    Chi phí O(√n): với số 64-bit lớn (ví dụ 2^63 - 25 trong dữ liệu mẫu) cần khoảng
    1,5 tỷ vòng lặp, mất vài phút trên một core.
    """
    if n < 2:
        return False          # 0, 1 và số âm không phải số nguyên tố
    if n < 4:
        return True           # 2 và 3 là số nguyên tố
    if n % 2 == 0:
        return False          # loại toàn bộ số chẵn còn lại
    i = 3
    while i * i <= n:         # tương đương i <= sqrt(n), tránh sai số số thực
        if n % i == 0:
            return False
        i += 2                # số chẵn đã bị loại nên chỉ thử ước lẻ
    return True


def to_int(tok):
    try:
        return int(tok)
    except ValueError:
        return None


# 1. Đọc file thành RDD các dòng.
lines = sc.textFile(INPUT)

# 2. flatMap + map: tách token và chuyển sang số nguyên.
numbers = (
    lines.flatMap(lambda line: SEP.split(line))
    .map(to_int)
    .filter(lambda n: n is not None)
    .cache()
)

# 3. filter: giữ lại các số thỏa điều kiện nguyên tố.
#    cache(): is_prime là bước tốn kém nhất; giữ kết quả để count() và phần in
#    danh sách bên dưới không phải kiểm tra lại từng số lần thứ hai.
primes = numbers.filter(is_prime).cache()

# 4. count(): ACTION kích hoạt pipeline và trả về kết quả cuối.
so_luong = primes.count()

if so_luong <= SHOW:
    danh_sach = sorted(primes.collect())
else:
    danh_sach = f"{primes.takeOrdered(SHOW)} ... ({SHOW}/{so_luong:,} so nho nhat)"

print("===== KET QUA BAI 14 — DEM SO NGUYEN TO =====")
print("File input                :", INPUT)
print("Tong so phan tu doc duoc  :", numbers.count())
print("Cac so nguyen to tim duoc :", danh_sach)
print("so_luong_so_nguyen_to     :", so_luong)

run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
output_dir = Path("/opt/spark/data/output") / f"bai14_{run_id}"
output_dir.mkdir(parents=True, exist_ok=False)

primes.map(str).saveAsTextFile(
    (output_dir / "prime_numbers").as_uri()
)

print("Da luu ket qua tai:", output_dir)

spark.stop()

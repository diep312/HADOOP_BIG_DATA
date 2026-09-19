#!/usr/bin/env python3
"""Bài 7 — Đếm các số nguyên chỉ xuất hiện đúng một lần (Apache Spark, RDD API).

Ý tưởng: đưa mỗi số về cặp (n, 1), dùng reduceByKey để cộng theo khóa ra tần
suất của từng số, lọc các số có tần suất bằng 1 rồi đếm.

Khác với bản MapReduce phải tách thành hai job nối tiếp (vì reducer không nhìn
được toàn cục giữa các khóa), Spark giữ kết quả trung gian trên bộ nhớ nên cả
hai bước nằm gọn trong MỘT chương trình, không phải ghi ra đĩa ở giữa.

Cách chạy (từ thư mục spark-docker):
    docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/apps/bai07_dem_so_duy_nhat.py [input]
"""
import re
import sys
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession


INPUT = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/bai07/sample/numbers.txt"
SHOW = 20  # số phần tử tối đa in ra màn hình


SEP = re.compile(r"[\s,;]+")

spark = SparkSession.builder.appName("BTL-Bai7-UniqueIntegerCount").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")


def to_int(tok):
    """Trả về số nguyên, hoặc None nếu token không phải số (dữ liệu bẩn)."""
    try:
        return int(tok)
    except ValueError:
        return None


def preview(rdd, total):
    """In toàn bộ nếu ít phần tử; nếu nhiều thì chỉ lấy SHOW phần tử nhỏ nhất."""
    if total <= SHOW:
        return sorted(rdd.collect())
    return f"{rdd.takeOrdered(SHOW)} ... ({SHOW}/{total:,} gia tri nho nhat)"


# 1. Đọc file thành RDD, mỗi phần tử là một dòng văn bản.
lines = sc.textFile(INPUT)

# 2. flatMap: tách mỗi dòng thành nhiều token rồi làm phẳng thành một RDD số.
#    cache(): RDD này được dùng lại ở nhiều action bên dưới, giữ trên bộ nhớ để
#    không phải đọc và tách file lại từ đầu mỗi lần.
numbers = (
    lines.flatMap(lambda line: SEP.split(line))
    .map(to_int)
    .filter(lambda n: n is not None)
    .cache()
)

# 3. map + reduceByKey: đếm tần suất xuất hiện của từng số.
#    reduceByKey gom các phần tử cùng khóa và cộng giá trị: (5,1)+(5,1) -> (5,2)
freq = numbers.map(lambda n: (n, 1)).reduceByKey(lambda a, b: a + b).cache()

# 4. filter: chỉ giữ các số có tần suất đúng bằng 1.
unique = freq.filter(lambda kv: kv[1] == 1)

# 5. count(): đây là ACTION, chính nó mới kích hoạt toàn bộ pipeline ở trên chạy.
so_luong = unique.count()

print("===== KET QUA BAI 7 — DEM SO XUAT HIEN DUNG 1 LAN =====")
print("File input                   :", INPUT)
print("Tong so phan tu doc duoc     :", numbers.count())
print("So gia tri phan biet         :", freq.count())
print("Cac gia tri xuat hien 1 lan  :", preview(unique.map(lambda kv: kv[0]), so_luong))
print("so_luong_xuat_hien_1_lan     :", so_luong)

run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
output_dir = Path("/opt/spark/data/output") / f"bai07_{run_id}"
output_dir.mkdir(parents=True, exist_ok=False)

unique.map(lambda kv: str(kv[0])).saveAsTextFile(
    (output_dir / "unique_numbers").as_uri()
)

print("Da luu ket qua tai:", output_dir)

spark.stop()

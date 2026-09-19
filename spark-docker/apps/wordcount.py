"""Đếm từ bằng PySpark trên cụm Spark Docker.

Cách chạy (từ thư mục spark-docker):
    docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/apps/wordcount.py [input] [output]

Mặc định đọc data/input.txt và ghi CSV vào data/output/wordcount/.
Đường dẫn là đường dẫn BÊN TRONG container: ./data trên máy = /opt/spark/data.
"""
import sys

from pyspark.sql import SparkSession

INPUT = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/input.txt"
OUTPUT = sys.argv[2] if len(sys.argv) > 2 else "/opt/spark/data/output/wordcount"

spark = SparkSession.builder.appName("wordcount-docker").getOrCreate()

lines = spark.sparkContext.textFile(INPUT)
counts = (lines.flatMap(lambda l: l.lower().split())
               .map(lambda w: (w.strip(".,;:!?\"'()"), 1))
               .filter(lambda kv: kv[0])
               .reduceByKey(lambda a, b: a + b))

df = spark.createDataFrame(counts, ["word", "count"]).orderBy("count", ascending=False)
print(f">>> Tổng số từ khác nhau: {df.count()}")
df.show(10)

df.coalesce(1).write.mode("overwrite").csv(OUTPUT, header=True)
print(f">>> Đã ghi kết quả vào {OUTPUT}")
spark.stop()

"""Sinh bộ dữ liệu lớn cho Bài 7: 1.000.000 số nguyên ngẫu nhiên trong [1, 1.000.000].

Với N số rút đều từ N giá trị, kỳ vọng số lượng giá trị xuất hiện đúng 1 lần ≈ N/e ≈ 367.879.
Dùng seed cố định để kết quả tái lập được.
"""
import os
import random
import sys

COUNT = 1_000_000
LOW, HIGH = 1, 1_000_000
PER_LINE = 10

out_dir = sys.argv[1]
os.makedirs(out_dir, exist_ok=True)
rng = random.Random(7)
with open(os.path.join(out_dir, "numbers.txt"), "w", newline="\n") as f:
    for start in range(0, COUNT, PER_LINE):
        f.write(" ".join(str(rng.randint(LOW, HIGH)) for _ in range(PER_LINE)) + "\n")
print(f"Da sinh {COUNT:,} so nguyen vao {out_dir}/numbers.txt")

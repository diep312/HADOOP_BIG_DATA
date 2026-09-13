"""Sinh bộ dữ liệu lớn cho Bài 14: 1.000.000 số nguyên ngẫu nhiên trong [0, 10.000.000].

Mật độ số nguyên tố quanh x xấp xỉ 1/ln(x), nên kỳ vọng khoảng 6-7% số trong file là
số nguyên tố. Dùng seed cố định để kết quả tái lập được.
"""
import os
import random
import sys

COUNT = 1_000_000
LOW, HIGH = 0, 10_000_000
PER_LINE = 10

out_dir = sys.argv[1]
os.makedirs(out_dir, exist_ok=True)
rng = random.Random(14)
with open(os.path.join(out_dir, "numbers.txt"), "w", newline="\n") as f:
    for start in range(0, COUNT, PER_LINE):
        f.write(" ".join(str(rng.randint(LOW, HIGH)) for _ in range(PER_LINE)) + "\n")
print(f"Da sinh {COUNT:,} so nguyen vao {out_dir}/numbers.txt")

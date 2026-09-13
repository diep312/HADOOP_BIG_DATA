"""Kiểm tra độc lập kết quả Bài 7 bằng Python thuần (không qua Hadoop).

Cách dùng: python3 verify.py <thu_muc_input> <thu_muc_output>
"""
import glob
import os
import re
import sys
from collections import Counter

input_dir, output_dir = sys.argv[1], sys.argv[2]

freq = Counter()
for path in sorted(glob.glob(os.path.join(input_dir, "*"))):
    with open(path) as f:
        for token in re.split(r"[\s,;]+", f.read()):
            if re.fullmatch(r"[+-]?\d+", token):
                freq[int(token)] += 1
expected_unique = sorted(n for n, c in freq.items() if c == 1)

count_line = open(os.path.join(output_dir, "2-unique-count", "part-r-00000")).read().split()
hadoop_count = int(count_line[1])
hadoop_unique = []
for path in sorted(glob.glob(os.path.join(output_dir, "1-unique-numbers", "part-r-*"))):
    hadoop_unique += [int(x) for x in open(path).read().split()]

print(f"Python  : {len(expected_unique):,} so chi xuat hien dung 1 lan")
print(f"Hadoop  : {hadoop_count:,} so chi xuat hien dung 1 lan")
ok = hadoop_count == len(expected_unique) and sorted(hadoop_unique) == expected_unique
print("KET LUAN: KHOP (so luong va danh sach deu trung khop)" if ok else "KET LUAN: KHONG KHOP")
sys.exit(0 if ok else 1)

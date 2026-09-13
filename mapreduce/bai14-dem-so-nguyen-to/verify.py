"""Kiểm tra độc lập kết quả Bài 14, dùng thuật toán khác với chương trình Hadoop:
  - số <= 10^7: sàng Eratosthenes
  - số lớn hơn: lệnh `factor` của GNU coreutils (nguyên tố <=> chỉ có 1 thừa số là chính nó)

Cách dùng: python3 verify.py <thu_muc_input> <thu_muc_output>
"""
import glob
import os
import re
import subprocess
import sys

SIEVE_LIMIT = 10_000_000
LONG_MAX = 2**63 - 1

input_dir, output_dir = sys.argv[1], sys.argv[2]

numbers = []
for path in sorted(glob.glob(os.path.join(input_dir, "*"))):
    with open(path) as f:
        for token in re.split(r"[\s,;]+", f.read()):
            if re.fullmatch(r"[+-]?\d+", token) and -LONG_MAX - 1 <= int(token) <= LONG_MAX:
                numbers.append(int(token))

sieve = bytearray([1]) * (SIEVE_LIMIT + 1)
sieve[0] = sieve[1] = 0
for i in range(2, int(SIEVE_LIMIT**0.5) + 1):
    if sieve[i]:
        sieve[i * i :: i] = bytearray(len(range(i * i, SIEVE_LIMIT + 1, i)))

big = sorted({n for n in numbers if n > SIEVE_LIMIT})
big_primes = set()
if big:
    out = subprocess.run(["factor"], input="\n".join(map(str, big)), capture_output=True, text=True, check=True)
    for line in out.stdout.splitlines():
        n, factors = line.split(":")
        if factors.split() == [n]:
            big_primes.add(int(n))

expected = sum(1 for n in numbers if (sieve[n] if 0 <= n <= SIEVE_LIMIT else n in big_primes))

result = dict(line.split("\t") for line in open(os.path.join(output_dir, "part-r-00000")).read().splitlines())
hadoop_primes = int(result["SO_LUONG_SO_NGUYEN_TO"])
hadoop_total = int(result["TONG_SO_LUONG_SO_NGUYEN"])

print(f"Python  : {len(numbers):,} so nguyen, {expected:,} so nguyen to")
print(f"Hadoop  : {hadoop_total:,} so nguyen, {hadoop_primes:,} so nguyen to")
ok = hadoop_primes == expected and hadoop_total == len(numbers)
print("KET LUAN: KHOP" if ok else "KET LUAN: KHONG KHOP")
sys.exit(0 if ok else 1)

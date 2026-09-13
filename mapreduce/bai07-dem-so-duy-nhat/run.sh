#!/usr/bin/env bash
# Bài 7 – Đếm số duy nhất: biên dịch, chạy trên Hadoop Standalone và kiểm tra kết quả.
#
# Cách dùng (trong WSL):  bash run.sh [sample|large]      (mặc định: sample)
set -euo pipefail
source "$HOME/opt/bigdata-env.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF="$HERE/../conf-standalone"
DATASET="${1:-sample}"
INPUT="$HERE/data/$DATASET"
OUTPUT="$HERE/output/$DATASET"

echo ">>> [1/4] Bien dich va dong goi JAR"
rm -rf "$HERE/build" && mkdir -p "$HERE/build/classes"
javac -encoding UTF-8 -cp "$(hadoop classpath)" -d "$HERE/build/classes" "$HERE"/src/*.java
jar cf "$HERE/build/bai07.jar" -C "$HERE/build/classes" .

if [[ "$DATASET" == "large" && ! -d "$INPUT" ]]; then
  echo ">>> [2/4] Sinh du lieu lon"
  python3 "$HERE/gen_data.py" "$INPUT"
else
  echo ">>> [2/4] Dung du lieu co san: $INPUT"
fi

echo ">>> [3/4] Chay MapReduce tren Hadoop Standalone (conf: $CONF)"
rm -rf "$OUTPUT"   # Hadoop yeu cau thu muc output chua ton tai
time hadoop --config "$CONF" jar "$HERE/build/bai07.jar" bigdata.bai07.UniqueNumberCount "$INPUT" "$OUTPUT"

echo ">>> [4/4] Kiem tra doc lap bang Python"
python3 "$HERE/verify.py" "$INPUT" "$OUTPUT"

#!/usr/bin/env bash
# Bai 7 / Bai 14 tren cum Spark Docker: chuan bi du lieu, khoi dong cum (neu chua chay), chay job.
#
# Cach dung (trong WSL):  bash run.sh <bai07|bai14> [sample|large]      (mac dinh: sample)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BAI="${1:?Cach dung: bash run.sh <bai07|bai14> [sample|large]}"
DATASET="${2:-sample}"

case "$BAI" in
  bai07) APP=bai07_dem_so_duy_nhat.py;  MR="$HERE/../mapreduce/bai07-dem-so-duy-nhat" ;;
  bai14) APP=bai14_dem_so_nguyen_to.py; MR="$HERE/../mapreduce/bai14-dem-so-nguyen-to" ;;
  *) echo "Bai khong hop le: $BAI (chon bai07 hoac bai14)" >&2; exit 1 ;;
esac
case "$DATASET" in
  sample|large) ;;
  *) echo "Bo du lieu khong hop le: $DATASET (chon sample hoac large)" >&2; exit 1 ;;
esac
DATA="$HERE/data/$BAI/$DATASET"
cd "$HERE"

echo ">>> [1/3] Du lieu: $DATA/numbers.txt"
if [[ ! -f "$DATA/numbers.txt" ]]; then
  if [[ "$DATASET" == "large" ]]; then
    python3 "$MR/gen_data.py" "$DATA"          # seed co dinh => giong het du lieu ban MapReduce
  else
    mkdir -p "$DATA" && cp "$MR/data/sample/numbers.txt" "$DATA/"
  fi
fi

echo ">>> [2/3] Cum Spark"
if ! docker compose ps --status running --services | grep -qx spark-worker; then
  docker compose up -d
fi
# Doi worker dang ky voi master (toi da 30 s)
for _ in $(seq 30); do
  docker exec spark-master python3 -c "import json, sys, urllib.request; \
sys.exit(json.load(urllib.request.urlopen('http://spark-master:8080/json'))['aliveworkers'] < 1)" 2>/dev/null && break
  sleep 1
done

echo ">>> [3/3] Chay $APP tren du lieu $DATASET"
# An log INFO/WARN cua Spark, giu lai ket qua va loi
time docker exec spark-master /opt/spark/bin/spark-submit \
  "/opt/spark/apps/$APP" "/opt/spark/data/$BAI/$DATASET/numbers.txt" 2>&1 \
  | grep -vE '^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9:]+ (INFO|WARN) '

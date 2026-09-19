# Spark 3.5.9 trên Docker – Cụm Standalone

**Môi trường:** Docker Desktop (backend WSL2) trên Windows 11, image chính thức
`spark:3.5.9-python3` (Spark 3.5.9, Java 11, Python 3.10).
**Chế độ chạy:** Spark Standalone – không dùng HDFS, không dùng YARN. Master, worker và History
Server mỗi thứ chạy trong một container riêng; dữ liệu đọc/ghi thẳng trên thư mục `data/` của máy.

Cụm Hadoop + Spark cài trực tiếp trong WSL (xem `../BAO-CAO-CAI-DAT-HADOOP-SPARK.md`) **không bị
ảnh hưởng**: thư mục này không dùng gì trong `~/opt`. Chỉ lưu ý **không chạy cả hai cùng lúc**
(xem [Lưu ý](#lưu-ý)).

## Kiến trúc

| Container | Vai trò | Web UI |
|---|---|---|
| `spark-master` | Điều phối cụm; đồng thời là nơi chạy **driver** khi `spark-submit` | http://localhost:8080 |
| `spark-spark-worker-1` | Chạy executor: 4 core, 1,5 GB RAM | http://localhost:8081 |
| `spark-history` | Xem lại các job đã chạy xong (đọc event log) | http://localhost:18080 |
| – | Job **đang** chạy (driver trong `spark-master`) | http://localhost:4040 |

```
         spark-submit (docker exec)
                  │
      ┌───────────▼───────────┐   spark://spark-master:7077   ┌──────────────────┐
      │ spark-master          │◄──────────────────────────────│ spark-worker     │
      │  Master + driver      │──── giao task cho executor ──►│  executor 1 GB   │
      └───────────┬───────────┘                               └────────┬─────────┘
                  │ ghi event log                                      │
      ┌───────────▼───────────┐                                        │
      │ volume spark-events   │◄── spark-history đọc                   │
      └───────────────────────┘                                        │
      ./apps  → /opt/spark/apps   (mọi container)  ◄───────────────────┘
      ./data  → /opt/spark/data   (mọi container)
```

## Cấu trúc thư mục

```
spark-docker/
├── compose.yaml                   # Định nghĩa 3 service: master, worker, history
├── run.sh                         # Chạy Bài 7 / Bài 14: bash run.sh <bai07|bai14> [sample|large]
├── conf/
│   └── spark-defaults.conf        # Cấu hình chung: địa chỉ master, RAM, event log
├── apps/                          # Chương trình Spark (.py / .jar) → /opt/spark/apps
│   ├── bai07_dem_so_duy_nhat.py   #   Bài 7: đếm số xuất hiện đúng 1 lần
│   ├── bai14_dem_so_nguyen_to.py  #   Bài 14: đếm số nguyên tố
│   └── wordcount.py               #   Ví dụ: đếm từ, ghi kết quả CSV
└── data/                          # Dữ liệu vào/ra → /opt/spark/data
    ├── bai07/{sample,large}/numbers.txt   # Cùng dữ liệu với bản MapReduce
    ├── bai14/{sample,large}/numbers.txt   #   (large bị .gitignore bỏ qua, run.sh tự sinh lại)
    ├── input.txt                  #   Dữ liệu mẫu cho wordcount.py
    └── output/                    #   Kết quả (bị .gitignore bỏ qua)
```

## Chạy nhanh

Mở WSL (Ubuntu) – hoặc từ máy khác: `ssh -t diep-pc wsl -d Ubuntu` – rồi:

```bash
cd /mnt/c/Users/thomm/Coding/Big_data/spark-docker

docker compose up -d                                                      # 1. Khởi động cụm
docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/apps/wordcount.py   # 2. Chạy job
cat data/output/wordcount/part-*.csv                                      # 3. Xem kết quả
docker compose down                                                       # 4. Dừng cụm
```

---

## Bài 7 & Bài 14 trên Spark

Hai bài giống phần MapReduce (`../mapreduce/`), viết lại bằng Spark RDD API và chạy trên cụm này.

```bash
cd /mnt/c/Users/thomm/Coding/Big_data/spark-docker

bash run.sh bai07 sample     # hoặc: large
bash run.sh bai14 sample     # hoặc: large
docker compose down          # dừng cụm khi xong
```

`run.sh` làm 3 bước: chuẩn bị dữ liệu (thiếu `large` thì sinh lại bằng `gen_data.py` của bản
MapReduce – cùng seed nên cùng dữ liệu) → khởi động cụm nếu chưa chạy và đợi worker đăng ký →
`spark-submit` chương trình. Có thể chạy tay với file bất kỳ trong `data/`:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  /opt/spark/apps/bai07_dem_so_duy_nhat.py /opt/spark/data/bai07/large/numbers.txt
```

### Thuật toán

| | Bài 7 – Đếm số duy nhất | Bài 14 – Đếm số nguyên tố |
|---|---|---|
| Pipeline | `textFile → flatMap → map(n,1) → reduceByKey → filter(tần suất = 1) → count` | `textFile → flatMap → filter(is_prime) → count` |
| Shuffle | Có (`reduceByKey` gom theo số) | Không – mỗi số kiểm tra độc lập |
| Kiểm tra nguyên tố | – | Chia thử tới √n, chỉ thử ước lẻ |
| So với MapReduce | 1 chương trình thay cho chuỗi 2 job | 1 `filter` thay cho map + combiner + reducer |

### Kết quả (đã chạy trên `diep-pc`, trùng khớp với bản MapReduce)

| Bộ dữ liệu | Bài 7 | Bài 14 | Thời gian |
|---|---|---|---|
| `sample` | 21 số, 13 giá trị phân biệt, xuất hiện 1 lần: `0 1 15 21 42 100` → **6** | **15** số nguyên tố / 37 số | Bài 7: 8 s · Bài 14: **137 s** |
| `large` (1.000.000 số) | 631.532 giá trị phân biệt → **367.509** | **66.561** số nguyên tố | ~10 s mỗi bài |

- **Bài 14 / sample chậm là bình thường:** dữ liệu mẫu có số 2⁶³−25 (nguyên tố), chia thử tới √n
  cần ~1,5 tỷ vòng lặp Python trên một core. Bản MapReduce dùng Miller–Rabin nên không gặp vấn đề
  này. Bộ `large` chỉ có số ≤ 10⁷ (√n ≤ 3.163) nên nhanh.
- Danh sách kết quả dài hơn 20 phần tử chỉ in 20 giá trị nhỏ nhất (in hết 367.509 số sẽ ra một
  dòng hàng MB).

### Những điểm đã chỉnh để chạy được trên cụm

| Điểm chỉnh | Lý do |
|---|---|
| `INPUT` là đường dẫn tuyệt đối `/opt/spark/data/...`, truyền được qua tham số | Đường dẫn tương đối (`input/numbers.txt`) được driver (container master) và executor (container worker) hiểu theo hai thư mục làm việc khác nhau → executor không tìm thấy file |
| Tách token bằng `re.split(r"[\s,;]+")` thay cho `line.split()` | `line.split()` làm `8,` và `15;` không đổi được sang số và bị bỏ mất: Bài 7/sample vẫn ra 6 nhưng **danh sách sai** (`8` thay vì `15`, chỉ đọc được 19/21 số); Bài 14/sample đọc được 36/37 số |
| `.cache()` cho các RDD dùng lại nhiều lần | Mỗi action (`count`, `collect`…) chạy lại toàn bộ pipeline từ đầu; không cache thì Bài 14/sample kiểm tra 2⁶³−25 **hai lần** (gấp đôi thời gian) |
| In tối đa 20 phần tử | Tránh `collect()` và in hàng trăm nghìn số với bộ `large` |

---

## Hướng dẫn chi tiết

### 1. Yêu cầu

- **Docker Desktop đang chạy** trên Windows (biểu tượng cá voi ở khay hệ thống). Kiểm tra:
  `docker ps` không báo lỗi.
- **Image `spark:3.5.9-python3`** (1,9 GB). Kiểm tra: `docker images spark`. Nếu chưa có, xem bước 2.
- **RAM:** Docker và WSL dùng chung một máy ảo (~3,8 GB). Cụm lúc rảnh tốn khoảng **720 MB**.

### 2. Tải image (chỉ làm một lần)

Nếu đang ngồi trực tiếp trên máy:

```bash
docker pull spark:3.5.9-python3
```

Nếu đang **SSH vào máy**, lệnh trên sẽ lỗi
`error getting credentials ... A specified logon session does not exist`: trình quản lý mật khẩu
của Docker Desktop cần phiên đăng nhập Windows, mà phiên SSH không có. Image này công khai nên
tải ẩn danh bằng một thư mục cấu hình rỗng:

```bash
mkdir -p ~/.docker-anon && echo '{}' > ~/.docker-anon/config.json
DOCKER_CONFIG=~/.docker-anon docker pull spark:3.5.9-python3
```

Sau khi đã có image, `docker compose up` chạy bình thường qua SSH, không cần mẹo này.

### 3. Khởi động cụm

```bash
cd /mnt/c/Users/thomm/Coding/Big_data/spark-docker
docker compose up -d
docker compose ps
```

Kết quả mong đợi – cả 3 container có `STATUS` là `Up` (đã rút gọn bớt cột):

```
NAME                   SERVICE         STATUS
spark-history          spark-history   Up 12 seconds
spark-master           spark-master    Up 12 seconds
spark-spark-worker-1   spark-worker    Up 12 seconds
```

Sau khoảng 10 giây worker đăng ký với master. Mở http://localhost:8080 phải thấy
**Alive Workers: 1**, **Cores in use: 4 Total**, **Memory in use: 1536.0 MiB Total**.

### 4. Chạy job

Mọi lệnh `spark-submit` được chạy **bên trong** container `spark-master` bằng `docker exec`.
Không cần truyền `--master`: đã đặt sẵn `spark://spark-master:7077` trong `conf/spark-defaults.conf`.

**Ví dụ có sẵn của Spark (Scala) – tính số Pi:**

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.12-3.5.9.jar 50
# ... Pi is roughly 3.14185...
```

**Chương trình PySpark của mình:**

```bash
# Dữ liệu mặc định: data/input.txt → data/output/wordcount/
docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/apps/wordcount.py

# Chỉ định file vào/ra (đường dẫn BÊN TRONG container)
docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/apps/wordcount.py \
  /opt/spark/data/input.txt /opt/spark/data/output/wordcount
```

**Shell tương tác:**

```bash
docker exec -it spark-master /opt/spark/bin/pyspark
>>> spark.range(1_000_000).selectExpr("sum(id)").show()
```

**Tùy chỉnh tài nguyên cho một job** (không sửa file cấu hình):

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --executor-memory 1g --total-executor-cores 2 /opt/spark/apps/wordcount.py
```

### 5. Đường dẫn: máy ↔ container

Chương trình chạy trong container nên **phải dùng đường dẫn của container**:

| Trên máy (Windows / WSL) | Trong container |
|---|---|
| `spark-docker\apps\` · `/mnt/c/.../spark-docker/apps/` | `/opt/spark/apps/` |
| `spark-docker\data\` · `/mnt/c/.../spark-docker/data/` | `/opt/spark/data/` |
| `spark-docker\conf\spark-defaults.conf` | `/opt/spark/conf/spark-defaults.conf` |
| (volume Docker `spark_spark-events`) | `/opt/spark/spark-events/` |

File sửa trên máy có hiệu lực ngay trong container, không cần khởi động lại. Kết quả job ghi vào
`/opt/spark/data/...` xuất hiện trong `data/` và thuộc sở hữu của user `thomm`.

### 6. Web UI

Trên chính máy `diep-pc`, mở trình duyệt tới các địa chỉ trong bảng [Kiến trúc](#kiến-trúc).

Từ **máy khác**, mở đường hầm SSH rồi dùng đúng các địa chỉ đó:

```bash
ssh -N -L 8080:localhost:8080 -L 8081:localhost:8081 \
       -L 4040:localhost:4040 -L 18080:localhost:18080 diep-pc
```

- **4040** chỉ có khi một job đang chạy.
- **18080** hiện job sau khi job kết thúc (có độ trễ vài giây).

### 7. Dừng cụm

```bash
docker compose down        # Xóa container, GIỮ lịch sử job (volume spark-events)
docker compose down -v     # Xóa luôn lịch sử job
docker compose logs -f spark-worker   # (khi cần) xem log của một service
```

---

## Viết chương trình mới

1. Tạo file trong `apps/`, ví dụ `apps/bai_moi.py`; đặt dữ liệu vào `data/`.
2. Khung chương trình:

   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.appName("bai-moi").getOrCreate()   # KHÔNG gọi .master(...)
   df = spark.read.csv("/opt/spark/data/du_lieu.csv", header=True, inferSchema=True)
   # ... xử lý ...
   df.write.mode("overwrite").parquet("/opt/spark/data/output/bai_moi")
   spark.stop()
   ```

3. Chạy: `docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/apps/bai_moi.py`

Quy tắc:

- **Không** gọi `.master("local[*]")` trong code – nếu gọi, job chạy cục bộ trong container master
  và bỏ qua worker.
- Đọc/ghi qua `/opt/spark/data/...`. **Mọi container** (cả worker) phải thấy cùng đường dẫn,
  nên đừng dùng `/tmp` hay đường dẫn chỉ có trên máy.
- Container dùng **Python 3.10** (WSL là 3.12) – tránh cú pháp/thư viện chỉ có từ 3.11.
- Cần thư viện ngoài (pandas, numpy…)? Image không có sẵn; phải tạo image riêng từ
  `spark:3.5.9-python3` rồi `pip install`.

## Cấu hình tài nguyên

| Muốn thay đổi | Sửa ở đâu | Mặc định |
|---|---|---|
| Số core / RAM worker cấp cho executor | `compose.yaml` → `--cores 4 --memory 1536m` | 4 core, 1,5 GB |
| RAM mỗi executor | `conf/spark-defaults.conf` → `spark.executor.memory` | 1 GB |
| RAM driver | `conf/spark-defaults.conf` → `spark.driver.memory` | 512 MB |
| Heap của tiến trình master / worker / history | `compose.yaml` → `SPARK_DAEMON_MEMORY` | 512 MB |
| Số partition khi shuffle (SQL) | `conf/spark-defaults.conf` → `spark.sql.shuffle.partitions` | 8 |

- `spark.executor.memory` **phải ≤** bộ nhớ worker (`--memory`), nếu không job sẽ treo với
  thông báo `Initial job has not accepted any resources`.
- Sửa `compose.yaml` → chạy lại `docker compose up -d`. Sửa `spark-defaults.conf` → có hiệu lực
  từ lần `spark-submit` sau.

## Xử lý sự cố

| Hiện tượng | Nguyên nhân | Cách khắc phục |
|---|---|---|
| `error getting credentials ... logon session does not exist` khi `docker pull` | Trình quản lý mật khẩu của Docker Desktop cần phiên đăng nhập Windows, phiên SSH không có | Tải qua `DOCKER_CONFIG=~/.docker-anon` (bước 2) hoặc chạy lệnh ngay trên máy |
| `Cannot connect to the Docker daemon` | Docker Desktop chưa chạy | Mở Docker Desktop trên Windows, đợi báo *Engine running* |
| `chmod: changing permissions of '/opt/spark/data/...': Operation not permitted` | Container chạy bằng user khác root (uid 185) – không chmod được thư mục bind-mount từ ổ C: | Giữ `user: root` trong `compose.yaml` |
| Master thoát ngay: `failure to login ... invalid null input: name` | Container chạy bằng uid không có tên trong `/etc/passwd` của image (ví dụ `user: "1000"`) | Giữ `user: root` |
| `Bind for 127.0.0.1:18080 failed: port is already allocated` | Spark History Server của cụm WSL đang chạy | `bash ~/bin/stop-cluster.sh` rồi `docker compose up -d` |
| `Bind for 127.0.0.1:8081 failed` khi `--scale spark-worker=2` | Hai worker cùng xin cổng 8081 | Không scale; với ~3,8 GB RAM, 1 worker là hợp lý |
| Job treo: `Initial job has not accepted any resources` | Worker chưa đăng ký, hoặc executor xin nhiều RAM/core hơn worker có | Kiểm tra http://localhost:8080 có 1 worker; giảm `spark.executor.memory` |
| `Connection refused ... localhost:9000` | Dùng `spark-submit` của WSL (`~/opt/spark`) thay vì trong container – cấu hình đó ghi log lên HDFS | Chạy qua `docker exec spark-master ...` |

## Lưu ý

- **Không chạy song song với cụm Hadoop/Spark trong WSL.** Cả hai dùng chung ~3,8 GB RAM của máy ảo
  WSL2, và cùng dùng cổng 18080. Dừng cụm này (`docker compose down`) trước khi
  `bash ~/bin/start-cluster.sh`, và ngược lại.
- **Bảo mật:** Spark master không có xác thực – ai kết nối được cổng 7077 đều chạy được code. Vì
  vậy mọi cổng chỉ mở trên `127.0.0.1`; truy cập từ xa bằng đường hầm SSH (bước 6), **đừng** đổi
  thành `0.0.0.0`.
- **Chạy bằng root trong container** là cố ý (xem bảng sự cố). Kết quả ghi ra `data/` vẫn thuộc
  user `thomm` trên máy.

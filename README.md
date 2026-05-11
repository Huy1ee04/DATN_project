* Module vwap

uv sync --extra vwap

docker compose --profile vwap up -d

1, mở terminal 1 (lấy dữ liệu thị trường đưa vào kafka topic): 
uv run vwap_system/producer/ohlc_producer.py
2, mở terminal 2 (đọc dữ liệu từ kafka topic, tính toán VWAP và đưa ra cảnh báo:
uv run vwap_system/alert_detector/detector.py
3, mở terminal 3 (hiển thị dữ liệu và cảnh báo):
uv run streamlit run vwap_system/dashboard/app.py
4, mở terminal 4 (back up dữ liệu thô cuối ngày):
uv run vwap_system/backup/minio_exporter.py

---

## Module Airflow (ingestion theo lịch, vnstock_data)

Chuẩn bị file `.env` ở **thư mục gốc repo** (được mount vào container tại `/opt/airflow/.env`):

- `VNSTOCK_API_KEY` — bắt buộc cho bước cài vnstock qua CLI installer.
- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` / `MINIO_BUCKET` — khớp với service `minio` trong `docker-compose.yml` (mặc định: `minio_access_key`, `minio_secret_key`, `stock-data`).
- Các biến khác script cần (ví dụ `SYMBOLS`, …).

**Lưu ý mạng Docker:** các DAG ghi đè `MINIO_ENDPOINT=http://minio:9000` sau khi `source .env`, vì trong container `localhost` không trỏ tới MinIO. Trên máy host, script Python vẫn có thể dùng `MINIO_ENDPOINT=localhost:9100` trong `.env`.

### Lần đầu: khởi động stack + tạo venv vnstock (1 lần)

Trong thư mục repo:

```bash
# 1) Bật ClickHouse, MinIO, Postgres, Airflow (scheduler + webserver)
docker compose up -d

# 2) Tạo volume vnstock-venv + vnstock-home và cài vnstock_data (profile setup)
docker compose --profile setup run --rm vnstock-setup
```

Bước (2) tải installer từ vnstocks.com, cài dependency vào volume `vnstock-venv`, đăng ký thiết bị/license vào volume `vnstock-home`. Chỉ chạy lại khi cần cài lại môi trường.

### Cài lại venv vnstock từ đầu

Service `vnstock-setup` **bỏ qua** cài đặt nếu đã có `/opt/vnstock-venv/bin/python` trong volume. Để làm sạch và chạy lại:

```bash
docker compose down
docker volume rm datn_project_vnstock-venv datn_project_vnstock-home
# Nếu tên project khác, xem đúng tên: docker volume ls | grep vnstock
docker compose up -d
docker compose --profile setup run --rm vnstock-setup
```

### Mở UI và trigger DAG

```bash
# Webserver map cổng host 8083 → 8080 trong container
open http://localhost:8083
```

- DAG mặc định **paused** (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`). Trong UI: bật toggle **Unpause** cho DAG cần chạy.
- **Trigger:** nút **Trigger DAG** (hoặc *Trigger DAG w/ config* nếu DAG hỗ trợ `run_date` trong docstring).

Nếu chưa có tài khoản đăng nhập Airflow, tạo một lần (đổi user/password theo ý bạn):

```bash
docker compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### Kiểm tra nhanh

```bash
docker compose ps
docker compose logs -f airflow-scheduler
```
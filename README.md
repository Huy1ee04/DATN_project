# Hệ Thống Phân Tích Dữ Liệu Chứng Khoán (Batch & Real-time Hybrid Pipeline)

Hệ thống thu thập, xử lý và phân tích dữ liệu chứng khoán Việt Nam kết hợp luồng xử lý theo lô (Batch) phục vụ phân tích dữ liệu lịch sử và luồng xử lý thời gian thực (Real-time) phát hiện cảnh báo sớm. 

Hệ thống được thiết kế theo kiến trúc lai Lambda & Medallion, sử dụng hồ dữ liệu MinIO, kho dữ liệu phân tích ClickHouse, điều phối bởi Apache Airflow và trực quan hóa qua Next.js 14 & FastAPI.

---

## 🚀 Tính Năng Chính

1. **Luồng xử lý theo lô (Batch Pipeline):**
   * **Ingestion:** Thu thập định kỳ dữ liệu tham chiếu (doanh nghiệp, sự kiện, tin tức, chỉ số) và dữ liệu giao dịch từ thư viện Vnstock.
   * **Transform (Medallion):** Chuẩn hóa, biến đổi qua 3 tầng chất lượng dữ liệu (Bronze $\rightarrow$ Silver $\rightarrow$ Gold/Master) bằng thư viện hiệu năng cao **Polars**.
   * **Quality Control:** Kiểm định chất lượng dữ liệu tại mỗi cổng (validation gate) bằng framework **Great Expectations** (qua wrapper tự xây dựng `vtit_gx` hoạt động trực tiếp trên Polars).
   * **OLAP Load:** Đồng bộ hoàn toàn dữ liệu từ hồ dữ liệu MinIO sang kho dữ liệu ClickHouse (1 Shard, 2 Replicas, điều phối qua ClickHouse Keeper) để phục vụ phân tích.

2. **Luồng xử lý thời gian thực (Real-time Pipeline):**
   * **Streaming Ingestion:** Tiếp nhận nến giao dịch 1 phút từ WebSocket API của DNSE qua `ohlc_producer.py` đẩy vào **Apache Kafka** broker.
   * **OLAP Ingestion:** ClickHouse tự động tiêu thụ từ Kafka thông qua cơ chế **Kafka Engine + Materialized View** ghi vào bảng nội bộ.
   * **Alert Detection:** Định kỳ quét ClickHouse tính toán chỉ báo động (VWAP tích lũy, dải Bollinger Bands $2\sigma$, RSI, Volume Spike) qua `detector.py` để phát hiện cảnh báo kết hợp đa tín hiệu.
   * **Slack Alert:** Tích hợp gửi thông báo đẩy lập tức đến kênh Slack khi phát hiện tín hiệu nghiêm trọng (`CRITICAL`).

3. **Quản lý vòng đời dữ liệu (Data Lifecycle):**
   * Tự động lưu trữ (archive) dữ liệu nến thô/cảnh báo thời gian thực thành file Parquet trên MinIO hàng ngày.
   * Tự động dọn dẹp (purge) dữ liệu ClickHouse cũ hơn 30 ngày để tối ưu hóa hiệu năng bộ nhớ.

4. **Ứng dụng trực quan hóa (Dashboard):**
   * **Backend:** FastAPI phục vụ dữ liệu truy vấn thông qua các endpoints REST API tối ưu hóa.
   * **Frontend:** Next.js 14 hiển thị Dashboard tổng quan thị trường, chi tiết cổ phiếu (TradingView Lightweight Charts), phân tích ngành (Nivo Treemap), bộ lọc cổ phiếu (`screener`) và giám sát độ trễ luồng streaming.

---

## 📁 Cấu Trúc Dự Án

```
├── airflow/               # Cấu hình Apache Airflow & DAGs điều phối
├── backend/               # Mã nguồn API Backend (FastAPI, Python)
├── frontend/              # Giao diện người dùng (Next.js 14, TypeScript)
├── ingestion/             # Tập hợp các script thu thập dữ liệu thô (raw)
├── transformed/           # Script biến đổi dữ liệu (Stage 1 & Stage 2)
├── master/                # Script chuẩn hóa nghiệp vụ dữ liệu lên tầng Master
├── vwap_system/           # Luồng xử lý realtime (WebSocket, Kafka, Alert)
├── vtit_gx/               # Thư viện kiểm định chất lượng (Polars + Great Expectations)
├── clickhouse_student/    # Cấu hình cụm ClickHouse & SQL schemas
├── scripts/               # Script tiện ích và tự động hóa vận hành
├── docker-compose.yml     # Định nghĩa toàn bộ hạ tầng dịch vụ Docker
└── README.md
```

---

## 🛠️ Hướng Dẫn Khởi Chạy Nhanh

### 1. Thiết lập Môi Trường
Sao chép tệp cấu hình mẫu và khai báo các khóa bí mật của bạn:
```bash
cp .env.example .env
# Chỉnh sửa tệp .env điền DNSE_API_KEY, SLACK_WEBHOOK_URL, ...
```

### 2. Khởi Động Hạ Tầng (Docker)
Khởi chạy ClickHouse, Kafka, MinIO, PostgreSQL và Airflow:
```bash
docker compose up -d
```

### 3. Thiết Lập Môi Trường Ảo Airflow (Setup 1 lần đầu)
```bash
# Khởi chạy setup cài đặt thư viện cho Airflow
docker compose --profile setup run --rm vnstock-setup

# Khởi động lại các tác vụ Airflow sau khi cài đặt thành công
docker compose up -d airflow-scheduler airflow-webserver
```
*Truy cập giao diện quản lý Airflow tại: [http://localhost:8083](http://localhost:8083) (User: `admin` / Pass: `admin`)*

### 4. Vận Hành Luồng Real-time (WebSocket & Kafka)
Khởi chạy cụm Kafka và chạy các tiến trình Python:
```bash
# Khởi động hạ tầng realtime trong docker
docker compose --profile vwap up -d

# Kích hoạt luồng thời gian thực bằng bash script tự động
bash scripts/start_vwap.sh
```

---

## 📈 Giám Sát và Tài Liệu API

* **FastAPI Swagger UI:** [http://localhost:8000/docs](http://localhost:8000/docs) (Chạy cục bộ) hoặc [http://localhost:8001/docs](http://localhost:8001/docs) (Chạy docker).
* **Kafka UI:** [http://localhost:8080](http://localhost:8080)
* **MinIO Console:** [http://localhost:9001](http://localhost:9001)
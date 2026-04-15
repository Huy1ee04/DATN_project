* Module vwap

uv sync --extra vwap

docker compose --profile vwap up -d

1, mở terminal 1 (lấy dữ liệu thị trường đưa vào kafka topic): 
uv run vwap_system/producer/trade_producer.py
2, mở terminal 2 (đọc dữ liệu từ kafka topic, tính toán VWAP và đưa ra cảnh báo):
uv run vwap_system/alert_detector/detector.py
3, mở terminal 3 (hiển thị dữ liệu và cảnh báo):
uv run streamlit run vwap_system/dashboard/app.py
4, mở terminal 4 (back up dữ liệu thô cuối ngày):
uv run vwap_system/backup/minio_exporter.py
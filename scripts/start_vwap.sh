#!/bin/bash
# start_vwap.sh — Khởi động VWAP streaming services trước phiên giao dịch
# Cron: 50 8 * * 1-5  /Users/builehuy/DATN_project/scripts/start_vwap.sh

set -e
PROJECT_DIR="/Users/builehuy/DATN_project"
LOG_DIR="$PROJECT_DIR/logs/vwap"
PID_DIR="$PROJECT_DIR/logs/vwap/pids"
DATE=$(date +%Y-%m-%d)

mkdir -p "$LOG_DIR" "$PID_DIR"

# Kill previous instances (nếu còn sống) trước khi start mới
"$PROJECT_DIR/scripts/stop_vwap.sh" 2>/dev/null || true
sleep 2

echo "[$DATE $(date +%H:%M:%S)] Starting VWAP services..." >> "$LOG_DIR/cron.log"

# 1. OHLC Producer
cd "$PROJECT_DIR"
nohup uv run vwap_system/producer/ohlc_producer.py \
  >> "$LOG_DIR/producer_$DATE.log" 2>&1 &
echo $! > "$PID_DIR/producer.pid"
echo "  Producer PID: $!" >> "$LOG_DIR/cron.log"

# 2. Alert Detector
nohup uv run vwap_system/alert_detector/detector.py \
  >> "$LOG_DIR/detector_$DATE.log" 2>&1 &
echo $! > "$PID_DIR/detector.pid"
echo "  Detector PID: $!" >> "$LOG_DIR/cron.log"

echo "[$DATE $(date +%H:%M:%S)] All services started." >> "$LOG_DIR/cron.log"

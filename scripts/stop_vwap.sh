#!/bin/bash
# stop_vwap.sh — Tắt VWAP streaming services sau phiên giao dịch
# Cron: 10 15 * * 1-5  /Users/builehuy/DATN_project/scripts/stop_vwap.sh

set -e
PROJECT_DIR="/Users/builehuy/DATN_project"
LOG_DIR="$PROJECT_DIR/logs/vwap"
PID_DIR="$PROJECT_DIR/logs/vwap/pids"
DATE=$(date +%Y-%m-%d)

echo "[$DATE $(date +%H:%M:%S)] Stopping VWAP services..." >> "$LOG_DIR/cron.log"

for svc in producer detector; do
  PID_FILE="$PID_DIR/$svc.pid"
  if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
      kill "$PID"
      echo "  Stopped $svc (PID $PID)" >> "$LOG_DIR/cron.log"
    else
      echo "  $svc (PID $PID) already stopped" >> "$LOG_DIR/cron.log"
    fi
    rm -f "$PID_FILE"
  else
    echo "  No PID file for $svc" >> "$LOG_DIR/cron.log"
  fi
done

echo "[$DATE $(date +%H:%M:%S)] All services stopped." >> "$LOG_DIR/cron.log"

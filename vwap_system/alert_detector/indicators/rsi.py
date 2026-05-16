"""
RSI — Relative Strength Index calculator.

Công thức:
    RSI = 100 - 100 / (1 + RS)
    RS  = avg_gain / avg_loss   (trung bình N phiên)
"""

import logging
from typing import Optional

logger = logging.getLogger('indicators.rsi')


def compute_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """
    Tính RSI từ danh sách close prices.

    Args:
        closes: Danh sách giá đóng cửa, **mới nhất ở cuối**.
                Cần ít nhất ``period + 1`` phần tử.
        period: Chu kỳ RSI (mặc định 14).

    Returns:
        RSI (0-100) hoặc None nếu không đủ dữ liệu.
    """
    if len(closes) < period + 1:
        return None

    # Tính biến động giá giữa các nến liên tiếp
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

    # Lấy `period` delta gần nhất
    recent = deltas[-period:]

    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0  # Không có loss → RSI = 100
    if avg_gain == 0:
        return 0.0  # Không có gain → RSI = 0

    rs = avg_gain / avg_loss
    rsi = 100.0 - 100.0 / (1.0 + rs)
    return round(rsi, 2)

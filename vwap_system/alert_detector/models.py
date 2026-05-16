"""
Alert models — dataclasses và enums cho hệ thống cảnh báo đa tín hiệu.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class AlertSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertCategory(str, Enum):
    TREND = "TREND"             # VWAP
    MOMENTUM = "MOMENTUM"       # RSI
    VOLUME = "VOLUME"           # Volume Spike


@dataclass
class Alert:
    """Một cảnh báo được phát ra bởi một rule."""
    alert_time: datetime
    symbol: str
    rule_name: str          # "VWAP", "RSI", "VOLUME_SPIKE"
    alert_type: str         # "VWAP_BREAKOUT_UP", "RSI_OVERBOUGHT", ...
    severity: str           # AlertSeverity value
    price: float
    indicator_value: float  # Giá trị chỉ báo lúc trigger (VWAP, RSI, vol ratio)
    threshold: float        # Ngưỡng đã dùng để trigger
    deviation_pct: float    # % lệch (tương thích cũ)
    message: str            # Mô tả tiếng Việt cho dashboard

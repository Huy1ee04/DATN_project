"""
BaseAlertRule — abstract class cho tất cả alert rules.

Mỗi rule kế thừa class này và implement ``evaluate()``.
Cooldown mechanism tích hợp sẵn: tránh spam cùng loại alert
cho cùng symbol trong khoảng thời gian ngắn.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Optional

from models import Alert
from candle_buffer import CandleBuffer

ICT = timezone(timedelta(hours=7))


class BaseAlertRule(ABC):
    """Base class cho tất cả alert rules."""

    # Tên rule — subclass phải gán
    RULE_NAME: str = ""

    def __init__(self, config, cooldown_sec: int = 300):
        self.config = config
        self.cooldown_sec = cooldown_sec
        # (symbol, alert_type) → thời điểm fire gần nhất
        self._last_fired: dict[tuple[str, str], datetime] = {}

    @abstractmethod
    def evaluate(
        self,
        symbol: str,
        price: float,
        ts: datetime,
        buffer: CandleBuffer,
    ) -> Optional[Alert]:
        """
        Đánh giá xem có cần phát cảnh báo không.

        Args:
            symbol: Mã chứng khoán.
            price: Giá close hiện tại.
            ts: Thời điểm nến.
            buffer: CandleBuffer chứa N nến gần nhất.

        Returns:
            Alert nếu thỏa điều kiện, None nếu không.
        """
        ...

    def _can_fire(self, symbol: str, alert_type: str, ts: datetime) -> bool:
        """Kiểm tra cooldown — trả False nếu đang trong cooldown."""
        key = (symbol, alert_type)
        last = self._last_fired.get(key)
        if last and (ts - last).total_seconds() < self.cooldown_sec:
            return False
        return True

    def _mark_fired(self, symbol: str, alert_type: str, ts: datetime) -> None:
        """Đánh dấu đã fire — bắt đầu cooldown."""
        self._last_fired[(symbol, alert_type)] = ts

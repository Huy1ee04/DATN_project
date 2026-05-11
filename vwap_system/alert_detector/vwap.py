"""
VWAP Calculator — Volume Weighted Average Price (Session/Daily)

Tính session VWAP theo công thức chuẩn:
    typical_price = (high + low + close) / 3
    VWAP = Σ(typical_price * volume) / Σ(volume)

Anchor cho mỗi phiên/symbol được lấy từ "bản ghi đầu tiên" trong ngày.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, time, timezone, timedelta
from typing import Dict, Optional, Tuple

logger = logging.getLogger('vwap')

ICT = timezone(timedelta(hours=7))
MARKET_OPEN = time(9, 0, 0)
MARKET_CLOSE = time(14, 45, 0)


@dataclass
class AnchorState:
    anchor_id: str
    symbol: str
    anchor_time: datetime
    # Σ(typical_price × volume)
    sum_pv: float = 0.0
    # Σ(volume)
    sum_qty: int = 0
    # Σ(typical_price^2 × volume) — dùng để suy ra σ theo công thức phương sai có trọng số
    sum_p2v: float = 0.0
    tick_count: int = 0

    @property
    def vwap(self) -> Optional[float]:
        """VWAP = Σ(price×qty) / Σ(qty)"""
        return self.sum_pv / self.sum_qty if self.sum_qty > 0 else None

    @property
    def sigma(self) -> Optional[float]:
        """Volume-weighted standard deviation of typical price around current VWAP."""
        if self.sum_qty <= 0:
            return None
        vwap = self.sum_pv / self.sum_qty
        # variance = E[x^2] - (E[x])^2 (weighted)
        variance = self.sum_p2v / self.sum_qty - vwap * vwap
        variance = max(variance, 0.0)
        return variance ** 0.5


class VWAPCalculator:
    """
    Quản lý VWAP (Session) theo symbol.

    Example:
        calc = VWAPCalculator()
        # Một nến 1 phút (OHLCV)
        calc.update(
            "HPG",
            high=27.0,
            low=26.5,
            close=26.75,
            volume=1000,
            ts=datetime.now(ICT),
        )
        session_vwap = calc.get_session_vwap("HPG")
    """

    def __init__(self):
        # (symbol, anchor_id) → AnchorState
        self._states: Dict[Tuple[str, str], AnchorState] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(
        self,
        symbol: str,
        high: float,
        low: float,
        close: float,
        volume: int,
        ts: datetime,
    ) -> None:
        """Cập nhật session VWAP từ 1 nến OHLCV."""
        ts = ts.astimezone(ICT) if ts.tzinfo else ts.replace(tzinfo=ICT)
        t = ts.time()

        # Chỉ xử lý trong giờ giao dịch.
        if not (MARKET_OPEN <= t <= MARKET_CLOSE):
            return

        typical_price = (high + low + close) / 3.0
        self._update_session(symbol, typical_price, volume, ts)

    def get_session_vwap(self, symbol: str, ts: Optional[datetime] = None) -> Optional[float]:
        """Lấy VWAP phiên hiện tại của symbol."""
        ref_ts = ts or datetime.now(ICT)
        sid = self._session_id(symbol, ref_ts)
        state = self._states.get((symbol, sid))
        return state.vwap if state else None

    def get_session_vwap_and_sigma(
        self, symbol: str, ts: Optional[datetime] = None
    ) -> Tuple[Optional[float], Optional[float]]:
        """Lấy (VWAP, σ) phiên hiện tại của symbol."""
        ref_ts = ts or datetime.now(ICT)
        sid = self._session_id(symbol, ref_ts)
        state = self._states.get((symbol, sid))
        if not state:
            return None, None
        return state.vwap, state.sigma

    def cleanup_old_anchors(self, cutoff_days: int = 1) -> None:
        """Xóa session anchor cũ hơn cutoff_days để tránh memory leak."""
        cutoff = datetime.now(ICT) - timedelta(days=cutoff_days)
        to_delete = [
            key for key, s in self._states.items()
            if s.anchor_time < cutoff
        ]
        for key in to_delete:
            self._states.pop(key, None)
        if to_delete:
            logger.debug(f"Cleaned up {len(to_delete)} old session anchors")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _session_id(self, symbol: str, ts: datetime) -> str:
        date = ts.astimezone(ICT).date()
        return f"session_{date.isoformat()}_{symbol}"

    def _update_session(
        self,
        symbol: str,
        typical_price: float,
        volume: int,
        ts: datetime,
    ) -> None:
        sid = self._session_id(symbol, ts)
        key = (symbol, sid)
        if key not in self._states:
            self._states[key] = AnchorState(
                anchor_id=sid,
                symbol=symbol,
                anchor_time=ts,
            )
            logger.info(f"Session anchor started: {symbol} @ {ts}")
        s = self._states[key]
        s.sum_pv += typical_price * volume
        s.sum_p2v += (typical_price * typical_price) * volume
        s.sum_qty += volume
        s.tick_count += 1

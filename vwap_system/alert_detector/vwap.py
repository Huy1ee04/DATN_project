"""
VWAP Calculator — Volume Weighted Average Price (Session/Daily)

Chỉ có 1 anchor mặc định là đầu phiên (Session).
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
    sum_pv: float = 0.0  # Σ(price × quantity)
    sum_qty: int = 0     # Σ(quantity)
    tick_count: int = 0

    @property
    def vwap(self) -> Optional[float]:
        """VWAP = Σ(price×qty) / Σ(qty)"""
        return self.sum_pv / self.sum_qty if self.sum_qty > 0 else None


class VWAPCalculator:
    """
    Quản lý VWAP (Session) theo symbol.

    Example:
        calc = VWAPCalculator()
        calc.update("HPG", price=26.75, quantity=1000, ts=datetime.now(ICT))
        session_vwap = calc.get_session_vwap("HPG")
    """

    def __init__(self):
        # (symbol, anchor_id) → AnchorState
        self._states: Dict[Tuple[str, str], AnchorState] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, symbol: str, price: float, quantity: int, ts: datetime) -> None:
        """Cập nhật anchor của symbol với tick mới."""
        ts = ts.astimezone(ICT) if ts.tzinfo else ts.replace(tzinfo=ICT)
        t = ts.time()

        # Chỉ xử lý trong giờ giao dịch
        if not (MARKET_OPEN <= t <= MARKET_CLOSE):
            return

        # Chỉ cập nhật Session anchor cố định theo ngày
        self._update_session(symbol, price, quantity, ts)

    def get_session_vwap(self, symbol: str, ts: Optional[datetime] = None) -> Optional[float]:
        """Lấy VWAP phiên hiện tại của symbol."""
        ref_ts = ts or datetime.now(ICT)
        sid = self._session_id(symbol, ref_ts)
        state = self._states.get((symbol, sid))
        return state.vwap if state else None

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
        self, symbol: str, price: float, quantity: int, ts: datetime
    ) -> None:
        sid = self._session_id(symbol, ts)
        key = (symbol, sid)
        if key not in self._states:
            session_start = datetime.combine(
                ts.astimezone(ICT).date(), MARKET_OPEN, tzinfo=ICT
            )
            self._states[key] = AnchorState(
                anchor_id=sid,
                symbol=symbol,
                anchor_time=session_start,
            )
            logger.info(f"Session anchor started: {symbol} @ {session_start}")
        s = self._states[key]
        s.sum_pv += price * quantity
        s.sum_qty += quantity
        s.tick_count += 1

"""
Streamlit Dashboard — Multi-Signal Alert System

Real-time visualization: price + VWAP chart, RSI chart, volume chart, alert table.
Chạy: streamlit run app.py
"""

import os
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / '.env')

logging.basicConfig(level=logging.WARNING)
ICT = timezone(timedelta(hours=7))

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'default')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'vwap')
SYMBOLS = [s.strip() for s in os.getenv('SYMBOLS', 'HPG,SSI,VNM,VCB,TCB').split(',') if s.strip()]
REFRESH_SEC = int(os.getenv('DASHBOARD_REFRESH_SEC', '5'))
RSI_PERIOD = int(os.getenv('RSI_PERIOD', '14'))

# ─── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title='Multi-Signal Alert System',
    page_icon='📈',
    layout='wide',
    initial_sidebar_state='expanded',
)
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}
.stApp {
    background: #f8f9fc;
    color: #1e293b;
}
.block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
}
/* Metrics Cards */
div[data-testid="stMetric"] {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    padding: 15px 20px;
    border-radius: 12px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.04);
    transition: all 0.3s ease;
}
div[data-testid="stMetric"]:hover {
    transform: translateY(-3px);
    border-color: #cbd5e1;
    box-shadow: 0 8px 16px rgba(0, 0, 0, 0.08);
}
div[data-testid="stMetricLabel"] > div > div > p {
    font-size: 0.95rem;
    color: #64748b;
    font-weight: 600;
}
div[data-testid="stMetricValue"] > div {
    font-size: 1.8rem;
    font-weight: 800;
    color: #0f172a;
}
/* Header Gradients */
h1 {
    font-weight: 800 !important;
    background: -webkit-linear-gradient(45deg, #2563eb, #9333ea);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    padding-bottom: 0.2rem;
}
/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #ffffff !important;
    border-right: 1px solid #e2e8f0;
}
/* Dataframe Tweaks */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid #e2e8f0;
}
thead tr th {
    background-color: #f1f5f9 !important;
    color: #334155 !important;
    font-weight: 600 !important;
}
</style>
""", unsafe_allow_html=True)


# ─── ClickHouse ───────────────────────────────────────────────
@st.cache_resource
def get_ch():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def query_df(ch, sql: str, cols: list) -> pd.DataFrame:
    try:
        rows = ch.query(sql).result_rows
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    except Exception as e:
        st.error(f"ClickHouse error: {e}")
        return pd.DataFrame(columns=cols)


# ─── Data loaders ─────────────────────────────────────────────

def load_ohlc_with_indicators(ch, symbol: str, minutes: int, start_time) -> pd.DataFrame:
    """Tải OHLCV (1 phút) + tính running VWAP + RSI qua SQL."""
    start_time_str = start_time.strftime('%H:%M:%S')
    df = query_df(ch, f"""
        SELECT * FROM (
            SELECT
                candle_time AS time,
                open, high, low,
                close AS price,
                volume AS quantity,
                vwap,
                sigma
            FROM
            (
                SELECT
                candle_time, open, high, low, close, volume,
                (
                    sum(((high + low + close) / 3.0) * volume)
                        OVER (ORDER BY candle_time
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    /
                    nullIf(
                        sum(volume) OVER (ORDER BY candle_time
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                        0
                    )
                ) AS vwap,
                sqrt(greatest(
                    (
                        sum(
                            (((high + low + close) / 3.0) * ((high + low + close) / 3.0)) * volume
                        )
                            OVER (ORDER BY candle_time
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                        /
                        nullIf(
                            sum(volume) OVER (ORDER BY candle_time
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                            0
                        )
                    )
                    -
                    (
                        (
                            sum(((high + low + close) / 3.0) * volume)
                                OVER (ORDER BY candle_time
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                            /
                            nullIf(
                                sum(volume) OVER (ORDER BY candle_time
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                                0
                            )
                        )
                        *
                        (
                            sum(((high + low + close) / 3.0) * volume)
                                OVER (ORDER BY candle_time
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                            /
                            nullIf(
                                sum(volume) OVER (ORDER BY candle_time
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                                0
                            )
                        )
                    ),
                    0
                )) AS sigma
                FROM ohlc_raw
                WHERE symbol = '{symbol}'
                  AND toDate(candle_time) = today()
            ) t
            WHERE formatDateTime(candle_time, '%H:%M:%S') >= '{start_time_str}'
            ORDER BY time DESC
            LIMIT {minutes}
        )
        ORDER BY time ASC
    """, ['time', 'open', 'high', 'low', 'price', 'quantity', 'vwap', 'sigma'])
    if not df.empty:
        df['time'] = pd.to_datetime(df['time'])
        # Tính RSI trong Python (cần toàn bộ close từ đầu phiên)
        df['rsi'] = _compute_rsi_series(df['price'], RSI_PERIOD)
    return df


def _compute_rsi_series(closes: pd.Series, period: int = 14) -> pd.Series:
    """Tính RSI cho toàn bộ chuỗi close prices."""
    delta = closes.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, float('nan'))
    rsi = 100.0 - 100.0 / (1.0 + rs)
    return rsi


def load_alerts_v2(ch, limit: int = 50, rule_filter: str = 'ALL') -> pd.DataFrame:
    where = ""
    if rule_filter != 'ALL':
        where = f"AND rule_name = '{rule_filter}'"
    return query_df(ch, f"""
        SELECT alert_time, symbol, rule_name, alert_type, severity,
               price, indicator_value, threshold, message
        FROM alerts_v2
        WHERE toDate(alert_time) = today() {where}
        ORDER BY alert_time DESC
        LIMIT {limit}
    """, ['time', 'symbol', 'rule', 'type', 'severity',
          'price', 'indicator', 'threshold', 'message'])


def load_summary(ch) -> dict:
    candles = ch.query("SELECT count() FROM ohlc_raw WHERE toDate(candle_time) = today()").result_rows
    # Thử alerts_v2, fallback alerts cũ
    try:
        alts = ch.query("SELECT count() FROM alerts_v2 WHERE toDate(alert_time) = today()").result_rows
    except Exception:
        alts = ch.query("SELECT count() FROM alerts WHERE toDate(alert_time) = today()").result_rows
    return {
        'candles': candles[0][0] if candles else 0,
        'alerts': alts[0][0] if alts else 0,
    }


def load_last_price(ch, symbol: str) -> float | None:
    rows = ch.query(
        f"SELECT close FROM ohlc_raw WHERE symbol='{symbol}' "
        f"ORDER BY candle_time DESC LIMIT 1"
    ).result_rows
    return rows[0][0] if rows else None


# ─── Chart builder ────────────────────────────────────────────

def build_multi_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Xây biểu đồ 3 panel: Price+VWAP, RSI, Volume."""
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.55, 0.20, 0.25],
        subplot_titles=[
            f'{symbol} — Price & VWAP',
            'RSI (14)',
            'Volume',
        ],
    )

    if df.empty:
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', height=750,
        )
        return fig

    # ── Panel 1: Price + VWAP + σ-bands ──
    fig.add_trace(go.Scatter(
        x=df['time'], y=df['price'], mode='lines', name='Price',
        line=dict(color='#0f172a', width=2),
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df['time'], y=df['vwap'], mode='lines', name='VWAP',
        line=dict(color='#2563eb', width=2.5, dash='dash'),
    ), row=1, col=1)

    k = float(os.getenv('BAND_SIGMA_MULTIPLIER', '2.0'))
    if 'sigma' in df.columns:
        hi = df['vwap'] + k * df['sigma']
        lo = df['vwap'] - k * df['sigma']
    else:
        hi = df['vwap'] * 1.015
        lo = df['vwap'] * 0.985
    fig.add_trace(go.Scatter(
        x=pd.concat([df['time'], df['time'][::-1]]),
        y=pd.concat([hi, lo[::-1]]),
        fill='toself', fillcolor='rgba(37, 99, 235, 0.08)',
        line=dict(color='rgba(0,0,0,0)'), name=f'±{k}σ Band',
        hoverinfo='skip'
    ), row=1, col=1)

    # ── Panel 2: RSI ──
    if 'rsi' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['time'], y=df['rsi'], mode='lines', name='RSI',
            line=dict(color='#9333ea', width=2),
        ), row=2, col=1)
        fig.add_hline(y=70, line_dash='dash', line_color='rgba(239, 68, 68, 0.7)',
                      annotation_text='70', annotation_font_color='#ef4444', row=2, col=1)
        fig.add_hline(y=30, line_dash='dash', line_color='rgba(16, 185, 129, 0.7)',
                      annotation_text='30', annotation_font_color='#10b981', row=2, col=1)
        fig.add_hline(y=50, line_dash='dot', line_color='rgba(0, 0, 0, 0.2)',
                      row=2, col=1)

    # ── Panel 3: Volume ──
    vol_avg = df['quantity'].rolling(20, min_periods=1).mean()
    spike_ratio = float(os.getenv('VOLUME_SPIKE_RATIO', '3.0'))
    colors = [
        '#ef4444' if v >= avg * spike_ratio else 'rgba(148, 163, 184, 0.5)'
        for v, avg in zip(df['quantity'], vol_avg)
    ]
    fig.add_trace(go.Bar(
        x=df['time'], y=df['quantity'], name='Volume',
        marker_color=colors, marker_line_width=0,
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=df['time'], y=vol_avg, mode='lines', name='Vol Avg',
        line=dict(color='#ea580c', width=1.5, dash='dot'),
    ), row=3, col=1)

    # ── Layout chung ──
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#475569', family='Inter'), height=750,
        legend=dict(
            bgcolor='rgba(255, 255, 255, 0.9)', bordercolor='rgba(0,0,0,0.1)', borderwidth=1,
            orientation='h', y=1.02, x=0.5, xanchor='center'
        ),
        margin=dict(l=40, r=40, t=40, b=20),
        showlegend=True,
        hovermode='x unified',
    )
    for i in range(1, 4):
        fig.update_xaxes(gridcolor='rgba(0,0,0,0.06)', zeroline=False, row=i, col=1)
        fig.update_yaxes(gridcolor='rgba(0,0,0,0.06)', zeroline=False, row=i, col=1)
    
    # Add RangeSlider to the bottom chart (Volume)
    fig.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.05, row=3, col=1)

    fig.update_yaxes(title_text='Giá', row=1, col=1)
    fig.update_yaxes(title_text='RSI', range=[0, 100], row=2, col=1)
    fig.update_yaxes(title_text='KL', row=3, col=1)

    return fig


# ─── Alert styling ────────────────────────────────────────────

SEVERITY_COLORS = {
    'CRITICAL': 'color: #ff4444; font-weight: bold',
    'WARNING': 'color: #ffa800; font-weight: bold',
    'INFO': 'color: #4fc3f7',
}

ALERT_TYPE_COLORS = {
    'VWAP_BREAKOUT_UP': 'color: #00d4aa; font-weight: bold',
    'VWAP_BREAKDOWN': 'color: #ff6b6b; font-weight: bold',
    'RSI_OVERBOUGHT': 'color: #ff6b6b; font-weight: bold',
    'RSI_OVERSOLD': 'color: #00d4aa; font-weight: bold',
    'VOLUME_SPIKE': 'color: #ffa800; font-weight: bold',
}


def style_severity(val: str) -> str:
    return SEVERITY_COLORS.get(val, '')


def style_alert_type(val: str) -> str:
    return ALERT_TYPE_COLORS.get(val, '')


# ─── Main ─────────────────────────────────────────────────────

def main():
    ch = get_ch()
    now_str = datetime.now(ICT).strftime('%H:%M:%S')

    # Sidebar
    with st.sidebar:
        st.title('⚙️ Cài đặt')
        sym = st.selectbox('Mã chứng khoán', SYMBOLS)
        start_time_val = st.time_input('Từ thời điểm', value=datetime.strptime('09:00', '%H:%M').time())
        minutes = st.slider('Hiển thị (số nến)', 10, 300, 300)
        alert_limit = st.slider('Số cảnh báo hiển thị', 10, 100, 30, step=5)
        rule_filter = st.selectbox(
            'Lọc theo rule',
            ['ALL', 'VWAP', 'RSI', 'VOLUME_SPIKE'],
        )
        st.divider()
        st.caption(f'🔄 Tự làm mới mỗi {REFRESH_SEC}s')
        st.caption(f'🕐 {now_str} ICT')

    # Header
    st.title('⚡ Multi-Signal Engine')
    st.markdown('<p style="color: #64748b; font-size: 1.1rem; margin-top: -15px;">Real-time Confluence Detection: VWAP · RSI · Volume Spike</p>', unsafe_allow_html=True)
    st.markdown('<br>', unsafe_allow_html=True)

    # Metrics
    summary = load_summary(ch)
    last_price = load_last_price(ch, sym)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric('📊 Candles hôm nay', f"{summary['candles']:,}")
    c2.metric('🚨 Alerts hôm nay', f"{summary['alerts']}")
    c3.metric(f'💰 Giá {sym}', f'{last_price:.2f}' if last_price else '—')
    c4.metric('⏱️ Cập nhật lúc', now_str)

    st.divider()

    # Layout chính: trái = multi-chart, phải = bảng cảnh báo
    col_chart, col_alerts = st.columns([2, 1])

    with col_chart:
        df = load_ohlc_with_indicators(ch, sym, minutes, start_time_val)
        st.plotly_chart(build_multi_chart(df, sym), use_container_width=True)

        # RSI + Volume ratio hiện tại
        if not df.empty:
            rsi_now = df['rsi'].iloc[-1] if 'rsi' in df.columns else None
            vol_avg_20 = df['quantity'].tail(21).head(20).mean()
            vol_current = df['quantity'].iloc[-1]
            vol_ratio = vol_current / vol_avg_20 if vol_avg_20 > 0 else 0

            r1, r2, r3 = st.columns(3)
            if rsi_now and not pd.isna(rsi_now):
                rsi_color = '🔴' if rsi_now > 70 else ('🟢' if rsi_now < 30 else '⚪')
                r1.metric(f'{rsi_color} RSI({RSI_PERIOD})', f'{rsi_now:.1f}')
            else:
                r1.metric(f'RSI({RSI_PERIOD})', '—')
            vol_color = '🔴' if vol_ratio >= 3.0 else '⚪'
            r2.metric(f'{vol_color} Vol Ratio', f'{vol_ratio:.1f}x')
            vwap_now = df['vwap'].iloc[-1] if 'vwap' in df.columns else None
            r3.metric('📐 VWAP', f'{vwap_now:.2f}' if vwap_now else '—')

    with col_alerts:
        st.markdown('<h3 style="margin-top: 0; padding-bottom: 10px; font-weight: 700; color: #1e293b;">🚨 Tín hiệu gần nhất</h3>', unsafe_allow_html=True)
        df_alerts = load_alerts_v2(ch, limit=alert_limit, rule_filter=rule_filter)
        if df_alerts.empty:
            st.info('Chưa có cảnh báo nào trong phiên.')
        else:
            df_alerts['price'] = df_alerts['price'].map(lambda x: f'{x:.2f}')
            df_alerts['indicator'] = df_alerts['indicator'].map(lambda x: f'{x:.2f}')
            styled = (
                df_alerts.style
                .map(style_alert_type, subset=['type'])
                .map(style_severity, subset=['severity'])
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)

    # Auto refresh
    time.sleep(REFRESH_SEC)
    st.rerun()


if __name__ == '__main__':
    main()

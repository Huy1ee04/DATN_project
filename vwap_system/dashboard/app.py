"""
Streamlit Dashboard — VWAP Alert System

Real-time visualization: price chart, VWAP lines, alert table.
Chạy: streamlit run app.py
"""

import os
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
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

# ─── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title='VWAP Alert System',
    page_icon='📈',
    layout='wide',
    initial_sidebar_state='expanded',
)
st.markdown("""
<style>
body, .stApp { background-color: #0e1117; color: #e0e0e0; }
.block-container { padding-top: 1rem; }
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

def load_ohlc_with_vwap(ch, symbol: str, minutes: int) -> pd.DataFrame:
    """Tải OHLCV (1 phút) + tính running VWAP (session) qua window function."""
    # VWAP dùng typical price = (high+low+close)/3.
    # Bands chuẩn: upper = vwap + k*sigma, lower = vwap - k*sigma.
    df = query_df(ch, f"""
        SELECT
            candle_time AS time,
            close AS price,
            volume AS quantity,
            vwap,
            sigma
        FROM
        (
            SELECT
                candle_time,
                close,
                volume,
                -- cumulative VWAP
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
                -- volume-weighted sigma of typical price:
                -- variance = E[x^2] - (E[x])^2 with weights=volume
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
        WHERE time >= now() - INTERVAL {minutes} MINUTE
        ORDER BY time ASC
        LIMIT 5000
    """, ['time', 'price', 'quantity', 'vwap', 'sigma'])
    if not df.empty:
        df['time'] = pd.to_datetime(df['time'])
    return df


def load_alerts(ch, limit: int = 50) -> pd.DataFrame:
    return query_df(ch, f"""
        SELECT alert_time, symbol, alert_type, price, vwap, deviation_pct
        FROM alerts
        ORDER BY alert_time DESC
        LIMIT {limit}
    """, ['time', 'symbol', 'type', 'price', 'vwap', 'dev_%'])


def load_summary(ch) -> dict:
    candles = ch.query("SELECT count() FROM ohlc_raw WHERE toDate(candle_time) = today()").result_rows
    alts  = ch.query("SELECT count() FROM alerts WHERE toDate(alert_time) = today()").result_rows
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

def build_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=f"{symbol} — Không có dữ liệu",
                          paper_bgcolor='#1e2130', plot_bgcolor='#161929', height=420)
        return fig

    # Volume bars (secondary y-axis)
    fig.add_trace(go.Bar(
        x=df['time'], y=df['quantity'], name='Volume',
        marker_color='rgba(100,149,237,0.25)', yaxis='y2',
    ))
    # Price line
    fig.add_trace(go.Scatter(
        x=df['time'], y=df['price'], mode='lines', name='Price',
        line=dict(color='#e8eaf6', width=1.5),
    ))
    # Session VWAP line
    fig.add_trace(go.Scatter(
        x=df['time'], y=df['vwap'], mode='lines', name='Session VWAP',
        line=dict(color='#ffa800', width=2, dash='dash'),
    ))

    # Bands theo sigma multiplier (frontend dùng cố định k=2.0 cho UI; detector dùng k từ env)
    k = float(os.getenv('BAND_SIGMA_MULTIPLIER', '2.0'))
    if 'sigma' in df.columns:
        hi = df['vwap'] + k * df['sigma']
        lo = df['vwap'] - k * df['sigma']
    else:
        # fallback: nếu query cũ
        hi = df['vwap'] * 1.015
        lo = df['vwap'] * 0.985
    fig.add_trace(go.Scatter(
        x=pd.concat([df['time'], df['time'][::-1]]),
        y=pd.concat([hi, lo[::-1]]),
        fill='toself', fillcolor='rgba(255,168,0,0.07)',
        line=dict(color='rgba(0,0,0,0)'), name=f'VWAP ±{k}σ Band',
    ))

    fig.update_layout(
        title=dict(text=f'{symbol} — Price & Session VWAP', font=dict(size=15, color='#e8eaf6')),
        paper_bgcolor='#1e2130', plot_bgcolor='#161929',
        font=dict(color='#9e9e9e'), height=430,
        xaxis=dict(gridcolor='#2d3147'),
        yaxis=dict(gridcolor='#2d3147', title='Giá (nghìn VNĐ)'),
        yaxis2=dict(overlaying='y', side='right', showgrid=False, title='Khối lượng'),
        legend=dict(bgcolor='rgba(0,0,0,0)', orientation='h', y=1.05),
        margin=dict(l=55, r=55, t=45, b=35),
    )
    return fig


# ─── Alert styling ────────────────────────────────────────────

def style_alert_type(val: str) -> str:
    if 'BREAKOUT_UP' in val:
        return 'color: #00d4aa; font-weight: bold'
    if 'BREAKDOWN' in val:
        return 'color: #ff6b6b; font-weight: bold'
    return ''


# ─── Main ─────────────────────────────────────────────────────

def main():
    ch = get_ch()
    now_str = datetime.now(ICT).strftime('%H:%M:%S')

    # Sidebar
    with st.sidebar:
        st.title('⚙️ Cài đặt')
        sym = st.selectbox('Mã chứng khoán', SYMBOLS)
        minutes = st.slider('Hiển thị (phút)', 5, 60, 30)
        alert_limit = st.slider('Số cảnh báo hiển thị', 10, 100, 30, step=5)
        st.divider()
        st.caption(f'🔄 Tự làm mới mỗi {REFRESH_SEC}s')
        st.caption(f'🕐 {now_str} ICT')

    # Header
    st.title('📈 VWAP Alert System')
    st.caption('Real-time Session VWAP Monitoring · DNSE Market Data · Đồ án Tốt nghiệp')
    st.divider()

    # Metrics
    summary = load_summary(ch)
    last_price = load_last_price(ch, sym)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric('📊 Tổng candles hôm nay', f"{summary['candles']:,}")
    c2.metric('🚨 Alerts hôm nay', f"{summary['alerts']}")
    c3.metric(f'💰 Giá {sym}', f'{last_price:.2f}' if last_price else '—')
    c4.metric('⏱️ Cập nhật lúc', now_str)

    st.divider()

    # Layout chính: trái = chart, phải = bảng cảnh báo
    col_chart, col_alerts = st.columns([2, 1])

    with col_chart:
        df = load_ohlc_with_vwap(ch, sym, minutes)
        st.plotly_chart(build_chart(df, sym), use_container_width=True)

    with col_alerts:
        st.subheader('🚨 Cảnh báo gần nhất')
        df_alerts = load_alerts(ch, limit=alert_limit)
        if df_alerts.empty:
            st.info('Chưa có cảnh báo nào trong phiên.')
        else:
            df_alerts['dev_%'] = df_alerts['dev_%'].map(lambda x: f'{x:+.2f}%')
            df_alerts['price'] = df_alerts['price'].map(lambda x: f'{x:.2f}')
            df_alerts['vwap'] = df_alerts['vwap'].map(lambda x: f'{x:.2f}')
            styled = df_alerts.style.map(style_alert_type, subset=['type'])
            st.dataframe(styled, use_container_width=True, hide_index=True)

    # Auto refresh
    time.sleep(REFRESH_SEC)
    st.rerun()


if __name__ == '__main__':
    main()

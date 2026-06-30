'use client';

import { useEffect, useState, useCallback, useRef } from 'react';
import dynamic from 'next/dynamic';
import { fetchAPI } from '@/lib/api';
import {
  Zap, Activity, AlertTriangle, DollarSign, BarChart3, Timer, Clock,
  TrendingUp, TrendingDown, CircleDot, Wifi, WifiOff, Package,
  Radio, ArrowUpRight, History, Gauge, TriangleAlert,
} from 'lucide-react';

/* Plotly — dynamic import to avoid SSR issues */
const Plot = dynamic(() => import('react-plotly.js'), { ssr: false });

/* ── Types ──────────────────────────────────────────── */

interface OHLCRow {
  time: string;
  open: number;
  high: number;
  low: number;
  price: number;
  quantity: number;
  vwap: number;
  sigma: number;
}

interface AlertRow {
  alert_time: string;
  symbol: string;
  rule_name: string;
  alert_type: string;
  severity: string;
  price: number;
  indicator_value: number;
  threshold: number;
  deviation_pct: number;
  message: string;
}

interface Summary { candles: number; alerts: number; }

interface LatencyData {
  summary: { total: number; avg_ms: number; p50_ms: number; p95_ms: number; p99_ms: number; };
  timeseries: { minute: string; msg_count: number; avg_ms: number; p95_ms: number }[];
  current: { symbol: string; candle_time: string; received_at: string; latency_ms: number } | null;
  throughput: number;
  total_today: number;
}

interface DistBucket { bucket: string; cnt: number; }

/* ── Constants ──────────────────────────────────────── */

const VN30_SYMBOLS = [
  'ACB','BCM','BID','BVH','CTG','FPT','GAS','GVR','HDB','HPG',
  'MBB','MSN','MWG','PLX','POW','SAB','SHB','SSB','SSI','STB',
  'TCB','TPB','VCB','VHM','VIB','VIC','VJC','VNM','VPB','VRE',
];
const REFRESH_SEC = 5;
const SIGMA_K = 2.0;
const RSI_PERIOD = 14;
const RULE_OPTIONS = ['ALL', 'VWAP', 'RSI', 'VOLUME_SPIKE'];
const PLOTLY_LAYOUT_BASE: Partial<Plotly.Layout> = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: '#ffffff',
  font: { color: '#64748b', family: 'Inter, sans-serif', size: 12 },
  margin: { l: 50, r: 20, t: 30, b: 30 },
  showlegend: true,
  legend: {
    bgcolor: '#ffffff',
    bordercolor: '#e2e8f0',
    borderwidth: 1,
    orientation: 'h' as const,
    y: 1.08, x: 0.5, xanchor: 'center' as const,
    font: { size: 11 },
  },
  hovermode: 'x unified' as const,
  xaxis: { gridcolor: '#f1f5f9', zeroline: false },
  yaxis: { gridcolor: '#f1f5f9', zeroline: false },
};

/* ── Helpers ────────────────────────────────────────── */

function todayStr(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function formatTime(ts: string): string {
  if (!ts) return '—';
  return new Date(ts).toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatNum(val: number | null, digits = 2): string {
  if (val == null || isNaN(val)) return '—';
  return val.toLocaleString('vi-VN', { maximumFractionDigits: digits });
}

function computeRSI(closes: number[], period = 14): (number | null)[] {
  const rsi: (number | null)[] = new Array(closes.length).fill(null);
  if (closes.length < period + 1) return rsi;
  const gains: number[] = [];
  const losses: number[] = [];
  for (let i = 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    gains.push(diff > 0 ? diff : 0);
    losses.push(diff < 0 ? -diff : 0);
  }
  let avgGain = gains.slice(0, period).reduce((a, b) => a + b, 0) / period;
  let avgLoss = losses.slice(0, period).reduce((a, b) => a + b, 0) / period;
  rsi[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  for (let i = period; i < gains.length; i++) {
    avgGain = (avgGain * (period - 1) + gains[i]) / period;
    avgLoss = (avgLoss * (period - 1) + losses[i]) / period;
    rsi[i + 1] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }
  return rsi;
}

function rollingAvg(arr: number[], window: number): (number | null)[] {
  return arr.map((_, i) => {
    if (i < window - 1) return null;
    const slice = arr.slice(i - window + 1, i + 1);
    return slice.reduce((a, b) => a + b, 0) / window;
  });
}

function severityBadge(sev: string): string {
  switch (sev) {
    case 'CRITICAL': return 'rt-badge rt-badge-red';
    case 'WARNING': return 'rt-badge rt-badge-orange';
    default: return 'rt-badge rt-badge-blue';
  }
}

/* ── Chart Builders (Plotly) ────────────────────────── */

function buildMultiChart(data: OHLCRow[], rsiValues: (number | null)[], symbol: string) {
  const times = data.map(d => d.time);
  const prices = data.map(d => d.price);
  const vwaps = data.map(d => d.vwap);
  const volumes = data.map(d => d.quantity);
  const upperBand = data.map(d => d.vwap + SIGMA_K * (d.sigma || 0));
  const lowerBand = data.map(d => d.vwap - SIGMA_K * (d.sigma || 0));
  const volAvg = rollingAvg(volumes, 20);
  const spikeRatio = 3.0;
  const volColors = volumes.map((v, i) => {
    const avg = volAvg[i];
    return avg != null && v >= avg * spikeRatio ? '#dc2626' : 'rgba(43,94,167,0.3)';
  });

  return {
    priceTraces: [
      // Price line
      { x: times, y: prices, type: 'scatter' as const, mode: 'lines' as const, name: 'Price',
        line: { color: '#1e293b', width: 2 } },
      // VWAP line
      { x: times, y: vwaps, type: 'scatter' as const, mode: 'lines' as const, name: 'VWAP',
        line: { color: '#3b82f6', width: 2.5, dash: 'dash' as const } },
      // σ-band fill
      { x: [...times, ...[...times].reverse()], y: [...upperBand, ...[...lowerBand].reverse()],
        type: 'scatter' as const, fill: 'toself' as const, fillcolor: 'rgba(43,94,167,0.08)',
        line: { color: 'rgba(0,0,0,0)' }, name: `±${SIGMA_K}σ`, hoverinfo: 'skip' as const },
    ],
    priceLayout: {
      ...PLOTLY_LAYOUT_BASE,
      height: 350,
      title: { text: `${symbol} — Price & VWAP`, font: { size: 14, color: '#1a3a6c' } },
      yaxis: { ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Giá' },
      xaxis: { ...PLOTLY_LAYOUT_BASE.xaxis, rangeslider: { visible: false } },
    },
    rsiTraces: [
      { x: times, y: rsiValues, type: 'scatter' as const, mode: 'lines' as const, name: 'RSI',
        line: { color: '#8b5cf6', width: 2 } },
    ],
    rsiLayout: {
      ...PLOTLY_LAYOUT_BASE,
      height: 200,
      title: { text: `RSI (${RSI_PERIOD})`, font: { size: 14, color: '#1a3a6c' } },
      yaxis: { ...PLOTLY_LAYOUT_BASE.yaxis, title: 'RSI', range: [0, 100] },
      shapes: [
        { type: 'line' as const, y0: 70, y1: 70, x0: 0, x1: 1, xref: 'paper' as const, line: { color: 'rgba(239,68,68,0.5)', dash: 'dash' as const } },
        { type: 'line' as const, y0: 30, y1: 30, x0: 0, x1: 1, xref: 'paper' as const, line: { color: 'rgba(16,185,129,0.5)', dash: 'dash' as const } },
        { type: 'line' as const, y0: 50, y1: 50, x0: 0, x1: 1, xref: 'paper' as const, line: { color: 'rgba(100,116,139,0.2)', dash: 'dot' as const } },
      ],
    },
    volTraces: [
      { x: times, y: volumes, type: 'bar' as const, name: 'Volume',
        marker: { color: volColors, line: { width: 0 } } },
      { x: times, y: volAvg, type: 'scatter' as const, mode: 'lines' as const, name: 'Vol Avg(20)',
        line: { color: '#f59e0b', width: 1.5, dash: 'dot' as const } },
    ],
    volLayout: {
      ...PLOTLY_LAYOUT_BASE,
      height: 200,
      title: { text: 'Volume', font: { size: 14, color: '#1a3a6c' } },
      yaxis: { ...PLOTLY_LAYOUT_BASE.yaxis, title: 'KL' },
      xaxis: { ...PLOTLY_LAYOUT_BASE.xaxis, rangeslider: { visible: true, thickness: 0.05 } },
    },
  };
}

function buildLatencyLineChart(timeseries: { minute: string; avg_ms: number; p95_ms: number }[]) {
  const minutes = timeseries.map(r => r.minute);
  return {
    traces: [
      { x: minutes, y: timeseries.map(r => r.avg_ms), type: 'scatter' as const,
        mode: 'lines+markers' as const, name: 'Avg Latency',
        line: { color: '#3b82f6', width: 2.5 }, marker: { size: 4 } },
      { x: minutes, y: timeseries.map(r => r.p95_ms), type: 'scatter' as const,
        mode: 'lines' as const, name: 'p95 Latency',
        line: { color: '#ef4444', width: 2, dash: 'dash' as const } },
    ],
    layout: {
      ...PLOTLY_LAYOUT_BASE,
      height: 350,
      title: { text: 'Latency Over Time', font: { size: 14, color: '#1a3a6c' } },
      xaxis: { ...PLOTLY_LAYOUT_BASE.xaxis, title: 'Thời gian' },
      yaxis: { ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Latency (ms)' },
    },
  };
}

function buildLatencyDistChart(distribution: DistBucket[]) {
  const bucketOrder = ['<500ms', '500–1000ms', '1000–1500ms', '1500–2000ms', '2000–3000ms', '>3000ms'];
  const colors = ['#10b981', '#34d399', '#fbbf24', '#f59e0b', '#ef4444', '#dc2626'];
  // Reindex to order
  const ordered = bucketOrder.map((b, i) => {
    const found = distribution.find(d => d.bucket === b);
    return { bucket: b, cnt: found ? found.cnt : 0, color: colors[i] };
  });
  return {
    traces: [
      { x: ordered.map(d => d.bucket), y: ordered.map(d => d.cnt), type: 'bar' as const,
        marker: { color: ordered.map(d => d.color), line: { width: 0 } },
        text: ordered.map(d => String(d.cnt)), textposition: 'outside' as const,
        textfont: { color: '#64748b' } },
    ],
    layout: {
      ...PLOTLY_LAYOUT_BASE,
      height: 350,
      title: { text: 'Latency Distribution', font: { size: 14, color: '#1a3a6c' } },
      xaxis: { ...PLOTLY_LAYOUT_BASE.xaxis, title: 'Khoảng Latency' },
      yaxis: { ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Số messages' },
      showlegend: false,
    },
  };
}

/* ── Main Page Component ───────────────────────────── */

type TabKey = 'signals' | 'latency';

export default function RealtimePage() {
  /* ── State: Controls ── */
  const [symbol, setSymbol] = useState('HPG');
  const [selectedDate, setSelectedDate] = useState(todayStr());
  const [startTime, setStartTime] = useState('09:00');
  const [candleLimit, setCandleLimit] = useState(300);
  const [alertLimit, setAlertLimit] = useState(30);
  const [ruleFilter, setRuleFilter] = useState('ALL');
  const [activeTab, setActiveTab] = useState<TabKey>('signals');

  /* ── State: Data ── */
  const [ohlcData, setOhlcData] = useState<OHLCRow[]>([]);
  const [alerts, setAlerts] = useState<AlertRow[]>([]);
  const [summary, setSummary] = useState<Summary>({ candles: 0, alerts: 0 });
  const [latencyData, setLatencyData] = useState<LatencyData | null>(null);
  const [distribution, setDistribution] = useState<DistBucket[]>([]);
  const [latencyWindow, setLatencyWindow] = useState(30);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdate, setLastUpdate] = useState('');
  const timerRef = useRef<NodeJS.Timeout | null>(null);

  const isToday = selectedDate === todayStr();

  /* ── Data Loading ── */
  const loadSignalData = useCallback(async () => {
    try {
      const dateParam = isToday ? '' : `&date=${selectedDate}`;
      const dateQ = isToday ? '' : `?date=${selectedDate}`;
      const [ohlcRes, alertsRes, summaryRes] = await Promise.all([
        fetchAPI(`/api/v1/vwap/ohlc/${symbol}?start_time=${startTime}:00&limit=${candleLimit}${dateParam}`),
        fetchAPI(`/api/v1/vwap/alerts?limit=${alertLimit}&rule=${ruleFilter}${dateParam}`),
        fetchAPI(`/api/v1/vwap/summary${dateQ}`),
      ]);
      setOhlcData(ohlcRes.data || []);
      setAlerts(alertsRes.data || []);
      setSummary(summaryRes.data || { candles: 0, alerts: 0 });
      setLastUpdate(new Date().toLocaleTimeString('vi-VN'));
      setError(null);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [symbol, selectedDate, startTime, candleLimit, alertLimit, ruleFilter, isToday]);

  const loadLatencyData = useCallback(async () => {
    try {
      const [latRes, distRes] = await Promise.all([
        fetchAPI(`/api/v1/vwap/latency?window=${latencyWindow}`),
        fetchAPI(`/api/v1/vwap/latency/distribution?window=${latencyWindow}`),
      ]);
      setLatencyData(latRes.data || null);
      setDistribution(distRes.data || []);
    } catch { /* silent */ }
  }, [latencyWindow]);

  /* ── Auto-refresh ── */
  useEffect(() => {
    setLoading(true);
    loadSignalData();
    if (activeTab === 'latency') loadLatencyData();

    // Auto-refresh only for today
    if (isToday) {
      timerRef.current = setInterval(() => {
        loadSignalData();
        if (activeTab === 'latency') loadLatencyData();
      }, REFRESH_SEC * 1000);
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, [symbol, selectedDate, startTime, candleLimit, alertLimit, ruleFilter, activeTab, latencyWindow, isToday, loadSignalData, loadLatencyData]);

  /* ── Derived Values ── */
  const closes = ohlcData.map(d => d.price);
  const rsiValues = computeRSI(closes, RSI_PERIOD);
  const latestRSI = rsiValues.filter(v => v != null).pop() ?? null;
  const latestPrice = ohlcData.length > 0 ? ohlcData[ohlcData.length - 1].price : null;
  const latestVWAP = ohlcData.length > 0 ? ohlcData[ohlcData.length - 1].vwap : null;
  const lastVol = ohlcData.length > 0 ? ohlcData[ohlcData.length - 1].quantity : 0;
  const volSlice = ohlcData.slice(-21, -1).map(d => d.quantity);
  const avgVol = volSlice.length > 0 ? volSlice.reduce((a, b) => a + b, 0) / volSlice.length : 1;
  const volRatio = avgVol > 0 ? lastVol / avgVol : 0;

  /* ── Chart data ── */
  const charts = ohlcData.length >= 2 ? buildMultiChart(ohlcData, rsiValues, symbol) : null;

  if (loading) return <div className="page"><div className="loading">Đang tải dữ liệu real-time...</div></div>;

  return (
    <div className="page">
      <h1 className="page-title"><Zap size={24} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 8 }} />Multi-Signal Engine</h1>
      <p className="page-subtitle">
        Real-time Confluence Detection: VWAP · RSI · Volume Spike
        {isToday
          ? <span className="rt-live-dot"><Wifi size={14} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} /> Live — tự làm mới mỗi {REFRESH_SEC}s</span>
          : <span className="rt-history-label"><History size={14} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} /> Xem lịch sử — {selectedDate}</span>
        }
      </p>

      {error && <div className="rt-status rt-status-error"><TriangleAlert size={16} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 6 }} />{error}</div>}

      {/* ── Controls Panel ── */}
      <div className="rt-controls">
        <div className="rt-control-group">
          <label className="rt-label">Mã CK</label>
          <select className="rt-select" value={symbol} onChange={e => setSymbol(e.target.value)}>
            {VN30_SYMBOLS.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
        <div className="rt-control-group">
          <label className="rt-label">Ngày</label>
          <input type="date" className="rt-input" value={selectedDate}
            onChange={e => setSelectedDate(e.target.value)} />
        </div>
        <div className="rt-control-group">
          <label className="rt-label">Từ giờ</label>
          <input type="time" className="rt-input" value={startTime}
            onChange={e => setStartTime(e.target.value)} />
        </div>
        <div className="rt-control-group">
          <label className="rt-label">Số nến</label>
          <input type="range" min={10} max={300} step={10} value={candleLimit}
            onChange={e => setCandleLimit(+e.target.value)} className="rt-range" />
          <span className="rt-range-val">{candleLimit}</span>
        </div>
        <div className="rt-control-group">
          <label className="rt-label">Alert limit</label>
          <input type="range" min={10} max={100} step={5} value={alertLimit}
            onChange={e => setAlertLimit(+e.target.value)} className="rt-range" />
          <span className="rt-range-val">{alertLimit}</span>
        </div>
        <div className="rt-control-group">
          <label className="rt-label">Lọc rule</label>
          <select className="rt-select" value={ruleFilter} onChange={e => setRuleFilter(e.target.value)}>
            {RULE_OPTIONS.map(r => <option key={r} value={r}>{r}</option>)}
          </select>
        </div>
        <div className="rt-control-group" style={{ marginLeft: 'auto' }}>
          <span className="rt-update-time"><Timer size={14} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />{lastUpdate || '—'}</span>
        </div>
      </div>

      {/* ── Tabs ── */}
      <div className="tabs" style={{ marginTop: 16 }}>
        <button className={`tab ${activeTab === 'signals' ? 'active' : ''}`}
          onClick={() => setActiveTab('signals')}><Activity size={15} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Signal Detection</button>
        <button className={`tab ${activeTab === 'latency' ? 'active' : ''}`}
          onClick={() => setActiveTab('latency')}><Radio size={15} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Latency Monitor</button>
      </div>

      {/* ═══════════════ Tab: Signal Detection ═══════════════ */}
      {activeTab === 'signals' && (
        <>
          {/* Metrics cards */}
          <div className="rt-metrics-row">
            <div className="rt-metric">
              <div className="rt-metric-label"><BarChart3 size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Candles</div>
              <div className="rt-metric-value">{summary.candles.toLocaleString('vi-VN')}</div>
            </div>
            <div className="rt-metric">
              <div className="rt-metric-label"><AlertTriangle size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Alerts</div>
              <div className="rt-metric-value">{summary.alerts}</div>
            </div>
            <div className="rt-metric">
              <div className="rt-metric-label"><DollarSign size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Giá {symbol}</div>
              <div className="rt-metric-value">{latestPrice ? formatNum(latestPrice) : '—'}</div>
            </div>
            <div className="rt-metric">
              <div className="rt-metric-label">
                <CircleDot size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4, color: latestRSI != null && latestRSI > 70 ? 'var(--accent-red)' : latestRSI != null && latestRSI < 30 ? 'var(--accent-green)' : 'var(--text-muted)' }} />RSI({RSI_PERIOD})
              </div>
              <div className="rt-metric-value">{latestRSI != null ? formatNum(latestRSI, 1) : '—'}</div>
            </div>
            <div className="rt-metric">
              <div className="rt-metric-label"><CircleDot size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4, color: volRatio >= 3 ? 'var(--accent-red)' : 'var(--text-muted)' }} />Vol Ratio</div>
              <div className="rt-metric-value">{formatNum(volRatio, 1)}x</div>
            </div>
            <div className="rt-metric">
              <div className="rt-metric-label"><Gauge size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />VWAP</div>
              <div className="rt-metric-value">{latestVWAP ? formatNum(latestVWAP) : '—'}</div>
            </div>
          </div>

          {/* Chart + Alerts layout */}
          <div className="rt-main-layout">
            {/* Charts column */}
            <div className="rt-charts-col">
              {charts ? (
                <>
                  <div className="rt-chart-panel">
                    <Plot data={charts.priceTraces as any} layout={charts.priceLayout as any}
                      config={{ responsive: true, displayModeBar: false }} style={{ width: '100%' }} />
                  </div>
                  <div className="rt-chart-panel">
                    <Plot data={charts.rsiTraces as any} layout={charts.rsiLayout as any}
                      config={{ responsive: true, displayModeBar: false }} style={{ width: '100%' }} />
                  </div>
                  <div className="rt-chart-panel">
                    <Plot data={charts.volTraces as any} layout={charts.volLayout as any}
                      config={{ responsive: true, displayModeBar: false }} style={{ width: '100%' }} />
                  </div>
                </>
              ) : (
                <div className="rt-chart-panel">
                  <div className="rt-chart-empty">Chưa có đủ dữ liệu nến để hiển thị biểu đồ</div>
                </div>
              )}
            </div>

            {/* Alerts column */}
            <div className="rt-alerts-col">
              <h3 className="section-title" style={{ marginTop: 0 }}><AlertTriangle size={16} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 6 }} />Tín hiệu gần nhất</h3>
              {alerts.length === 0 ? (
                <div className="empty-state">Chưa có cảnh báo trong phiên.</div>
              ) : (
                <div className="rt-alert-list">
                  {alerts.map((a, i) => (
                    <div key={i} className="rt-alert-card">
                      <div className="rt-alert-header">
                        <span className={severityBadge(a.severity)}>{a.severity}</span>
                        <span className="rt-alert-symbol">{a.symbol}</span>
                        <span className="rt-alert-time">{formatTime(a.alert_time)}</span>
                      </div>
                      <div className="rt-alert-type">{a.alert_type}</div>
                      <div className="rt-alert-msg">{a.message}</div>
                      <div className="rt-alert-meta">
                        Giá: {formatNum(a.price)} · Chỉ báo: {formatNum(a.indicator_value)}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </>
      )}

      {/* ═══════════════ Tab: Latency Monitor ═══════════════ */}
      {activeTab === 'latency' && (
        <div>
          <p className="rt-subtitle">Pipeline Health: DNSE WebSocket → Kafka → ClickHouse</p>

          {/* Latency window control */}
          <div className="rt-controls" style={{ marginBottom: 20 }}>
            <div className="rt-control-group">
              <label className="rt-label">Cửa sổ (phút)</label>
              <input type="range" min={5} max={60} step={5} value={latencyWindow}
                onChange={e => setLatencyWindow(+e.target.value)} className="rt-range" />
              <span className="rt-range-val">{latencyWindow}</span>
            </div>
          </div>

          {latencyData && (
            <>
              {/* Metrics */}
              <div className="rt-metrics-row">
                <div className="rt-metric">
                  <div className="rt-metric-label"><Zap size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Current</div>
                  <div className="rt-metric-value">
                    {latencyData.current ? `${formatNum(latencyData.current.latency_ms, 0)}ms` : '—'}
                  </div>
                </div>
                <div className="rt-metric">
                  <div className="rt-metric-label"><BarChart3 size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Avg</div>
                  <div className="rt-metric-value">{formatNum(latencyData.summary.avg_ms, 0)}ms</div>
                </div>
                <div className="rt-metric">
                  <div className="rt-metric-label"><TrendingUp size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />p95</div>
                  <div className="rt-metric-value">{formatNum(latencyData.summary.p95_ms, 0)}ms</div>
                </div>
                <div className="rt-metric">
                  <div className="rt-metric-label"><ArrowUpRight size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />p99</div>
                  <div className="rt-metric-value">{formatNum(latencyData.summary.p99_ms, 0)}ms</div>
                </div>
                <div className="rt-metric">
                  <div className="rt-metric-label"><Radio size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Msgs/s</div>
                  <div className="rt-metric-value">{formatNum(latencyData.throughput, 1)}</div>
                </div>
                <div className="rt-metric">
                  <div className="rt-metric-label"><Package size={13} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />Total</div>
                  <div className="rt-metric-value">{latencyData.total_today.toLocaleString('vi-VN')}</div>
                </div>
              </div>

              {/* Connection status */}
              {latencyData.current ? (
                <div className="rt-status rt-status-ok">
                  <Wifi size={14} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 6 }} />Kết nối hoạt động — message gần nhất [{latencyData.current.symbol}] latency {formatNum(latencyData.current.latency_ms, 0)}ms
                </div>
              ) : (
                <div className="rt-status rt-status-error">
                  <WifiOff size={14} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 6 }} />Không có dữ liệu streaming hôm nay. Kiểm tra producer + Kafka.
                </div>
              )}

              {/* Charts: Line + Distribution side by side */}
              <div className="two-col" style={{ marginTop: 24 }}>
                <div className="rt-chart-panel">
                  {latencyData.timeseries.length > 0 ? (
                    <Plot
                      data={buildLatencyLineChart(latencyData.timeseries).traces as any}
                      layout={buildLatencyLineChart(latencyData.timeseries).layout as any}
                      config={{ responsive: true, displayModeBar: false }}
                      style={{ width: '100%' }}
                    />
                  ) : (
                    <div className="rt-chart-empty">Chưa có dữ liệu latency</div>
                  )}
                </div>
                <div className="rt-chart-panel">
                  {distribution.length > 0 ? (
                    <Plot
                      data={buildLatencyDistChart(distribution).traces as any}
                      layout={buildLatencyDistChart(distribution).layout as any}
                      config={{ responsive: true, displayModeBar: false }}
                      style={{ width: '100%' }}
                    />
                  ) : (
                    <div className="rt-chart-empty">Chưa có dữ liệu distribution</div>
                  )}
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

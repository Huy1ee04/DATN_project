'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import { fetchAPI } from '@/lib/api';
import { Target, BarChart3 } from 'lucide-react';
import dynamic from 'next/dynamic';

const StockChart = dynamic(() => import('@/components/StockChart'), { ssr: false });

/* ── Types ──────────────────────────────────────────── */

interface StockDetail {
  symbol: string;
  name: string;
  sector: string;
  exchange: string;
  organ_short_name: string;
  organ_name: string;
  listing_date: string;
  issued_share: number;
  profile: string;
  type: string;
}

interface OHLCVRow {
  symbol: string;
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  total_volume: number;
  price_change_pct: number;
  sma_20: number;
  sma_50: number;
  rsi_14: number;
  macd: number;
  vwap: number;
  pe: number;
  pb: number;
  roe: number;
  eps: number;
  bvps: number;
  market_cap: number;
  dividend_yield: number;
  high_52w: number;
  low_52w: number;
  beta: number;
}

interface SignalRow {
  trade_date: string;
  signal_rsi: string;
  signal_trend: string;
  signal_macd: string;
  signal_dividend: string;
  signal_roe: string;
  signal_pe: string;
  signal_pb: string;
  signal_price_pos: string;
  label_stock_class: string;
  label_trading_action: string;
}

interface NewsRow {
  trade_date: string;
  news_title: string;
  news_short_content: string;
  news_source_link: string;
}

interface EventRow {
  trade_date: string;
  event_name_vi: string;
  event_title_vi: string;
  event_code: string;
}

/* ── Helpers ────────────────────────────────────────── */

function pctClass(val: number | null): string {
  if (!val) return 'change-neutral';
  return val > 0 ? 'change-positive' : val < 0 ? 'change-negative' : 'change-neutral';
}

function formatPct(val: number | null): string {
  if (val == null) return '—';
  return `${val > 0 ? '+' : ''}${(val * 100).toFixed(2)}%`;
}

function formatNum(val: number | null, digits: number = 2): string {
  if (val == null) return '—';
  return val.toLocaleString('vi-VN', { maximumFractionDigits: digits });
}

function formatMarketCap(val: number | null): string {
  if (val == null) return '—';
  if (val >= 1e12) return `${(val / 1e12).toFixed(1)}T`;
  if (val >= 1e9) return `${(val / 1e9).toFixed(1)}B`;
  if (val >= 1e6) return `${(val / 1e6).toFixed(1)}M`;
  return formatNum(val, 0);
}

function formatVolume(val: number | null): string {
  if (val == null) return '—';
  if (val >= 1e6) return `${(val / 1e6).toFixed(1)}M`;
  if (val >= 1e3) return `${(val / 1e3).toFixed(0)}K`;
  return val.toLocaleString('vi-VN');
}

function showSignal(signal: string | null | undefined): boolean {
  return !!signal && signal !== 'Không có';
}

function signalClass(signal: string | null): string {
  if (!signal || !showSignal(signal)) return 'badge badge-muted';
  const s = signal.toLowerCase();
  if (s.includes('tăng') || s.includes('hấp dẫn') || s.includes('rẻ') || s.includes('tốt') || s.includes('mua') || s.includes('quá bán')) return 'badge badge-green';
  if (s.includes('giảm') || s.includes('đắt') || s.includes('yếu') || s.includes('bán') || s.includes('quá mua') || s.includes('nóng')) return 'badge badge-red';
  if (s.includes('trung') || s.includes('hợp lý')) return 'badge badge-blue';
  if (s.includes('nắm') || s.includes('gần')) return 'badge badge-orange';
  return 'badge badge-muted';
}

function eventCodeColor(code: string | null): string {
  if (!code) return 'badge badge-muted';
  switch (code) {
    case 'DDIND':
    case 'DDINS': return 'badge badge-purple';
    case 'ISS': return 'badge badge-blue';
    case 'DIV': return 'badge badge-green';
    default: return 'badge badge-muted';
  }
}

type TabKey = 'overview' | 'ohlcv' | 'signals' | 'news' | 'events';

/* ── Component ──────────────────────────────────────── */

export default function StockDetailPage() {
  const params = useParams();
  const symbol = (params.symbol as string)?.toUpperCase();

  const [activeTab, setActiveTab] = useState<TabKey>('overview');
  const [detail, setDetail] = useState<StockDetail | null>(null);
  const [ohlcv, setOhlcv] = useState<OHLCVRow[]>([]);
  const [signals, setSignals] = useState<SignalRow[]>([]);
  const [news, setNews] = useState<NewsRow[]>([]);
  const [events, setEvents] = useState<EventRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [profileExpanded, setProfileExpanded] = useState(false);

  useEffect(() => {
    if (!symbol) return;
    Promise.all([
      fetchAPI(`/api/v1/stocks/${symbol}/detail`),
      fetchAPI(`/api/v1/stocks/${symbol}/ohlcv?limit=90`),
      fetchAPI(`/api/v1/stocks/${symbol}/signals?limit=10`),
      fetchAPI(`/api/v1/stocks/${symbol}/news?limit=20`),
      fetchAPI(`/api/v1/stocks/${symbol}/events?limit=20`),
    ])
      .then(([detailRes, ohlcvRes, signalRes, newsRes, eventsRes]) => {
        setDetail(detailRes.data || null);
        setOhlcv(ohlcvRes.data || []);
        setSignals(signalRes.data || []);
        setNews(newsRes.data || []);
        setEvents(eventsRes.data || []);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, [symbol]);

  if (loading) return <div className="page"><div className="loading">Đang tải dữ liệu {symbol}</div></div>;
  if (error) return <div className="page"><div className="empty-state">Lỗi: {error}</div></div>;

  const latest = ohlcv.length > 0 ? ohlcv[0] : null;
  const latestSignal = signals.length > 0 ? signals[0] : null;

  const tabs: { key: TabKey; label: string; count?: number }[] = [
    { key: 'overview', label: 'Tổng quan' },
    { key: 'ohlcv', label: 'Lịch sử GD', count: ohlcv.length },
    { key: 'signals', label: 'Tín hiệu', count: signals.length },
    { key: 'news', label: 'Tin tức', count: news.length },
    { key: 'events', label: 'Sự kiện', count: events.length },
  ];

  return (
    <div className="page">
      {/* Header */}
      <div className="detail-header">
        <span className="detail-symbol">{symbol}</span>
        {latest && (
          <>
            <span className="detail-price">{formatNum(latest.close)}</span>
            <span className={pctClass(latest.price_change_pct)} style={{ fontSize: 20, fontWeight: 600 }}>
              {formatPct(latest.price_change_pct)}
            </span>
            <span className="detail-volume">
              <BarChart3 size={14} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 3, opacity: 0.5 }} />
              KL: {formatVolume(latest.total_volume)}
            </span>
          </>
        )}
        {detail?.exchange && <span className="badge badge-muted">{detail.exchange}</span>}
        {detail?.sector && <span className="badge badge-blue">{detail.sector}</span>}
      </div>

      {detail && (
        <p className="detail-company-name">
          {detail.organ_name || detail.organ_short_name || detail.name}
        </p>
      )}

      {/* Tabs */}
      <div className="tabs">
        {tabs.map((t) => (
          <button
            key={t.key}
            className={`tab ${activeTab === t.key ? 'active' : ''}`}
            onClick={() => setActiveTab(t.key)}
          >
            {t.label}{t.count != null ? ` (${t.count})` : ''}
          </button>
        ))}
      </div>

      {/* ── Tab: Overview ──────────────────────────── */}
      {activeTab === 'overview' && (
        <>
          {/* OHLCV Chart */}
          {ohlcv.length >= 2 && (
            <StockChart
              data={ohlcv}
              title={`${symbol} — ${detail?.organ_short_name || detail?.name || ''}`}
            />
          )}

          {/* Key metrics cards with inline signals */}
          {latest && (
            <div className="metrics-grid">
              <div className="metric-item">
                <div className="metric-label">RSI (14)</div>
                <div className="metric-value">{formatNum(latest.rsi_14)}</div>
                {showSignal(latestSignal?.signal_rsi) && <span className={`metric-signal ${signalClass(latestSignal!.signal_rsi)}`}>{latestSignal!.signal_rsi}</span>}
              </div>
              <div className="metric-item">
                <div className="metric-label">MACD</div>
                <div className="metric-value">{formatNum(latest.macd, 4)}</div>
                {showSignal(latestSignal?.signal_macd) && <span className={`metric-signal ${signalClass(latestSignal!.signal_macd)}`}>{latestSignal!.signal_macd}</span>}
              </div>
              <div className="metric-item">
                <div className="metric-label">VWAP</div>
                <div className="metric-value">{formatNum(latest.vwap)}</div>
              </div>
              <div className="metric-item">
                <div className="metric-label">P/E</div>
                <div className="metric-value">{formatNum(latest.pe)}</div>
                {showSignal(latestSignal?.signal_pe) && <span className={`metric-signal ${signalClass(latestSignal!.signal_pe)}`}>{latestSignal!.signal_pe}</span>}
              </div>
              <div className="metric-item">
                <div className="metric-label">P/B</div>
                <div className="metric-value">{formatNum(latest.pb)}</div>
                {showSignal(latestSignal?.signal_pb) && <span className={`metric-signal ${signalClass(latestSignal!.signal_pb)}`}>{latestSignal!.signal_pb}</span>}
              </div>
              <div className="metric-item">
                <div className="metric-label">ROE (%)</div>
                <div className="metric-value">{formatNum(latest.roe)}</div>
                {showSignal(latestSignal?.signal_roe) && <span className={`metric-signal ${signalClass(latestSignal!.signal_roe)}`}>{latestSignal!.signal_roe}</span>}
              </div>
              <div className="metric-item">
                <div className="metric-label">EPS</div>
                <div className="metric-value">{formatNum(latest.eps, 0)}</div>
              </div>
              <div className="metric-item">
                <div className="metric-label">BVPS</div>
                <div className="metric-value">{formatNum(latest.bvps, 0)}</div>
              </div>
              <div className="metric-item">
                <div className="metric-label">Vốn hóa</div>
                <div className="metric-value">{formatMarketCap(latest.market_cap)}</div>
              </div>
              <div className="metric-item">
                <div className="metric-label">Cổ tức</div>
                <div className="metric-value">{formatPct(latest.dividend_yield)}</div>
                {showSignal(latestSignal?.signal_dividend) && <span className={`metric-signal ${signalClass(latestSignal!.signal_dividend)}`}>{latestSignal!.signal_dividend}</span>}
              </div>
              <div className="metric-item">
                <div className="metric-label">Beta</div>
                <div className="metric-value">{formatNum(latest.beta)}</div>
              </div>
              <div className="metric-item">
                <div className="metric-label">52W Range</div>
                <div className="metric-value" style={{ fontSize: 14 }}>
                  {formatNum(latest.low_52w, 0)} — {formatNum(latest.high_52w, 0)}
                </div>
                {showSignal(latestSignal?.signal_price_pos) && <span className={`metric-signal ${signalClass(latestSignal!.signal_price_pos)}`}>{latestSignal!.signal_price_pos}</span>}
              </div>
            </div>
          )}

          {/* General signals (trading action, classification, trend) */}
          {latestSignal && (
            <div className="section" style={{ marginTop: 16 }}>
              <h2 className="section-title">Tín hiệu tổng hợp — {latestSignal.trade_date}</h2>
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                {latestSignal.label_trading_action && <span className={signalClass(latestSignal.label_trading_action)}><Target size={12} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />{latestSignal.label_trading_action}</span>}
                {latestSignal.label_stock_class && <span className={signalClass(latestSignal.label_stock_class)}><BarChart3 size={12} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 4 }} />{latestSignal.label_stock_class}</span>}
                {latestSignal.signal_trend && <span className={signalClass(latestSignal.signal_trend)}>Xu hướng: {latestSignal.signal_trend}</span>}
              </div>
            </div>
          )}

          {/* Company profile */}
          {detail?.profile && (
            <div className="section">
              <h2 className="section-title">Giới thiệu công ty</h2>
              <div className={`company-profile ${profileExpanded ? 'expanded' : ''}`}>
                {detail.profile}
              </div>
              <button className="profile-toggle" onClick={() => setProfileExpanded(!profileExpanded)}>
                {profileExpanded ? '▲ Thu gọn' : '▼ Xem thêm'}
              </button>
            </div>
          )}
        </>
      )}

      {/* ── Tab: OHLCV ─────────────────────────────── */}
      {activeTab === 'ohlcv' && (
        <div className="section">
          <div className="table-wrapper">
            <table className="table">
              <thead>
                <tr>
                  <th>Ngày</th>
                  <th className="text-right">Mở cửa</th>
                  <th className="text-right">Cao</th>
                  <th className="text-right">Thấp</th>
                  <th className="text-right">Đóng cửa</th>
                  <th className="text-right">KL</th>
                  <th className="text-right">%</th>
                  <th className="text-right">SMA20</th>
                  <th className="text-right">SMA50</th>
                  <th className="text-right">RSI</th>
                  <th className="text-right">VWAP</th>
                </tr>
              </thead>
              <tbody>
                {ohlcv.map((r) => (
                  <tr key={r.trade_date}>
                    <td>{r.trade_date}</td>
                    <td className="text-right">{formatNum(r.open)}</td>
                    <td className="text-right">{formatNum(r.high)}</td>
                    <td className="text-right">{formatNum(r.low)}</td>
                    <td className="text-right" style={{ fontWeight: 600 }}>{formatNum(r.close)}</td>
                    <td className="text-right">{r.total_volume?.toLocaleString('vi-VN') || '—'}</td>
                    <td className={`text-right ${pctClass(r.price_change_pct)}`} style={{ fontWeight: 600 }}>
                      {formatPct(r.price_change_pct)}
                    </td>
                    <td className="text-right">{formatNum(r.sma_20)}</td>
                    <td className="text-right">{formatNum(r.sma_50)}</td>
                    <td className="text-right">{formatNum(r.rsi_14)}</td>
                    <td className="text-right">{formatNum(r.vwap)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Tab: Signals ────────────────────────────── */}
      {activeTab === 'signals' && (
        <div className="section">
          {signals.length === 0 ? (
            <div className="empty-state">Chưa có dữ liệu tín hiệu cho {symbol}</div>
          ) : (
            <div className="table-wrapper">
              <table className="table">
                <thead>
                  <tr>
                    <th>Ngày</th>
                    <th>Xu hướng</th>
                    <th>RSI</th>
                    <th>MACD</th>
                    <th>P/E</th>
                    <th>P/B</th>
                    <th>ROE</th>
                    <th>Cổ tức</th>
                    <th>Vị thế giá</th>
                    <th>Phân loại</th>
                    <th>Hành động</th>
                  </tr>
                </thead>
                <tbody>
                  {signals.map((s) => (
                    <tr key={s.trade_date}>
                      <td>{s.trade_date}</td>
                      <td><span className={signalClass(s.signal_trend)}>{s.signal_trend || '—'}</span></td>
                      <td><span className={signalClass(s.signal_rsi)}>{s.signal_rsi || '—'}</span></td>
                      <td><span className={signalClass(s.signal_macd)}>{s.signal_macd || '—'}</span></td>
                      <td><span className={signalClass(s.signal_pe)}>{s.signal_pe || '—'}</span></td>
                      <td><span className={signalClass(s.signal_pb)}>{s.signal_pb || '—'}</span></td>
                      <td><span className={signalClass(s.signal_roe)}>{s.signal_roe || '—'}</span></td>
                      <td><span className={signalClass(s.signal_dividend)}>{s.signal_dividend || '—'}</span></td>
                      <td><span className={signalClass(s.signal_price_pos)}>{s.signal_price_pos || '—'}</span></td>
                      <td><span className={signalClass(s.label_stock_class)}>{s.label_stock_class || '—'}</span></td>
                      <td><span className={signalClass(s.label_trading_action)}>{s.label_trading_action || '—'}</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ── Tab: News ───────────────────────────────── */}
      {activeTab === 'news' && (
        <div className="section">
          {news.length === 0 ? (
            <div className="empty-state">Chưa có tin tức cho {symbol}</div>
          ) : (
            <div className="news-list">
              {news.map((n, i) => (
                <div className="news-card" key={i}>
                  <div className="news-date">{n.trade_date}</div>
                  <div className="news-title">{n.news_title}</div>
                  {n.news_short_content && (
                    <div className="news-content">{n.news_short_content}</div>
                  )}
                  {n.news_source_link && (
                    <a href={n.news_source_link} target="_blank" rel="noopener noreferrer" className="news-link">
                      Xem chi tiết →
                    </a>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* ── Tab: Events ─────────────────────────────── */}
      {activeTab === 'events' && (
        <div className="section">
          {events.length === 0 ? (
            <div className="empty-state">Chưa có sự kiện cho {symbol}</div>
          ) : (
            <div className="event-list">
              {events.map((ev, i) => (
                <div className="event-item" key={i}>
                  <div className="event-date-col">
                    <div className="event-date">{ev.trade_date}</div>
                    {ev.event_code && (
                      <span className={eventCodeColor(ev.event_code)} style={{ marginTop: 6, display: 'inline-block' }}>
                        {ev.event_code}
                      </span>
                    )}
                  </div>
                  <div className="event-body">
                    <div className="event-type">{ev.event_name_vi || '—'}</div>
                    <div className="event-title">{ev.event_title_vi || '—'}</div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

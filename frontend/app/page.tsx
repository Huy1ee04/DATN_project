'use client';

import { useEffect, useState } from 'react';
import { fetchAPI } from '@/lib/api';
import { TrendingUp, TrendingDown } from 'lucide-react';
import dynamic from 'next/dynamic';

const MiniCandleChart = dynamic(() => import('@/components/MiniCandleChart'), { ssr: false });

interface IndexData {
  index_symbol: string;
  index_name: string;
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  total_volume: number;
  price_change_pct: number;
  signal_market_trend: string;
  signal_market_rsi: string;
}

interface MiniOHLCVRow {
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  total_volume: number;
}

interface StockMover {
  symbol: string;
  close: number;
  price_change_pct: number;
}

interface SectorMover {
  sector: string;
  price_change_pct: number;
  total_market_cap: number;
  total_trade_value: number;
}

const SECTOR_VN: Record<string, string> = {
  'Banks': 'Ngân hàng',
  'Real Estate': 'Bất động sản',
  'Food & Beverage': 'Thực phẩm & Đồ uống',
  'Financial Services': 'Dịch vụ tài chính',
  'Utilities': 'Tiện ích',
  'Travel & Leisure': 'Du lịch & Giải trí',
  'Chemicals': 'Hóa chất',
  'Basic Resources': 'Tài nguyên cơ bản',
  'Industrial Goods & Services': 'Hàng & DV công nghiệp',
  'Oil & Gas': 'Dầu khí',
  'Retail': 'Bán lẻ',
  'Construction & Materials': 'Xây dựng & Vật liệu',
  'Technology': 'Công nghệ',
  'Insurance': 'Bảo hiểm',
  'Personal & Household Goods': 'Hàng cá nhân & Gia dụng',
  'Health Care': 'Chăm sóc sức khỏe',
  'Automobiles & Parts': 'Ô tô & Phụ tùng',
  'Media': 'Truyền thông',
};

function formatMarketCap(val: number | null): string {
  if (val == null) return '—';
  if (val >= 1e12) return `${(val / 1e12).toFixed(1)}T`;
  if (val >= 1e9) return `${(val / 1e9).toFixed(1)}B`;
  return `${(val / 1e6).toFixed(0)}M`;
}

interface Overview {
  indices: IndexData[];
  top_gainers: StockMover[];
  top_losers: StockMover[];
  sector_top_gainers: SectorMover[];
  sector_top_losers: SectorMover[];
}

function signalBadgeClass(signal: string | null): string {
  if (!signal) return 'badge badge-muted';
  const s = signal.toLowerCase();
  if (s.includes('tăng mạnh') || s.includes('quá bán')) return 'badge badge-green';
  if (s.includes('tăng nhẹ')) return 'badge badge-blue';
  if (s.includes('giảm nhẹ')) return 'badge badge-orange';
  if (s.includes('giảm mạnh') || s.includes('quá mua')) return 'badge badge-red';
  return 'badge badge-muted';
}

function pctClass(val: number | null): string {
  if (!val) return 'change-neutral';
  return val > 0 ? 'change-positive' : val < 0 ? 'change-negative' : 'change-neutral';
}

function formatPct(val: number | null): string {
  if (val == null) return '—';
  return `${val > 0 ? '+' : ''}${(val * 100).toFixed(2)}%`;
}

/* Map index_symbol to the symbol used by the OHLCV API */
const INDEX_SYMBOL_MAP: Record<string, string> = {
  'VNINDEX': 'VNINDEX',
  'VN30': 'VN30',
  'HNXINDEX': 'HNXINDEX',
  'HNX30': 'HNX30',
};

export default function HomePage() {
  const [data, setData] = useState<Overview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [miniCharts, setMiniCharts] = useState<Record<string, MiniOHLCVRow[]>>({});

  useEffect(() => {
    fetchAPI('/api/v1/market/overview')
      .then((res) => {
        setData(res.data);
        setLoading(false);

        // Fetch mini OHLCV for each index (last 30 days)
        const indices = res.data?.indices || [];
        indices.forEach((idx: IndexData) => {
          const sym = INDEX_SYMBOL_MAP[idx.index_symbol] || idx.index_symbol;
          fetchAPI(`/api/v1/market/indices/${sym}/ohlcv?limit=30`)
            .then((ohlcv) => {
              setMiniCharts(prev => ({
                ...prev,
                [idx.index_symbol]: ohlcv.data || [],
              }));
            })
            .catch(() => {});
        });
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  if (loading) return <div className="page"><div className="loading">Đang tải dữ liệu</div></div>;
  if (error) return <div className="page"><div className="empty-state">Lỗi: {error}</div></div>;
  if (!data) return <div className="page"><div className="empty-state">Không có dữ liệu. Chạy loader để đưa dữ liệu lên ClickHouse.</div></div>;

  return (
    <div className="page">
      <h1 className="page-title">Tổng quan thị trường</h1>
      <p className="page-subtitle">Dữ liệu mới nhất từ ClickHouse • {data.indices?.[0]?.trade_date || ''}</p>

      {/* Index cards */}
      <div className="index-card-grid">
        {data.indices.map((idx) => {
          const pct = idx.price_change_pct || 0;
          const isUp = pct >= 0;
          // Point change = close - prev_close, derived from pct
          const pointChange = idx.close * pct / (1 + pct);
          const chartData = miniCharts[idx.index_symbol] || [];

          return (
            <div className="index-card" key={idx.index_symbol}>
              {/* Header row */}
              <div className="index-card-header">
                <span className="index-card-name">{idx.index_symbol}</span>
                <span className={`index-card-pct-badge ${isUp ? 'up' : 'down'}`}>
                  {formatPct(idx.price_change_pct)}
                </span>
              </div>

              {/* Value + point change */}
              <div className="index-card-value">
                {idx.close?.toLocaleString('vi-VN', { maximumFractionDigits: 2 })}
              </div>
              <div className={`index-card-change ${isUp ? 'change-positive' : 'change-negative'}`}>
                {isUp ? <TrendingUp size={13} style={{ verticalAlign: '-2px', marginRight: 3 }} /> :
                        <TrendingDown size={13} style={{ verticalAlign: '-2px', marginRight: 3 }} />}
                {isUp ? '+' : ''}{pointChange.toFixed(2)}
              </div>

              {/* Mini candlestick chart */}
              <div className="index-card-chart">
                <MiniCandleChart data={chartData} height={130} />
              </div>

              {/* Signals */}
              <div className="index-card-signals">
                <span className={signalBadgeClass(idx.signal_market_trend)}>
                  {idx.signal_market_trend || '—'}
                </span>
                <span className={signalBadgeClass(idx.signal_market_rsi)}>
                  {idx.signal_market_rsi || '—'}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      {/* Top gainers & losers */}
      <div className="two-col">
        <div className="section">
          <h2 className="section-title"><TrendingUp size={18} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 6, color: 'var(--accent-green)' }} />Top tăng giá</h2>
          <div className="table-wrapper">
            <table className="table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Mã</th>
                  <th className="text-right">Giá</th>
                  <th className="text-right">Thay đổi</th>
                </tr>
              </thead>
              <tbody>
                {data.top_gainers.map((s, i) => (
                  <tr key={s.symbol}>
                    <td style={{ color: 'var(--text-muted)' }}>{i + 1}</td>
                    <td><a href={`/stocks/${s.symbol}`} className="symbol-link">{s.symbol}</a></td>
                    <td className="text-right">{s.close?.toLocaleString('vi-VN')}</td>
                    <td className="text-right change-positive" style={{ fontWeight: 600 }}>{formatPct(s.price_change_pct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="section">
          <h2 className="section-title"><TrendingDown size={18} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 6, color: 'var(--accent-red)' }} />Top giảm giá</h2>
          <div className="table-wrapper">
            <table className="table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Mã</th>
                  <th className="text-right">Giá</th>
                  <th className="text-right">Thay đổi</th>
                </tr>
              </thead>
              <tbody>
                {data.top_losers.map((s, i) => (
                  <tr key={s.symbol}>
                    <td style={{ color: 'var(--text-muted)' }}>{i + 1}</td>
                    <td><a href={`/stocks/${s.symbol}`} className="symbol-link">{s.symbol}</a></td>
                    <td className="text-right">{s.close?.toLocaleString('vi-VN')}</td>
                    <td className="text-right change-negative" style={{ fontWeight: 600 }}>{formatPct(s.price_change_pct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Sector top gainers & losers */}
      <div className="two-col">
        <div className="section">
          <h2 className="section-title"><TrendingUp size={18} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 6, color: 'var(--accent-green)' }} />Ngành tăng mạnh</h2>
          <div className="table-wrapper">
            <table className="table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ngành</th>
                  <th className="text-right">Vốn hóa</th>
                  <th className="text-right">Thay đổi</th>
                </tr>
              </thead>
              <tbody>
                {(data.sector_top_gainers || []).map((s, i) => (
                  <tr key={s.sector}>
                    <td style={{ color: 'var(--text-muted)' }}>{i + 1}</td>
                    <td>{SECTOR_VN[s.sector] || s.sector}</td>
                    <td className="text-right">{formatMarketCap(s.total_market_cap)}</td>
                    <td className="text-right change-positive" style={{ fontWeight: 600 }}>{formatPct(s.price_change_pct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="section">
          <h2 className="section-title"><TrendingDown size={18} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 6, color: 'var(--accent-red)' }} />Ngành giảm mạnh</h2>
          <div className="table-wrapper">
            <table className="table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ngành</th>
                  <th className="text-right">Vốn hóa</th>
                  <th className="text-right">Thay đổi</th>
                </tr>
              </thead>
              <tbody>
                {(data.sector_top_losers || []).map((s, i) => (
                  <tr key={s.sector}>
                    <td style={{ color: 'var(--text-muted)' }}>{i + 1}</td>
                    <td>{SECTOR_VN[s.sector] || s.sector}</td>
                    <td className="text-right">{formatMarketCap(s.total_market_cap)}</td>
                    <td className="text-right change-negative" style={{ fontWeight: 600 }}>{formatPct(s.price_change_pct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}

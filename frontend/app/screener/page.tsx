'use client';

import { useEffect, useState, useCallback } from 'react';
import { fetchAPI } from '@/lib/api';

interface ScreenerRow {
  symbol: string;
  organ_short_name: string;
  sector: string;
  exchange: string;
  trade_date: string;
  close: number;
  price_change_pct: number;
  pe: number;
  pb: number;
  roe: number;
  eps: number;
  market_cap: number;
  dividend_yield: number;
  rsi_14: number;
  macd: number;
  beta: number;
  high_52w: number;
  low_52w: number;
  label_stock_class: string;
  label_trading_action: string;
}

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

function signalClass(signal: string | null): string {
  if (!signal) return 'badge badge-muted';
  const s = signal.toLowerCase();
  if (s.includes('tăng') || s.includes('mua') || s.includes('tốt') || s.includes('rẻ') || s.includes('hấp dẫn')) return 'badge badge-green';
  if (s.includes('giảm') || s.includes('bán') || s.includes('đắt') || s.includes('yếu') || s.includes('nóng')) return 'badge badge-red';
  if (s.includes('trung') || s.includes('hợp lý') || s.includes('nắm')) return 'badge badge-blue';
  return 'badge badge-muted';
}

interface SortConfig {
  key: string;
  order: 'ASC' | 'DESC';
}

const PAGE_SIZE = 50;

export default function ScreenerPage() {
  const [stocks, setStocks] = useState<ScreenerRow[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sectors, setSectors] = useState<string[]>([]);
  const [offset, setOffset] = useState(0);

  // Filters
  const [sector, setSector] = useState('');
  const [signal, setSignal] = useState('');
  const [peMin, setPeMin] = useState('');
  const [peMax, setPeMax] = useState('');
  const [pbMin, setPbMin] = useState('');
  const [pbMax, setPbMax] = useState('');
  const [rsiMin, setRsiMin] = useState('');
  const [rsiMax, setRsiMax] = useState('');

  // Sort
  const [sort, setSort] = useState<SortConfig>({ key: 'market_cap', order: 'DESC' });

  // Load sectors
  useEffect(() => {
    fetchAPI('/api/v1/stocks/sectors')
      .then((res) => setSectors(res.data || []))
      .catch(() => {});
  }, []);

  const loadData = useCallback(() => {
    setLoading(true);
    const params = new URLSearchParams();
    if (sector) params.set('sector', sector);
    if (signal) params.set('signal', signal);
    if (peMin) params.set('pe_min', peMin);
    if (peMax) params.set('pe_max', peMax);
    if (pbMin) params.set('pb_min', pbMin);
    if (pbMax) params.set('pb_max', pbMax);
    if (rsiMin) params.set('rsi_min', rsiMin);
    if (rsiMax) params.set('rsi_max', rsiMax);
    params.set('sort_by', sort.key);
    params.set('sort_order', sort.order);
    params.set('limit', String(PAGE_SIZE));
    params.set('offset', String(offset));

    fetchAPI(`/api/v1/stocks/screener?${params.toString()}`)
      .then((res) => {
        setStocks(res.data || []);
        setTotal(res.total || 0);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, [sector, signal, peMin, peMax, pbMin, pbMax, rsiMin, rsiMax, sort, offset]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const handleFilter = (e: React.FormEvent) => {
    e.preventDefault();
    setOffset(0);
    loadData();
  };

  const handleReset = () => {
    setSector('');
    setSignal('');
    setPeMin('');
    setPeMax('');
    setPbMin('');
    setPbMax('');
    setRsiMin('');
    setRsiMax('');
    setSort({ key: 'market_cap', order: 'DESC' });
    setOffset(0);
  };

  const handleSort = (key: string) => {
    setSort((prev) => ({
      key,
      order: prev.key === key && prev.order === 'DESC' ? 'ASC' : 'DESC',
    }));
    setOffset(0);
  };

  const sortIndicator = (key: string) => {
    if (sort.key !== key) return '';
    return sort.order === 'DESC' ? ' ↓' : ' ↑';
  };

  const totalPages = Math.ceil(total / PAGE_SIZE);
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1;

  return (
    <div className="page">
      <h1 className="page-title">Bộ lọc cổ phiếu</h1>
      <p className="page-subtitle">Dữ liệu kĩ thuật cổ phiếu trong ngày</p>

      {/* Filter bar */}
      <form onSubmit={handleFilter} className="filter-bar">
        <div className="filter-group">
          <label className="filter-label">Ngành</label>
          <select className="filter-select" value={sector} onChange={(e) => setSector(e.target.value)}>
            <option value="">Tất cả</option>
            {sectors.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <label className="filter-label">Tín hiệu</label>
          <select className="filter-select" value={signal} onChange={(e) => setSignal(e.target.value)}>
            <option value="">Tất cả</option>
            <option value="Mua">Mua</option>
            <option value="Nắm giữ">Nắm giữ</option>
            <option value="Bán">Bán</option>
          </select>
        </div>

        <div className="filter-group">
          <label className="filter-label">P/E</label>
          <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
            <input className="filter-input" placeholder="Min" value={peMin} onChange={(e) => setPeMin(e.target.value)} />
            <span style={{ color: 'var(--text-muted)' }}>—</span>
            <input className="filter-input" placeholder="Max" value={peMax} onChange={(e) => setPeMax(e.target.value)} />
          </div>
        </div>

        <div className="filter-group">
          <label className="filter-label">P/B</label>
          <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
            <input className="filter-input" placeholder="Min" value={pbMin} onChange={(e) => setPbMin(e.target.value)} />
            <span style={{ color: 'var(--text-muted)' }}>—</span>
            <input className="filter-input" placeholder="Max" value={pbMax} onChange={(e) => setPbMax(e.target.value)} />
          </div>
        </div>

        <div className="filter-group">
          <label className="filter-label">RSI (14)</label>
          <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
            <input className="filter-input" placeholder="Min" value={rsiMin} onChange={(e) => setRsiMin(e.target.value)} />
            <span style={{ color: 'var(--text-muted)' }}>—</span>
            <input className="filter-input" placeholder="Max" value={rsiMax} onChange={(e) => setRsiMax(e.target.value)} />
          </div>
        </div>

        <div className="filter-group" style={{ justifyContent: 'flex-end' }}>
          <div style={{ display: 'flex', gap: 8 }}>
            <button type="submit" className="btn btn-primary">Lọc</button>
            <button type="button" className="btn btn-secondary" onClick={handleReset}>Reset</button>
          </div>
        </div>
      </form>

      {error && <div className="empty-state">Lỗi: {error}</div>}

      {/* Results table */}
      <div className="table-wrapper">
        <table className="table">
          <thead>
            <tr>
              <th>#</th>
              <th className="sortable" onClick={() => handleSort('symbol')}>Mã{sortIndicator('symbol')}</th>
              <th>Tên</th>
              <th>Ngành</th>
              <th className="text-right sortable" onClick={() => handleSort('close')}>Giá{sortIndicator('close')}</th>
              <th className="text-right sortable" onClick={() => handleSort('price_change_pct')}>%{sortIndicator('price_change_pct')}</th>
              <th className="text-right sortable" onClick={() => handleSort('pe')}>P/E{sortIndicator('pe')}</th>
              <th className="text-right sortable" onClick={() => handleSort('pb')}>P/B{sortIndicator('pb')}</th>
              <th className="text-right sortable" onClick={() => handleSort('roe')}>ROE{sortIndicator('roe')}</th>
              <th className="text-right sortable" onClick={() => handleSort('rsi_14')}>RSI{sortIndicator('rsi_14')}</th>
              <th className="text-right sortable" onClick={() => handleSort('market_cap')}>Vốn hóa{sortIndicator('market_cap')}</th>
              <th>Tín hiệu</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={12} className="loading" style={{ border: 'none' }}>Đang tải</td></tr>
            ) : stocks.length === 0 ? (
              <tr><td colSpan={12} style={{ textAlign: 'center', color: 'var(--text-muted)', padding: 40 }}>Không tìm thấy cổ phiếu phù hợp</td></tr>
            ) : stocks.map((s, i) => (
              <tr key={s.symbol}>
                <td style={{ color: 'var(--text-muted)' }}>{offset + i + 1}</td>
                <td><a href={`/stocks/${s.symbol}`} className="symbol-link">{s.symbol}</a></td>
                <td style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {s.organ_short_name || '—'}
                </td>
                <td style={{ maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)', fontSize: 12 }}>
                  {s.sector || '—'}
                </td>
                <td className="text-right" style={{ fontWeight: 600 }}>{formatNum(s.close)}</td>
                <td className={`text-right ${pctClass(s.price_change_pct)}`} style={{ fontWeight: 600 }}>
                  {formatPct(s.price_change_pct)}
                </td>
                <td className="text-right">{formatNum(s.pe)}</td>
                <td className="text-right">{formatNum(s.pb)}</td>
                <td className="text-right">{formatNum(s.roe)}</td>
                <td className="text-right">{formatNum(s.rsi_14, 1)}</td>
                <td className="text-right">{formatMarketCap(s.market_cap)}</td>
                <td>
                  {s.label_trading_action && (
                    <span className={signalClass(s.label_trading_action)} style={{ marginRight: 4 }}>
                      {s.label_trading_action}
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="pagination">
          <button
            className="page-btn"
            disabled={offset === 0}
            onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
          >
            ← Trước
          </button>
          <span className="pagination-info">
            Trang {currentPage} / {totalPages}
          </span>
          <button
            className="page-btn"
            disabled={offset + PAGE_SIZE >= total}
            onClick={() => setOffset(offset + PAGE_SIZE)}
          >
            Sau →
          </button>
        </div>
      )}
    </div>
  );
}

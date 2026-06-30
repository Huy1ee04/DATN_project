'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { fetchAPI } from '@/lib/api';
import {
  TrendingUp,
  TrendingDown,
} from 'lucide-react';

/* ── Types ── */
interface SectorData {
  sector_key: number;
  sector: string;
  trade_date: string;
  price_change_pct: number;
  total_trade_value: number;
  total_market_cap: number;
  avg_pe: number;
  avg_pb: number;
  avg_eps: number;
  stock_count: number;
}

/* ── Vietnamese mapping ── */
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

/* ── Helpers ── */
function pctClass(val: number | null): string {
  if (!val) return 'change-neutral';
  return val > 0 ? 'change-positive' : val < 0 ? 'change-negative' : 'change-neutral';
}

function formatPct(val: number | null): string {
  if (val == null) return '—';
  return `${val > 0 ? '+' : ''}${(val * 100).toFixed(2)}%`;
}

function formatMarketCap(val: number | null): string {
  if (val == null) return '—';
  if (val >= 1e12) return `${(val / 1e12).toFixed(1)}T`;
  if (val >= 1e9) return `${(val / 1e9).toFixed(1)}B`;
  return `${(val / 1e6).toFixed(0)}M`;
}

function formatNum(val: number | null, digits: number = 2): string {
  if (val == null) return '—';
  return val.toLocaleString('vi-VN', { maximumFractionDigits: digits });
}

export default function SectorDataPage() {
  const router = useRouter();
  const [sectors, setSectors] = useState<SectorData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sort, setSort] = useState<{ key: string; order: 'ASC' | 'DESC' }>({ key: 'total_market_cap', order: 'DESC' });

  useEffect(() => {
    fetchAPI('/api/v1/sectors')
      .then((res) => {
        setSectors(res.data || []);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  /* ── Sorting ── */
  const handleSort = (key: string) => {
    setSort((prev) => ({
      key,
      order: prev.key === key && prev.order === 'DESC' ? 'ASC' : 'DESC',
    }));
  };

  const sortIndicator = (key: string) => {
    if (sort.key !== key) return '';
    return sort.order === 'DESC' ? ' ↓' : ' ↑';
  };

  const sorted = [...sectors].sort((a, b) => {
    const aVal = (a as any)[sort.key] ?? 0;
    const bVal = (b as any)[sort.key] ?? 0;
    return sort.order === 'DESC' ? bVal - aVal : aVal - bVal;
  });

  if (loading) return <div className="page"><div className="loading">Đang tải dữ liệu ngành</div></div>;
  if (error) return <div className="page"><div className="empty-state">Lỗi: {error}</div></div>;

  const tradeDate = sectors[0]?.trade_date || '';

  return (
    <div className="page">
      <h1 className="page-title">Dữ liệu ngành</h1>
      <p className="page-subtitle">Tổng quan biến động 18 ngành nghề • {tradeDate}</p>

      {/* Results table — same structure as screener */}
      <div className="table-wrapper">
        <table className="table">
          <thead>
            <tr>
              <th>#</th>
              <th>Ngành</th>
              <th
                className="text-right sortable"
                onClick={() => handleSort('price_change_pct')}
              >
                % Thay đổi{sortIndicator('price_change_pct')}
              </th>
              <th
                className="text-right sortable"
                onClick={() => handleSort('total_market_cap')}
              >
                Vốn hóa{sortIndicator('total_market_cap')}
              </th>
              <th
                className="text-right sortable"
                onClick={() => handleSort('total_trade_value')}
              >
                GTGD{sortIndicator('total_trade_value')}
              </th>
              <th
                className="text-right sortable"
                onClick={() => handleSort('avg_pe')}
              >
                P/E{sortIndicator('avg_pe')}
              </th>
              <th
                className="text-right sortable"
                onClick={() => handleSort('avg_pb')}
              >
                P/B{sortIndicator('avg_pb')}
              </th>
              <th
                className="text-right sortable"
                onClick={() => handleSort('avg_eps')}
              >
                EPS{sortIndicator('avg_eps')}
              </th>
              <th
                className="text-right sortable"
                onClick={() => handleSort('stock_count')}
              >
                Số CP{sortIndicator('stock_count')}
              </th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((s, i) => (
              <tr
                key={s.sector_key}
                style={{ cursor: 'pointer' }}
                onClick={() => router.push(`/sectors/${s.sector_key}`)}
              >
                <td style={{ color: 'var(--text-muted)' }}>{i + 1}</td>
                <td style={{ maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                    <span style={{ fontWeight: 600, fontSize: 13 }}>{SECTOR_VN[s.sector] || s.sector}</span>
                    <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{s.sector}</span>
                  </div>
                </td>
                <td className={`text-right ${pctClass(s.price_change_pct)}`} style={{ fontWeight: 600 }}>
                  {s.price_change_pct > 0 ? <TrendingUp size={12} style={{ verticalAlign: '-1px', marginRight: 3, display: 'inline' }} /> :
                   s.price_change_pct < 0 ? <TrendingDown size={12} style={{ verticalAlign: '-1px', marginRight: 3, display: 'inline' }} /> : null}
                  {formatPct(s.price_change_pct)}
                </td>
                <td className="text-right" style={{ fontWeight: 600 }}>{formatMarketCap(s.total_market_cap)}</td>
                <td className="text-right">{formatMarketCap(s.total_trade_value)}</td>
                <td className="text-right">{formatNum(s.avg_pe, 1)}</td>
                <td className="text-right">{formatNum(s.avg_pb)}</td>
                <td className="text-right">{formatNum(s.avg_eps, 0)}</td>
                <td className="text-right">{s.stock_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

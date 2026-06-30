'use client';

import { useEffect, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { fetchAPI } from '@/lib/api';
import dynamic from 'next/dynamic';
import {
  ArrowLeft,
  TrendingUp,
  TrendingDown,
  BarChart3,
  DollarSign,
  PieChart,
  Users,
} from 'lucide-react';

const SectorChart = dynamic(() => import('@/components/SectorChart'), { ssr: false });

/* ── Types ── */
interface SectorInfo {
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

interface StockRow {
  symbol: string;
  organ_short_name: string;
  trade_date: string;
  close: number;
  price_change_pct: number;
  market_cap: number;
  pe: number;
  pb: number;
  eps: number;
}

interface HistoryRow {
  trade_date: string;
  price_change_pct: number;
  total_trade_value: number;
  total_market_cap: number;
  avg_pe: number;
  avg_pb: number;
  avg_eps: number;
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
  'Industrial Goods & Services': 'Hàng & Dịch vụ công nghiệp',
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


export default function SectorDetailPage() {
  const params = useParams();
  const router = useRouter();
  const sectorKey = Number(params.sectorKey);

  const [info, setInfo] = useState<SectorInfo | null>(null);
  const [stocks, setStocks] = useState<StockRow[]>([]);
  const [history, setHistory] = useState<HistoryRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  /* ── Fetch data ── */
  useEffect(() => {
    if (!sectorKey) return;
    Promise.all([
      fetchAPI(`/api/v1/sectors/${sectorKey}`),
      fetchAPI(`/api/v1/sectors/${sectorKey}/history?limit=365`),
    ])
      .then(([detail, hist]) => {
        setInfo(detail.data.info);
        setStocks(detail.data.top_stocks || []);
        setHistory(hist.data || []);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, [sectorKey]);

  if (loading) return <div className="page"><div className="loading">Đang tải dữ liệu ngành</div></div>;
  if (error) return <div className="page"><div className="empty-state">Lỗi: {error}</div></div>;
  if (!info) return <div className="page"><div className="empty-state">Không tìm thấy ngành</div></div>;

  const sectorVN = SECTOR_VN[info.sector] || info.sector;

  return (
    <div className="page">
      {/* Back + Header */}
      <div className="sector-detail-header">
        <button className="sector-back-btn" onClick={() => router.push('/sectors/data')}>
          <ArrowLeft size={18} />
          <span>Dữ liệu ngành</span>
        </button>
        <div className="sector-detail-title-row">
          <h1 className="page-title">{sectorVN}</h1>
          <span className="sector-detail-en">{info.sector}</span>
          <span className={`sector-detail-pct ${pctClass(info.price_change_pct)}`}>
            {info.price_change_pct > 0 ? <TrendingUp size={16} /> : <TrendingDown size={16} />}
            {formatPct(info.price_change_pct)}
          </span>
        </div>
        <p className="page-subtitle">Cập nhật ngày {info.trade_date}</p>
      </div>

      {/* Summary cards */}
      <div className="card-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
        <div className="card">
          <div className="card-title"><DollarSign size={16} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 4 }} />Vốn hóa</div>
          <div className="card-value">{formatMarketCap(info.total_market_cap)}</div>
        </div>
        <div className="card">
          <div className="card-title"><BarChart3 size={16} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 4 }} />P/E trung bình</div>
          <div className="card-value">{info.avg_pe?.toFixed(1) ?? '—'}</div>
        </div>
        <div className="card">
          <div className="card-title"><PieChart size={16} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 4 }} />P/B trung bình</div>
          <div className="card-value">{info.avg_pb?.toFixed(2) ?? '—'}</div>
        </div>
        <div className="card">
          <div className="card-title"><Users size={16} style={{ display: 'inline', verticalAlign: '-3px', marginRight: 4 }} />Số cổ phiếu</div>
          <div className="card-value">{info.stock_count}</div>
        </div>
      </div>

      {/* Chart */}
      <div className="card" style={{ marginTop: 24 }}>
        <div className="card-title" style={{ marginBottom: 12 }}>Biểu đồ vốn hóa & khối lượng giao dịch</div>
        <SectorChart data={history} />
      </div>

      {/* Top stocks table */}
      <div className="card" style={{ marginTop: 24 }}>
        <div className="card-title" style={{ marginBottom: 16 }}>Top cổ phiếu trong ngành</div>
        <div className="table-wrapper">
          <table className="table">
            <thead>
              <tr>
                <th>Mã</th>
                <th>Tên</th>
                <th style={{ textAlign: 'right' }}>Giá</th>
                <th style={{ textAlign: 'right' }}>%Δ Giá</th>
                <th style={{ textAlign: 'right' }}>Vốn hóa</th>
                <th style={{ textAlign: 'right' }}>P/E</th>
                <th style={{ textAlign: 'right' }}>P/B</th>
              </tr>
            </thead>
            <tbody>
              {stocks.map((s) => (
                <tr
                  key={s.symbol}
                  className="table-row-clickable"
                  onClick={() => router.push(`/stocks/${s.symbol}`)}
                >
                  <td>
                    <span className="stock-symbol-badge">{s.symbol}</span>
                  </td>
                  <td>{s.organ_short_name || '—'}</td>
                  <td style={{ textAlign: 'right' }}>{formatNum(s.close, 0)}</td>
                  <td style={{ textAlign: 'right' }}>
                    <span className={pctClass(s.price_change_pct)}>{formatPct(s.price_change_pct)}</span>
                  </td>
                  <td style={{ textAlign: 'right' }}>{formatMarketCap(s.market_cap)}</td>
                  <td style={{ textAlign: 'right' }}>{s.pe?.toFixed(1) ?? '—'}</td>
                  <td style={{ textAlign: 'right' }}>{s.pb?.toFixed(2) ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

'use client';

import { useEffect, useState, useCallback } from 'react';
import { useRouter } from 'next/navigation';
import { fetchAPI } from '@/lib/api';
import { ResponsiveTreeMap } from '@nivo/treemap';
import {
  TrendingUp,
  TrendingDown,
  Building2,
} from 'lucide-react';

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

/* ── Vietnamese sector name mapping ── */
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

/* ── Color scale: price_change_pct → color ── */
function getHeatColor(pct: number): string {
  // pct is decimal: 0.01 = 1%
  const val = pct * 100; // convert to percent
  if (val >= 3)   return '#00873c';
  if (val >= 2)   return '#1a9a4a';
  if (val >= 1)   return '#2eab5a';
  if (val >= 0.5) return '#52c47a';
  if (val > 0)    return '#7dd8a0';
  if (val === 0)  return '#6b7280';
  if (val > -0.5) return '#f0918a';
  if (val > -1)   return '#e6655a';
  if (val > -2)   return '#d94032';
  if (val > -3)   return '#c22520';
  return '#a01515';
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

function formatTradeValue(val: number | null): string {
  if (val == null) return '—';
  if (val >= 1e9) return `${(val / 1e9).toFixed(1)} tỷ`;
  if (val >= 1e6) return `${(val / 1e6).toFixed(0)} tr`;
  return `${val.toLocaleString('vi-VN')}`;
}

/* ── Build tree data for nivo ── */
function buildTreeData(sectors: SectorData[]) {
  return {
    id: 'root',
    children: sectors.map((s) => ({
      id: String(s.sector_key),
      name: SECTOR_VN[s.sector] || s.sector,
      nameEn: s.sector,
      value: Math.max(s.total_market_cap, 1),
      pct: s.price_change_pct,
      marketCap: s.total_market_cap,
      tradeValue: s.total_trade_value,
      pe: s.avg_pe,
      pb: s.avg_pb,
      stockCount: s.stock_count,
      color: getHeatColor(s.price_change_pct),
    })),
  };
}

/* ── Custom node renderer ── */
interface TreeNode {
  id: string;
  x: number;
  y: number;
  width: number;
  height: number;
  data: {
    name: string;
    nameEn: string;
    pct: number;
    marketCap: number;
    tradeValue: number;
    pe: number;
    pb: number;
    stockCount: number;
    color: string;
  };
}

function CustomNode({ node, onClick }: { node: TreeNode; onClick: (id: string) => void }) {
  const { width, height, data } = node;
  const isLarge = width > 140 && height > 100;
  const isMedium = width > 90 && height > 60;

  return (
    <g transform={`translate(${node.x}, ${node.y})`}>
      <rect
        width={width}
        height={height}
        rx={4}
        fill={data.color}
        stroke="rgba(255,255,255,0.3)"
        strokeWidth={1.5}
        style={{ cursor: 'pointer', transition: 'opacity 0.15s' }}
        onClick={() => onClick(node.id)}
        onMouseOver={(e) => { (e.target as SVGRectElement).style.opacity = '0.85'; }}
        onMouseOut={(e) => { (e.target as SVGRectElement).style.opacity = '1'; }}
      />
      {/* Sector name (VN) */}
      {isMedium && (
        <text
          x={width / 2}
          y={isLarge ? height / 2 - 16 : height / 2 - 6}
          textAnchor="middle"
          dominantBaseline="central"
          fill="#ffffff"
          fontSize={isLarge ? 14 : 12}
          fontWeight={600}
          fontFamily="Inter, sans-serif"
          style={{ pointerEvents: 'none', textShadow: '0 1px 3px rgba(0,0,0,0.4)' }}
        >
          {data.name}
        </text>
      )}
      {/* Price change pct */}
      {isMedium && (
        <text
          x={width / 2}
          y={isLarge ? height / 2 + 6 : height / 2 + 10}
          textAnchor="middle"
          dominantBaseline="central"
          fill="#ffffff"
          fontSize={isLarge ? 18 : 13}
          fontWeight={700}
          fontFamily="Inter, sans-serif"
          style={{ pointerEvents: 'none', textShadow: '0 1px 3px rgba(0,0,0,0.4)' }}
        >
          {formatPct(data.pct)}
        </text>
      )}
      {/* Market cap */}
      {isLarge && (
        <text
          x={width / 2}
          y={height / 2 + 28}
          textAnchor="middle"
          dominantBaseline="central"
          fill="rgba(255,255,255,0.8)"
          fontSize={11}
          fontWeight={400}
          fontFamily="Inter, sans-serif"
          style={{ pointerEvents: 'none' }}
        >
          VH: {formatMarketCap(data.marketCap)}
        </text>
      )}
    </g>
  );
}

/* ── Tooltip ── */
function CustomTooltip({ node }: { node: { data: TreeNode['data'] } }) {
  const d = node.data;
  return (
    <div className="heatmap-tooltip">
      <div className="heatmap-tooltip-name">{d.name}</div>
      <div className="heatmap-tooltip-en">{d.nameEn}</div>
      <div className="heatmap-tooltip-grid">
        <span>% Thay đổi</span>
        <span style={{ color: d.pct >= 0 ? '#16a34a' : '#dc2626', fontWeight: 600 }}>
          {formatPct(d.pct)}
        </span>
        <span>Vốn hóa</span>
        <span>{formatMarketCap(d.marketCap)}</span>
        <span>GTGD</span>
        <span>{formatTradeValue(d.tradeValue)}</span>
        <span>P/E</span>
        <span>{d.pe?.toFixed(1) ?? '—'}</span>
        <span>P/B</span>
        <span>{d.pb?.toFixed(2) ?? '—'}</span>
        <span>Số CP</span>
        <span>{d.stockCount}</span>
      </div>
    </div>
  );
}


export default function SectorsPage() {
  const router = useRouter();
  const [sectors, setSectors] = useState<SectorData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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

  const handleNodeClick = useCallback(
    (id: string) => {
      router.push(`/sectors/${id}`);
    },
    [router]
  );

  if (loading) return <div className="page"><div className="loading">Đang tải dữ liệu ngành</div></div>;
  if (error) return <div className="page"><div className="empty-state">Lỗi: {error}</div></div>;

  const tradeDate = sectors[0]?.trade_date || '';
  const treeData = buildTreeData(sectors);
  const upCount = sectors.filter((s) => s.price_change_pct > 0).length;
  const downCount = sectors.filter((s) => s.price_change_pct < 0).length;

  return (
    <div className="page">
      <h1 className="page-title">Thông tin ngành</h1>
      <p className="page-subtitle">
        Bản đồ nhiệt 18 ngành • Kích thước theo vốn hóa • {tradeDate}
      </p>

      {/* Summary bar */}
      <div className="sector-summary-bar">
        <div className="sector-summary-item">
          <Building2 size={16} />
          <span>{sectors.length} ngành</span>
        </div>
        <div className="sector-summary-item">
          <TrendingUp size={16} />
          <span className="change-positive">{upCount} tăng</span>
        </div>
        <div className="sector-summary-item">
          <TrendingDown size={16} />
          <span className="change-negative">{downCount} giảm</span>
        </div>

        {/* Color legend */}
        <div className="heatmap-legend">
          <span className="heatmap-legend-label">-3%</span>
          <div className="heatmap-legend-bar" />
          <span className="heatmap-legend-label">+3%</span>
        </div>
      </div>

      {/* Treemap heatmap */}
      <div className="heatmap-container">
        <ResponsiveTreeMap
          data={treeData}
          identity="id"
          value="value"
          leavesOnly={true}
          innerPadding={3}
          outerPadding={3}
          tile="squarify"
          colors={(node: { data: { color: string } }) => node.data.color}
          borderWidth={0}
          enableLabel={false}
          nodeComponent={({ node }: { node: TreeNode }) => (
            <CustomNode node={node} onClick={handleNodeClick} />
          )}
          tooltip={({ node }: { node: { data: TreeNode['data'] } }) => (
            <CustomTooltip node={node} />
          )}
        />
      </div>
    </div>
  );
}

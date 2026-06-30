'use client';

import { useEffect, useState } from 'react';
import { fetchAPI } from '@/lib/api';

interface Stock {
  stock_key: number;
  symbol: string;
  name: string;
  sector: string;
  exchange: string;
  organ_short_name: string;
}

export default function StocksPage() {
  const [stocks, setStocks] = useState<Stock[]>([]);
  const [total, setTotal] = useState(0);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadStocks = (q: string) => {
    setLoading(true);
    const params = q ? `?search=${encodeURIComponent(q)}&limit=50` : '?limit=50';
    fetchAPI(`/api/v1/stocks${params}`)
      .then((res) => {
        setStocks(res.data || []);
        setTotal(res.total || 0);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  };

  useEffect(() => {
    loadStocks('');
  }, []);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    loadStocks(search);
  };

  return (
    <div className="page">
      <h1 className="page-title">Danh sách cổ phiếu</h1>
      <p className="page-subtitle">{total} cổ phiếu trên sàn</p>

      <form onSubmit={handleSearch} className="search-bar">
        <input
          type="text"
          className="search-input"
          placeholder="Tìm kiếm theo mã hoặc tên công ty..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </form>

      {error && <div className="empty-state">Lỗi: {error}</div>}

      {loading ? (
        <div className="loading">Đang tải</div>
      ) : (
        <div className="table-wrapper">
          <table className="table">
            <thead>
              <tr>
                <th>Mã</th>
                <th>Tên công ty</th>
                <th>Ngành</th>
              </tr>
            </thead>
            <tbody>
              {stocks.map((s) => (
                <tr key={s.stock_key}>
                  <td>
                    <a href={`/stocks/${s.symbol}`} className="symbol-link">
                      {s.symbol}
                    </a>
                  </td>
                  <td>{s.organ_short_name || s.name || '—'}</td>
                  <td>{s.sector || '—'}</td>
                </tr>
              ))}
              {stocks.length === 0 && (
                <tr>
                  <td colSpan={3} style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
                    Không tìm thấy cổ phiếu
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

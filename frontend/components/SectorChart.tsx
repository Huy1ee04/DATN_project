'use client';

import { useEffect, useRef } from 'react';
import {
  createChart,
  ColorType,
  CrosshairMode,
  LineStyle,
  LineSeries,
  HistogramSeries,
  type IChartApi,
  type Time,
  type LineData,
  type HistogramData,
} from 'lightweight-charts';

export interface SectorHistoryRow {
  trade_date: string;
  price_change_pct: number;
  total_trade_value: number;
  total_market_cap: number;
  avg_pe: number;
  avg_pb: number;
  avg_eps: number;
}

interface Props {
  data: SectorHistoryRow[];
  height?: number;
}

function formatVal(v: number): string {
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
  return v.toFixed(0);
}

export default function SectorChart({ data, height = 400 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || data.length < 2) return;

    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }

    const sorted = [...data].sort((a, b) => a.trade_date.localeCompare(b.trade_date));

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height,
      layout: {
        background: { type: ColorType.Solid, color: '#ffffff' },
        textColor: '#64748b',
        fontFamily: 'Inter, sans-serif',
        fontSize: 12,
      },
      grid: {
        vertLines: { color: '#f1f5f9' },
        horzLines: { color: '#f1f5f9' },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: '#cbd5e1', width: 1, style: LineStyle.Dashed, labelBackgroundColor: '#1a3a6c' },
        horzLine: { color: '#cbd5e1', width: 1, style: LineStyle.Dashed, labelBackgroundColor: '#1a3a6c' },
      },
      rightPriceScale: {
        borderColor: '#e2e8f0',
        scaleMargins: { top: 0.05, bottom: 0.25 },
      },
      timeScale: {
        borderColor: '#e2e8f0',
        timeVisible: false,
        rightOffset: 3,
        barSpacing: 8,
        fixLeftEdge: true,
        fixRightEdge: true,
      },
    });

    chartRef.current = chart;

    // ── Market cap line (scaled to trillions) ──
    const mcapSeries = chart.addSeries(LineSeries, {
      color: '#1a3a6c',
      lineWidth: 2,
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => `${price.toFixed(1)}T`,
      },
      lastValueVisible: true,
      priceLineVisible: true,
    });

    const mcapData: LineData<Time>[] = sorted.map(d => ({
      time: d.trade_date as Time,
      value: d.total_market_cap / 1e12,
    }));
    mcapSeries.setData(mcapData);

    // ── Trade value histogram (scaled to billions) ──
    const volSeries = chart.addSeries(HistogramSeries, {
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => `${price.toFixed(1)}B`,
      },
      priceScaleId: 'volume',
    });

    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.75, bottom: 0 },
    });

    const volData: HistogramData<Time>[] = sorted.map(d => ({
      time: d.trade_date as Time,
      value: d.total_trade_value / 1e9,
      color: d.price_change_pct >= 0 ? 'rgba(22, 163, 74, 0.4)' : 'rgba(220, 38, 38, 0.4)',
    }));
    volSeries.setData(volData);

    // ── Crosshair tooltip ──
    const legendEl = containerRef.current.parentElement?.querySelector('.sector-chart-legend') as HTMLElement | null;

    chart.subscribeCrosshairMove(param => {
      if (!legendEl) return;
      if (!param.time || !param.seriesData) {
        legendEl.innerHTML = '';
        return;
      }

      const mcap = param.seriesData.get(mcapSeries) as LineData<Time> | undefined;
      const vol = param.seriesData.get(volSeries) as HistogramData<Time> | undefined;

      if (!mcap) { legendEl.innerHTML = ''; return; }

      // Find the original row for pct
      const dateStr = String(param.time);
      const row = sorted.find(d => d.trade_date === dateStr);
      const pct = row?.price_change_pct ?? 0;
      const clr = pct >= 0 ? '#16a34a' : '#dc2626';

      let html = `
        <span style="color:#64748b">${dateStr}</span>
        <span>VH: <b style="color:#1a3a6c">${mcap.value.toFixed(1)}T</b></span>
        <span style="color:${clr}">${pct >= 0 ? '+' : ''}${(pct * 100).toFixed(2)}%</span>
      `;

      if (vol) {
        html += `<span style="color:#94a3b8">GTGD: ${vol.value.toFixed(1)}B</span>`;
      }

      legendEl.innerHTML = html;
    });

    // ── Resize ──
    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        chart.applyOptions({ width: entry.contentRect.width });
      }
    });
    ro.observe(containerRef.current);

    chart.timeScale().fitContent();

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, [data, height]);

  if (data.length < 2) {
    return (
      <div style={{ padding: 48, textAlign: 'center', color: '#94a3b8' }}>
        Không đủ dữ liệu để hiển thị biểu đồ
      </div>
    );
  }

  return (
    <div>
      <div className="sector-chart-legend" />
      <div ref={containerRef} style={{ width: '100%', height }} />
    </div>
  );
}

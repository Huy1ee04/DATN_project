'use client';

import { useEffect, useRef, useState } from 'react';
import {
  createChart,
  ColorType,
  CrosshairMode,
  LineStyle,
  CandlestickSeries,
  LineSeries,
  HistogramSeries,
  type IChartApi,
  type CandlestickData,
  type HistogramData,
  type LineData,
  type Time,
} from 'lightweight-charts';
import { Eye, EyeOff } from 'lucide-react';

/* ── Types ── */
export interface OHLCVChartRow {
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  total_volume: number;
  sma_20?: number | null;
  ema_12?: number | null;
  vwap?: number | null;
}

interface IndicatorConfig {
  key: string;
  label: string;
  color: string;
  visible: boolean;
  dash?: boolean;
}

interface Props {
  data: OHLCVChartRow[];
  title?: string;
  height?: number;
}

/* ── Helpers ── */
function toTime(dateStr: string): Time {
  return dateStr as Time;
}

function formatVol(v: number): string {
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
  return String(v);
}

/* ── Component ── */
export default function StockChart({ data, title, height = 420 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const seriesRefs = useRef<{
    candle?: any;
    volume?: any;
    lines: Map<string, any>;
  }>({ lines: new Map() });

  const [indicators, setIndicators] = useState<IndicatorConfig[]>([
    { key: 'sma_20', label: 'SMA20', color: '#d97706', visible: true },
    { key: 'ema_12', label: 'EMA12', color: '#2b5ea7', visible: true },
    { key: 'vwap',   label: 'VWAP',  color: '#7c3aed', visible: false, dash: true },
  ]);

  function toggleIndicator(key: string) {
    setIndicators(prev =>
      prev.map(ind => (ind.key === key ? { ...ind, visible: !ind.visible } : ind)),
    );
  }

  /* ── Build chart ── */
  useEffect(() => {
    if (!containerRef.current || data.length < 2) return;

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

    // ── Candlestick ──
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#16a34a',
      downColor: '#dc2626',
      borderUpColor: '#16a34a',
      borderDownColor: '#dc2626',
      wickUpColor: '#16a34a',
      wickDownColor: '#dc2626',
    });

    const candleData: CandlestickData<Time>[] = sorted.map(d => ({
      time: toTime(d.trade_date),
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }));
    candleSeries.setData(candleData);
    seriesRefs.current.candle = candleSeries;

    // ── Volume histogram ──
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });

    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.75, bottom: 0 },
    });

    const volData: HistogramData<Time>[] = sorted.map(d => ({
      time: toTime(d.trade_date),
      value: d.total_volume,
      color: d.close >= d.open ? 'rgba(22, 163, 74, 0.4)' : 'rgba(220, 38, 38, 0.4)',
    }));
    volumeSeries.setData(volData);
    seriesRefs.current.volume = volumeSeries;

    // ── Indicator lines ──
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const lineMap = new Map<string, any>();

    indicators.forEach(ind => {
      const lineSeries = chart.addSeries(LineSeries, {
        color: ind.color,
        lineWidth: 2,
        lineStyle: ind.dash ? LineStyle.Dashed : LineStyle.Solid,
        crosshairMarkerVisible: false,
        priceLineVisible: false,
        lastValueVisible: false,
        visible: ind.visible,
      });

      const lineData: LineData<Time>[] = sorted
        .filter(d => {
          const val = d[ind.key as keyof OHLCVChartRow] as number | null;
          return val != null && val > 0;
        })
        .map(d => ({
          time: toTime(d.trade_date),
          value: d[ind.key as keyof OHLCVChartRow] as number,
        }));

      lineSeries.setData(lineData);
      lineMap.set(ind.key, lineSeries);
    });

    seriesRefs.current.lines = lineMap;

    // ── Crosshair tooltip ──
    const legendEl = containerRef.current.parentElement?.querySelector('.chart-legend-values') as HTMLElement | null;

    chart.subscribeCrosshairMove(param => {
      if (!legendEl) return;
      if (!param.time || !param.seriesData) {
        legendEl.innerHTML = '';
        return;
      }

      const candle = param.seriesData.get(candleSeries) as CandlestickData<Time> | undefined;
      const vol = param.seriesData.get(volumeSeries) as HistogramData<Time> | undefined;

      if (!candle) { legendEl.innerHTML = ''; return; }

      const change = candle.close - candle.open;
      const changePct = candle.open ? ((change / candle.open) * 100).toFixed(2) : '0.00';
      const clr = change >= 0 ? '#16a34a' : '#dc2626';

      let html = `
        <span style="color:#64748b">${String(param.time)}</span>
        <span>O: <b>${candle.open.toFixed(2)}</b></span>
        <span>H: <b>${candle.high.toFixed(2)}</b></span>
        <span>L: <b>${candle.low.toFixed(2)}</b></span>
        <span>C: <b style="color:${clr}">${candle.close.toFixed(2)}</b></span>
        <span style="color:${clr}">${change >= 0 ? '+' : ''}${changePct}%</span>
      `;

      if (vol) {
        html += `<span style="color:#94a3b8">KL: ${formatVol(vol.value)}</span>`;
      }

      indicators.forEach(ind => {
        if (!ind.visible) return;
        const series = lineMap.get(ind.key);
        if (!series) return;
        const d = param.seriesData.get(series) as LineData<Time> | undefined;
        if (d) {
          html += `<span style="color:${ind.color}">${ind.label}: ${d.value.toFixed(2)}</span>`;
        }
      });

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
      seriesRefs.current.lines = new Map();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, height]);

  /* ── Sync toggle visibility ── */
  useEffect(() => {
    indicators.forEach(ind => {
      const series = seriesRefs.current.lines.get(ind.key);
      if (series) {
        series.applyOptions({ visible: ind.visible });
      }
    });
  }, [indicators]);

  if (data.length < 2) {
    return (
      <div className="chart-container">
        <div style={{ padding: 48, textAlign: 'center', color: '#94a3b8' }}>
          Không đủ dữ liệu để hiển thị biểu đồ
        </div>
      </div>
    );
  }

  return (
    <div className="chart-container">
      {/* Header */}
      <div className="chart-header">
        {title && <div className="chart-title">{title}</div>}
        <div className="chart-toggles">
          {indicators.map(ind => (
            <button
              key={ind.key}
              className={`chart-toggle-btn ${ind.visible ? 'active' : ''}`}
              onClick={() => toggleIndicator(ind.key)}
              title={`${ind.visible ? 'Ẩn' : 'Hiện'} ${ind.label}`}
            >
              <span className="chart-toggle-dot" style={{ background: ind.color }} />
              <span className="chart-toggle-label">{ind.label}</span>
              {ind.visible
                ? <Eye size={13} style={{ opacity: 0.7 }} />
                : <EyeOff size={13} style={{ opacity: 0.4 }} />
              }
            </button>
          ))}
        </div>
      </div>

      {/* Legend (populated by crosshair) */}
      <div className="chart-legend-values" />

      {/* Chart canvas */}
      <div ref={containerRef} className="chart-canvas" />
    </div>
  );
}

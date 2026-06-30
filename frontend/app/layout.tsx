'use client';

import './globals.css';
import { useState, useEffect, useCallback, useRef } from 'react';
import { usePathname } from 'next/navigation';
import { fetchAPI } from '@/lib/api';
import {
  LayoutDashboard,
  TrendingUp,
  SlidersHorizontal,
  Zap,
  Search,
  Clock,
  CalendarDays,
  Bell,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  PanelLeftClose,
  PanelLeftOpen,
  PieChart,
  List,
  Filter,
  Grid3x3,
  BarChart3,
} from 'lucide-react';

/* ── Types ── */
interface CalendarEvent {
  trade_date: string;
  event_name: string | null;
  is_day_off: number;
}

/* ── Navigation items ── */
interface NavItem {
  href?: string;
  icon: React.ComponentType<{ size?: number }>;
  label: string;
  children?: { href: string; icon: React.ComponentType<{ size?: number }>; label: string }[];
}

const NAV_ITEMS: NavItem[] = [
  { href: '/',         icon: LayoutDashboard,   label: 'Tổng quan' },
  {
    icon: TrendingUp,
    label: 'Cổ phiếu',
    children: [
      { href: '/stocks',   icon: List,              label: 'Danh sách cổ phiếu' },
      { href: '/screener', icon: Filter,            label: 'Bộ lọc' },
    ],
  },
  {
    icon: PieChart,
    label: 'Ngành',
    children: [
      { href: '/sectors',      icon: Grid3x3,    label: 'Biểu đồ nhiệt' },
      { href: '/sectors/data', icon: BarChart3,  label: 'Dữ liệu ngành' },
    ],
  },
  { href: '/realtime', icon: Zap,               label: 'Real-time' },
];

/* ── Helpers ── */
const MONTH_NAMES = ['Tháng 1','Tháng 2','Tháng 3','Tháng 4','Tháng 5','Tháng 6','Tháng 7','Tháng 8','Tháng 9','Tháng 10','Tháng 11','Tháng 12'];

function formatDateShort(dateStr: string): { day: string; month: string } {
  const d = new Date(dateStr);
  return {
    day: String(d.getDate()).padStart(2, '0'),
    month: `Th${d.getMonth() + 1}`,
  };
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);
  const [showCalendar, setShowCalendar] = useState(false);
  const [calendarEvents, setCalendarEvents] = useState<CalendarEvent[]>([]);
  const [calMonth, setCalMonth] = useState(new Date().getMonth() + 1);
  const [calYear, setCalYear] = useState(new Date().getFullYear());
  const [lastUpdate, setLastUpdate] = useState('');
  const calRef = useRef<HTMLDivElement>(null);
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({ 'Cổ phiếu': true, 'Ngành': true });

  function toggleGroup(label: string) {
    setExpandedGroups(prev => ({ ...prev, [label]: !prev[label] }));
  }

  /* ── Last update time ── */
  useEffect(() => {
    setLastUpdate(new Date().toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }));
  }, [pathname]);

  /* ── Calendar events ── */
  const loadCalendar = useCallback(async () => {
    try {
      const res = await fetchAPI(`/api/v1/market/calendar?month=${calMonth}&year=${calYear}`);
      setCalendarEvents(res.data || []);
    } catch { setCalendarEvents([]); }
  }, [calMonth, calYear]);

  useEffect(() => {
    if (showCalendar) loadCalendar();
  }, [showCalendar, loadCalendar]);

  /* Close calendar on outside click */
  useEffect(() => {
    if (!showCalendar) return;
    const handler = (e: MouseEvent) => {
      if (calRef.current && !calRef.current.contains(e.target as Node)) {
        setShowCalendar(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showCalendar]);

  function prevMonth() {
    if (calMonth === 1) { setCalMonth(12); setCalYear(y => y - 1); }
    else setCalMonth(m => m - 1);
  }
  function nextMonth() {
    if (calMonth === 12) { setCalMonth(1); setCalYear(y => y + 1); }
    else setCalMonth(m => m + 1);
  }

  function isActive(href: string): boolean {
    if (href === '/') return pathname === '/';
    // Exact match or match with sub-path (e.g. /stocks matches /stocks/VIC)
    return pathname === href || pathname.startsWith(href + '/');
  }

  return (
    <html lang="vi">
      <head>
        <title>TCCK — Hệ thống Thông tin Chứng khoán</title>
        <meta name="description" content="TCCK — Hệ thống tổng hợp và phân tích dữ liệu chứng khoán Việt Nam" />
      </head>
      <body>
        {/* ══ TOP BAR ══ */}
        <header className="topbar">
          <div className="topbar-left">
            <a href="/" className="topbar-logo">
              <img src="/logo_tcck_hust.png" alt="TCCK Logo" />
              <span className="topbar-logo-text">TCCK</span>
            </a>
          </div>

          <div className="topbar-right">
            {/* Search */}
            <div className="topbar-search">
              <Search size={16} className="topbar-search-icon" />
              <input type="text" placeholder="Nhập dữ liệu..." />
            </div>

            {/* Last update */}
            <button className="topbar-icon-btn" title="Cập nhật gần nhất">
              <Clock size={18} />
              <span className="topbar-tooltip">
                Cập nhật: {lastUpdate || '—'}
              </span>
            </button>

            {/* Calendar */}
            <button
              className="topbar-icon-btn"
              onClick={() => setShowCalendar(!showCalendar)}
              title="Lịch sự kiện"
            >
              <CalendarDays size={18} />
            </button>

            {/* Notification bell */}
            <button className="topbar-icon-btn" title="Thông báo">
              <Bell size={18} />
              <span className="notif-dot" />
            </button>
          </div>
        </header>

        {/* ══ CALENDAR POPUP ══ */}
        {showCalendar && (
          <div ref={calRef} className="calendar-popup">
            <div className="calendar-header">
              <span className="calendar-title">
                <CalendarDays size={16} style={{ display: 'inline', verticalAlign: '-2px', marginRight: 6 }} />
                {MONTH_NAMES[calMonth - 1]} {calYear}
              </span>
              <div className="calendar-nav">
                <button onClick={prevMonth}><ChevronLeft size={16} /></button>
                <button onClick={nextMonth}><ChevronRight size={16} /></button>
              </div>
            </div>
            <div className="calendar-list">
              {calendarEvents.length === 0 ? (
                <div className="calendar-empty">
                  Không có sự kiện trong {MONTH_NAMES[calMonth - 1].toLowerCase()}
                </div>
              ) : (
                calendarEvents.map((ev, i) => {
                  const { day, month } = formatDateShort(ev.trade_date);
                  return (
                    <div key={i} className="calendar-event">
                      <div className="calendar-event-date">
                        <div className="calendar-event-day">{day}</div>
                        <div className="calendar-event-month">{month}</div>
                      </div>
                      <div className="calendar-event-info">
                        <div className="calendar-event-name">
                          {ev.event_name || (ev.is_day_off ? 'Nghỉ giao dịch' : 'Ngày giao dịch bình thường')}
                        </div>
                        {ev.is_day_off === 1 && (
                          <div className="calendar-event-dayoff">Nghỉ lễ</div>
                        )}
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        )}

        {/* ══ SIDEBAR ══ */}
        <aside className={`sidebar ${collapsed ? 'collapsed' : ''}`}>
          <nav className="sidebar-menu">
            {NAV_ITEMS.map((item) => {
              const Icon = item.icon;

              /* ── Item with children (expandable group) ── */
              if (item.children) {
                const isGroupActive = item.children.some(c => isActive(c.href));
                const isExpanded = expandedGroups[item.label] ?? false;

                return (
                  <div key={item.label} className="sidebar-group">
                    <button
                      className={`sidebar-link sidebar-group-toggle ${isGroupActive ? 'active' : ''}`}
                      onClick={() => toggleGroup(item.label)}
                    >
                      <span className="sidebar-link-icon"><Icon size={20} /></span>
                      <span className="sidebar-link-text">{item.label}</span>
                      <ChevronDown
                        size={14}
                        className={`sidebar-chevron ${isExpanded ? 'expanded' : ''}`}
                      />
                    </button>
                    <div className={`sidebar-children ${isExpanded && !collapsed ? 'open' : ''}`}>
                      <div className="sidebar-children-inner">
                        {item.children.map(child => {
                          const ChildIcon = child.icon;
                          return (
                            <a
                              key={child.href}
                              href={child.href}
                              className={`sidebar-link sidebar-child ${isActive(child.href) ? 'active' : ''}`}
                            >
                              <span className="sidebar-link-icon"><ChildIcon size={16} /></span>
                              <span className="sidebar-link-text">{child.label}</span>
                            </a>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                );
              }

              /* ── Simple link ── */
              return (
                <a
                  key={item.href}
                  href={item.href}
                  className={`sidebar-link ${isActive(item.href!) ? 'active' : ''}`}
                >
                  <span className="sidebar-link-icon"><Icon size={20} /></span>
                  <span className="sidebar-link-text">{item.label}</span>
                </a>
              );
            })}
          </nav>
          <div className="sidebar-toggle">
            <button
              className="sidebar-toggle-btn"
              onClick={() => setCollapsed(!collapsed)}
              title={collapsed ? 'Mở rộng' : 'Thu gọn'}
            >
              {collapsed ? <PanelLeftOpen size={16} /> : <PanelLeftClose size={16} />}
            </button>
          </div>
        </aside>

        {/* ══ MAIN CONTENT ══ */}
        <main className={`main-area ${collapsed ? 'sidebar-collapsed' : ''}`}>
          <div className="container">
            {children}
          </div>
        </main>
      </body>
    </html>
  );
}

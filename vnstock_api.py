
# from vnstock_data import Insights

# ins = Insights()

# # Xem danh sách tất cả tiêu chí
# criteria_df = ins.screener().criteria(lang="vi")

# # Lấy dữ liệu screener toàn thị trường
# df_all = ins.screener().filter()
# print(df_all)

from pathlib import Path

from vnstock_data import Reference

OUTPUT_FILE = Path(__file__).resolve().parent / "data3.txt"

ref = Reference()
events = ref.company("TCB").news().info()
print(events)

OUTPUT_FILE.write_text(events.to_string(index=False), encoding="utf-8")
print(f"Saved to {OUTPUT_FILE}")





# from vnstock_data import Market
# from pathlib import Path


# OUTPUT_FILE = Path(__file__).resolve().parent / "summary.txt"

# mkt = Market()

# # # Giá OHLCV lịch sử
# # df_ohlc = mkt.equity("VIC") \
# #               .ohlcv(start="2026-02-01", end="2026-03-01")

# # # Lệnh giao dịch chi tiết trong ngày
# # df_trades = mkt.equity("TCB").trades()

# # # Cấp độ mua/bán
# # df_orderbook = mkt.equity("VNM").order_book()

# # # Bảng giá
# # quote = mkt.equity("HPG").quote()

# # # Dòng tiền nước ngoài
# # foreign = mkt.equity("VIC").foreign_flow()

# # # Dòng tiền tự doanh
# # proprietary = mkt.equity("VIC").proprietary_flow()

# # # Giao dịch thỏa thuận
# # blocks = mkt.equity("VIC").block_trades()

# # # Giao dịch lô lẻ
# # odd = mkt.equity("HPG").odd_lot()

# # # Phân bố khối lượng theo giá
# # vol_profile = mkt.equity("VJC").volume_profile()

# # Tổng hợp thông tin
# summary = mkt.equity("TCB").summary()
# print(summary)

# # OUTPUT_FILE.write_text(summary.to_string(index=False), encoding="utf-8")
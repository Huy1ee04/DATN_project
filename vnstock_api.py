
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

# OUTPUT_FILE.write_text(events.to_string(index=False), encoding="utf-8")
# print(f"Saved to {OUTPUT_FILE}")
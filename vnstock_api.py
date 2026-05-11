
from vnstock_data import Insights

ins = Insights()

# Xem danh sách tất cả tiêu chí
criteria_df = ins.screener().criteria(lang="vi")

# Lấy dữ liệu screener toàn thị trường
df_all = ins.screener().filter()
print(df_all)
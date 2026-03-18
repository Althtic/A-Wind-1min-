import pandas as pd
from scipy.stats import percentileofscore
pd.set_option('display.max_columns', None)
c_bond_data = pd.read_csv(r"C:\Users\63585\Desktop\PycharmProjects\pythonProject\C_Bond\核心指标计算\clean_data.csv")

# 每分钟转股价值： (100 / 转股价) * 正股价
value_conv_to_stock = (100 / c_bond_data['bond_convprice']) * c_bond_data['Stock_Close']
c_bond_data['value_conv_to_stock'] = value_conv_to_stock

# 每分钟转股溢价率： (可转债价格 - 转股价值) / 转股价值 * 100
premium_rate = (c_bond_data['Bond_Close'] - c_bond_data['value_conv_to_stock']) / c_bond_data['value_conv_to_stock'] * 100
c_bond_data['premium_rate'] = premium_rate

# 纵向排名：当前转股溢价率在过去window分钟窗口内所处的分位数(0-100，如5表示比95%历史值都低)
WINDOW = 3
percentile_premium_rate = c_bond_data.groupby('Bond_Code')['premium_rate'].rolling(window=WINDOW).apply(
    lambda x: percentileofscore(x, x.iloc[-1], kind='rank')
).droplevel(0)
c_bond_data['percentile_premium_rate'] = percentile_premium_rate

# 横向排名：同一分钟内当前可转债溢价率所处的排名分位数(rank值越小，转股溢价率截面排名越低)
rank_percentile_premium_rate = c_bond_data.groupby('Time')['premium_rate'].rank(pct=True)
c_bond_data['rank_percentile_premium_rate'] = rank_percentile_premium_rate

c_bond_data.to_csv(r"C:\Users\63585\Desktop\PycharmProjects\pythonProject\C_Bond\策略信号\calculate_ratio_data.csv", index=False)

# print(c_bond_data[(c_bond_data['Time'] == '2025-11-11 13:58:00') & (c_bond_data['Bond_Code'] == '110062.SH')])
# print(c_bond_data[(c_bond_data['Time'] == '2025-11-11 13:59:00') & (c_bond_data['Bond_Code'] == '110062.SH')])
# print(c_bond_data[(c_bond_data['Time'] == '2025-11-11 14:00:00') & (c_bond_data['Bond_Code'] == '110062.SH')])
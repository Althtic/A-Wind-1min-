from pathlib import Path

import numpy as np
import pandas as pd

pd.set_option('display.max_columns', None)

script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
input_path = script_dir / "clean_data.csv"
output_path = project_dir / "策略信号" / "calculate_ratio_data.csv"

c_bond_data = pd.read_csv(input_path)
c_bond_data["Time"] = pd.to_datetime(c_bond_data["Time"], errors="coerce")
c_bond_data["Date"] = pd.to_datetime(c_bond_data["Date"], errors="coerce").dt.normalize()
c_bond_data = c_bond_data.sort_values(["Date", "Bond_Code", "Time"]).reset_index(drop=True)

valid_conversion = (
    c_bond_data["bond_convprice"].notna()
    & c_bond_data["Stock_Close"].notna()
    & c_bond_data["Bond_Close"].notna()
    & (c_bond_data["bond_convprice"] > 0)
    & (c_bond_data["Stock_Close"] > 0)
)

c_bond_data["value_conv_to_stock"] = np.nan
c_bond_data.loc[valid_conversion, "value_conv_to_stock"] = (
    100 / c_bond_data.loc[valid_conversion, "bond_convprice"]
) * c_bond_data.loc[valid_conversion, "Stock_Close"]

# 每分钟转股溢价率： (可转债价格 - 转股价值) / 转股价值 * 100
c_bond_data["premium_rate"] = np.nan
c_bond_data.loc[valid_conversion, "premium_rate"] = (
    c_bond_data.loc[valid_conversion, "Bond_Close"]
    - c_bond_data.loc[valid_conversion, "value_conv_to_stock"]
) / c_bond_data.loc[valid_conversion, "value_conv_to_stock"] * 100

# 纵向排名：当前转股溢价率在过去window分钟窗口内所处的分位数(0-100，如5表示比95%历史值都低)
WINDOW = 240
c_bond_data["percentile_premium_rate"] = (
    c_bond_data
    .groupby("Bond_Code", group_keys=False)["premium_rate"]
    .rolling(window=WINDOW, min_periods=WINDOW)
    .apply(lambda x: x.rank(pct=True).iloc[-1] * 100, raw=False)
    .reset_index(level=0, drop=True)
)

# 横向排名：同一分钟内当前可转债溢价率所处的排名分位数(rank值越小，转股溢价率截面排名越低)
rank_percentile_premium_rate = c_bond_data.groupby("Time")["premium_rate"].rank(pct=True)
c_bond_data["rank_percentile_premium_rate"] = rank_percentile_premium_rate

output_path.parent.mkdir(parents=True, exist_ok=True)
c_bond_data.to_csv(output_path, index=False, encoding="utf-8-sig")

# print(c_bond_data[(c_bond_data['Time'] == '2025-11-11 13:58:00') & (c_bond_data['Bond_Code'] == '110062.SH')])
# print(c_bond_data[(c_bond_data['Time'] == '2025-11-11 13:59:00') & (c_bond_data['Bond_Code'] == '110062.SH')])
# print(c_bond_data[(c_bond_data['Time'] == '2025-11-11 14:00:00') & (c_bond_data['Bond_Code'] == '110062.SH')])

import pandas as pd
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

script_dir = Path(__file__).resolve().parent
data = pd.read_csv(script_dir / "signal_buy_and_sell_v2.csv")

cols_to_show = ['Date','Time', 'Bond_Code', 'Bond_Close', 'value_conv_to_stock', 'premium_rate', 'percentile_premium_rate', 'rank_percentile_premium_rate', 'signal_buy', 'signal_sell']
test = (data[(data['Date'] == '2025-09-01') & (data['Bond_Code'] == '123162.SZ')][cols_to_show])
test.to_excel(script_dir / "signal_test.xlsx", index=False)

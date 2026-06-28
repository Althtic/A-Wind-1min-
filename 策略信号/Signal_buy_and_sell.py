from pathlib import Path

import pandas as pd 
import numpy as np

# 设置显示选项
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

script_dir = Path(__file__).resolve().parent
input_path = script_dir / "calculate_ratio_data.csv"
output_path = script_dir / "signal_buy_and_sell_v2.csv"

# 读取数据
data = pd.read_csv(input_path)
data["Time"] = pd.to_datetime(data["Time"], errors="coerce")
data["Date"] = pd.to_datetime(data["Date"], errors="coerce").dt.normalize()
data = data.sort_values(by=["Date", "Bond_Code", "Time"]).reset_index(drop=True)

# 参数配置
M_PERCENTILE_THRESHOLD = 100
PRICE_PROFIT_RATE = 0.01
PRICE_STOP_LOSS_RATE = 0.005
FORCE_SELL_TIME = '14:57:00'
NO_BUY_AFTER_TIME = '14:57:00' # 禁止开仓时间(即使有买入信号也不买)

# --- 第一步：预处理时间列 ---
data["TimeOnly"] = data["Time"].dt.strftime("%H:%M:%S")

# --- 第二步：标记不可交易状态 (-2) ---
data['signal_buy'] = 0
cond_vol = (data['Bond_Volume'] == 0)
cond_time = data['TimeOnly'] > FORCE_SELL_TIME # 大于 14:57:00 不可交易（用于买入限制）
# 注意：这里对于买入信号，我们通常认为 14:57:00 这一刻还可以买吗？不建议。
# 所以买入限制应该包括 14:57:00。
cond_no_buy = data['TimeOnly'] >= NO_BUY_AFTER_TIME 

# 标记全局不可交易状态(标记为-2)
data.loc[cond_vol | cond_time, 'trade_status'] = -2 
data.loc[~(cond_vol | cond_time), 'trade_status'] = 0

# --- 第三步：生成买入信号 (1) ---
# 条件：溢价率低 + 排名低 + 当前不是不可交易状态
buy_in_cond = (
    (data['percentile_premium_rate'] < 5) & 
    (data['rank_percentile_premium_rate'] < 0.05) & 
    (data['trade_status'] == 0) &   
    (data['TimeOnly'] < NO_BUY_AFTER_TIME) # 【关键】禁止在 14:57:00 及之后开新仓
)

# 移位：今天看到的信号，明天（下一分钟）执行买入
# shift(1) 意味着：第 i 行的条件满足 -> 第 i+1 行的 signal_buy = 1
data['raw_buy_signal'] = (
    buy_in_cond
    .groupby([data['Date'], data['Bond_Code']])
    .shift(1, fill_value=False)
    .astype(bool)
)
data.loc[data['TimeOnly'] >= NO_BUY_AFTER_TIME, 'raw_buy_signal'] = False
data.loc[data['trade_status'] != 0, 'raw_buy_signal'] = False

# --- 第四步：生成实际买卖信号 ---
def generate_trade_signals(group):
    """
    对单只债券的单日时间序列进行处理，生成实际成交的买入和卖出信号。
    买入/卖出状态都在这里维护，避免把持仓期间的候选买入误记为实际买入。
    """
    n = len(group)
    buy_signal = np.zeros(n, dtype=int)
    sell_signal = np.zeros(n, dtype=int) # 1 表示卖出
    
    in_position = False
    buy_price = 0.0
    
    # 【新增】挂起卖出标志：True 表示上一分钟触发了条件，本分钟必须执行卖出
    pending_sell = False 
    
    times = group['TimeOnly'].values      # 使用预处理好的 HH:MM:SS
    prices = group['Bond_Close'].values 
    premium_percents = group['percentile_premium_rate'].values
    buy_candidates = group['raw_buy_signal'].values
    
    for i in range(n):
        current_time = times[i]
        current_price = prices[i]
        current_premium = premium_percents[i]
        is_last_row = i == n - 1
        
        # ---------------------------------------------------------
        # 优先级 1: 执行挂起的卖出指令 (上一分钟触发的条件，今天落地)
        # ---------------------------------------------------------
        if in_position and pending_sell:
            sell_signal[i] = 1
            in_position = False
            pending_sell = False # 重置标志
            # 执行完卖出后，本分钟不能再做其他操作（不能又买又卖，虽然逻辑上也不太可能）
            continue
            
        # ---------------------------------------------------------
        # 优先级 2: 处理买入逻辑
        # ---------------------------------------------------------
        if not in_position and buy_candidates[i]:
            in_position = True
            buy_price = current_price
            buy_signal[i] = 1
            pending_sell = False # 确保重置
            continue # 买入后跳过后续判断，且本分钟不能卖出
            
        if not in_position:
            continue
            
        # ---------------------------------------------------------
        # 优先级 3: 检查是否触发新的卖出条件 (仅标记，不执行)
        # ---------------------------------------------------------
        
        # 条件 A: 时间止损 (强制平仓)
        # 特殊处理：如果是 14:57:00 触发，由于马上收盘，我们通常假设能在 14:57:00 这一分钟的最后时刻成交
        # 或者严格一点，也放到下一分钟？
        # 实际交易中，14:57:00 是最后通牒。如果 14:56:00 的数据让你决定 14:57:00 卖，
        # 那么 14:57:00 这一分钟你是可以操作的。
        # 这里的逻辑：如果在 14:57:00 这一分钟检测到时间到了，必须卖。
        # 但为了统一逻辑，我们可以认为：
        # 如果 current_time >= FORCE_SELL_TIME，这是硬性规定，必须在本分钟结束前清仓。
        # 所以这里可以直接执行卖出，不需要等到下一分钟（因为再等就收盘了）。
        if current_time >= FORCE_SELL_TIME:
            sell_signal[i] = 1
            in_position = False
            pending_sell = False
            continue

        # 如果该债当天数据提前结束，最后一条可用分钟也必须日内平仓。
        if is_last_row:
            sell_signal[i] = 1
            in_position = False
            pending_sell = False
            continue

        # 条件 B: 止盈信号 (触发挂起)
        cond_premium_rebound = current_premium > M_PERCENTILE_THRESHOLD
        cond_price_profit = (current_price - buy_price) / buy_price >= PRICE_PROFIT_RATE
        
        if cond_premium_rebound or cond_price_profit:
            pending_sell = True # 标记：下一分钟卖
            continue # 本分钟不卖，继续持有到下一分钟
            
        # 条件 C: 止损信号 (触发挂起)
        cond_price_loss = (current_price - buy_price) / buy_price <= -PRICE_STOP_LOSS_RATE
        if cond_price_loss:
            pending_sell = True # 标记：下一分钟卖
            continue 
            
    return pd.DataFrame(
        {"signal_buy": buy_signal, "signal_sell": sell_signal},
        index=group.index,
    )

# 应用买卖逻辑
signals = (
    data
    .groupby(['Date', 'Bond_Code'], group_keys=False)
    .apply(generate_trade_signals, include_groups=False)
    .sort_index()
)
data[['signal_buy', 'signal_sell']] = signals[['signal_buy', 'signal_sell']]

# --- 第五步：验证与输出 ---
# 筛选出有买卖操作的行进行预览
trades = data[(data['signal_buy'] == 1) | (data['signal_sell'] == 1)]

print("\n=== 交易信号预览 (前 20 行) ===")
cols = ['Date', 'Bond_Code', 'Time', 'Bond_Close', 'signal_buy', 'signal_sell', 'trade_status', 'percentile_premium_rate']
print(trades[cols].head(20))

# 检查是否有“同分钟买卖”的异常情况 (理论上不应该出现，因为买入后 continue 了)
same_minute_trade = data[(data['signal_buy'] == 1) & (data['signal_sell'] == 1)]
if not same_minute_trade.empty:
    print("\n⚠️ 警告：发现同分钟既买入又卖出的异常数据！")
    print(same_minute_trade[cols])
else:
    print("\n✅ 检查通过：没有同分钟既买入又卖出的情况（符合 T+0 下一分钟卖出逻辑）。")

# 保存结果
data = data.drop(columns=["TimeOnly", "raw_buy_signal"])
data.to_csv(output_path, index=False, encoding="utf-8-sig")
print("\n文件已保存。")

# 可转债分钟级信号与回测框架

本项目用于可转债分钟级数据清洗、转股溢价率信号生成，以及基于已有信号的 T+0 回测分析。

## 环境要求

- Python 3.9+
- 依赖见 `requirements.txt`

安装依赖：

```bash
pip install -r requirements.txt
```

## 数据放置

默认原始数据目录为：

```text
c_bond_data_1217/
  2025-09-01/
    110059.SH.csv
    ...
    merged_2025-09-01.xlsx
  2025-09-02/
    ...
```

每个日期文件夹应包含：

- 单只可转债分钟行情 CSV，例如 `110059.SH.csv`、`123187.SZ.csv`
- 当天转股价合并表 `merged_*.xlsx`

脚本会从这些文件中生成清洗数据、指标数据和信号数据。

## 一键生成数据与信号

从原始数据开始完整运行：

```bash
python3 run_data_and_signal_pipeline.py
```

如果原始合并数据 `数据清洗与预处理/concat_data.csv` 已经存在，只想重跑清洗、指标和信号：

```bash
python3 run_data_and_signal_pipeline.py --skip-merge
```

主要输出：

```text
数据清洗与预处理/concat_data.csv
核心指标计算/clean_data.csv
策略信号/calculate_ratio_data.csv
策略信号/signal_buy_and_sell_v2.csv
```

## 运行回测

回测默认读取：

```text
策略信号/signal_buy_and_sell_v2.csv
```

运行：

```bash
python3 回测框架/run_backtest.py
```

可调参数示例：

```bash
python3 回测框架/run_backtest.py \
  --initial-cash 500000 \
  --order-cash 10000 \
  --price-mode vwap \
  --buy-fee-rate 0.0001 \
  --sell-fee-rate 0.0001 \
  --slippage-rate 0
```

默认设置：

- 初始资金：`500000`
- 单笔买入金额：`10000`
- 买入手续费：万分之一
- 卖出手续费：万分之一
- 滑点：`0`
- 成交价：默认 `vwap`

其中可转债 VWAP 估算为：

```text
Bond_Amount / (Bond_Volume * 10)
```

如果 VWAP 无法计算或异常，会回退到 `Bond_Close`。

## 回测输出

输出目录：

```text
回测框架/outputs/
```

包含：

- `executions.csv`：所有买卖成交记录
- `trades.csv`：买卖配对后的逐笔交易
- `equity_curve.csv`：分钟级权益曲线
- `daily_summary.csv`：每日收益和交易统计
- `performance_summary.csv`：绩效摘要
- `performance_summary.json`：绩效摘要 JSON
- `skipped_orders.csv`：未成交订单
- `equity_curve.png`：净值曲线、日度回撤和关键指标图

## 项目结构

```text
.
├── run_data_and_signal_pipeline.py
├── requirements.txt
├── 数据清洗与预处理/
│   ├── DataConcat.py
│   └── DataClean.py
├── 核心指标计算/
│   └── calculate_ratio.py
├── 策略信号/
│   ├── Signal_buy_and_sell.py
│   └── signal_test.py
└── 回测框架/
    ├── config.py
    ├── backtest.py
    └── run_backtest.py
```

## 注意

- `c_bond_data_1217/`、中间 CSV 和回测输出默认在 `.gitignore` 中忽略，避免把大体量数据和生成结果提交到 GitHub。
- 如果你希望 GitHub 仓库自带样例数据，可以单独准备一个小型 `sample_data/`，再在 README 中增加样例运行命令。

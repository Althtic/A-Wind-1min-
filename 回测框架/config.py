from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent.parent


@dataclass
class BacktestConfig:
    signal_path: Path = PROJECT_DIR / "策略信号" / "signal_buy_and_sell_v2.csv"
    output_dir: Path = PROJECT_DIR / "回测框架" / "outputs"

    initial_cash: float = 500_000.0
    order_cash: float = 10_000.0
    buy_fee_rate: float = 0.0001
    sell_fee_rate: float = 0.0001
    slippage_rate: float = 0.0

    # "vwap" uses Bond_Amount / (Bond_Volume * volume_multiplier), falling back to close.
    # "close" uses Bond_Close directly.
    price_mode: str = "vwap"
    volume_multiplier: float = 10.0

    # None means no per-bond max weight limit. This is portfolio risk control, not an exchange rule.
    max_single_bond_weight: float | None = None
    annual_trading_days: int = 252


from __future__ import annotations

import argparse
from pathlib import Path

from backtest import run_backtest
from config import BacktestConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run convertible bond signal backtest.")
    parser.add_argument("--initial-cash", type=float, default=500_000.0)
    parser.add_argument("--order-cash", type=float, default=10_000.0)
    parser.add_argument("--buy-fee-rate", type=float, default=0.0001)
    parser.add_argument("--sell-fee-rate", type=float, default=0.0001)
    parser.add_argument("--slippage-rate", type=float, default=0.0)
    parser.add_argument("--price-mode", choices=["vwap", "close"], default="vwap")
    parser.add_argument("--volume-multiplier", type=float, default=10.0)
    parser.add_argument("--max-single-bond-weight", type=float, default=None)
    parser.add_argument(
        "--signal-path",
        type=Path,
        default=BacktestConfig.signal_path,
        help="Path to signal_buy_and_sell_v2.csv.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BacktestConfig.output_dir,
        help="Directory for backtest output files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = BacktestConfig(
        signal_path=args.signal_path,
        output_dir=args.output_dir,
        initial_cash=args.initial_cash,
        order_cash=args.order_cash,
        buy_fee_rate=args.buy_fee_rate,
        sell_fee_rate=args.sell_fee_rate,
        slippage_rate=args.slippage_rate,
        price_mode=args.price_mode,
        volume_multiplier=args.volume_multiplier,
        max_single_bond_weight=args.max_single_bond_weight,
    )
    summary = run_backtest(config)

    print("\nBacktest finished.")
    print(f"Final equity: {summary['final_equity']:.2f}")
    print(f"Total return: {summary['total_return']:.2%}")
    print(f"Max drawdown: {summary['max_drawdown']:.2%}")
    print(f"Trades: {summary['total_trades']}")
    print(f"Win rate: {summary['win_rate']:.2%}" if summary["total_trades"] else "Win rate: n/a")
    print(f"Outputs: {config.output_dir}")


if __name__ == "__main__":
    main()

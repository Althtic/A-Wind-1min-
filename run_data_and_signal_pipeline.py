from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent

STAGES = [
    ("数据合并", PROJECT_DIR / "数据清洗与预处理" / "DataConcat.py"),
    ("数据清洗", PROJECT_DIR / "数据清洗与预处理" / "DataClean.py"),
    ("核心指标计算", PROJECT_DIR / "核心指标计算" / "calculate_ratio.py"),
    ("买卖信号生成", PROJECT_DIR / "策略信号" / "Signal_buy_and_sell.py"),
]


def run_stage(name: str, script_path: Path) -> None:
    print(f"\n=== {name}: {script_path.relative_to(PROJECT_DIR)} ===", flush=True)
    subprocess.run([sys.executable, str(script_path)], cwd=PROJECT_DIR, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run data merge, cleaning, ratio calculation, and signal generation."
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="Skip DataConcat.py and reuse 数据清洗与预处理/concat_data.csv.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stages = STAGES[1:] if args.skip_merge else STAGES
    for name, script_path in stages:
        run_stage(name, script_path)

    print("\nPipeline finished. Generated files:")
    outputs = [
        PROJECT_DIR / "数据清洗与预处理" / "concat_data.csv",
        PROJECT_DIR / "核心指标计算" / "clean_data.csv",
        PROJECT_DIR / "策略信号" / "calculate_ratio_data.csv",
        PROJECT_DIR / "策略信号" / "signal_buy_and_sell_v2.csv",
    ]
    for output in outputs:
        status = "OK" if output.exists() else "MISSING"
        print(f"- {status}: {output.relative_to(PROJECT_DIR)}")


if __name__ == "__main__":
    main()

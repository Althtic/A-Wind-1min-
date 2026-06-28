import pandas as pd
import numpy as np
from pathlib import Path

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    input_path = script_dir / "concat_data.csv"
    output_dir = project_dir / "核心指标计算"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "clean_data.csv"

    df = pd.read_csv(input_path)
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()

    price_cols = [
        "Bond_Open", "Bond_High", "Bond_Low", "Bond_Close",
        "Stock_Open", "Stock_High", "Stock_Low", "Stock_Close",
    ]
    price_cols = [c for c in price_cols if c in df.columns]
    for col in price_cols:
        df.loc[df[col] <= 0, col] = np.nan

    df = df.sort_values(["Date", "Bond_Code", "Stock_Code", "Time"])
    group_keys = ["Date", "Bond_Code", "Stock_Code"]
    if price_cols:
        df[price_cols] = df.groupby(group_keys, dropna=False)[price_cols].ffill()

    if "bond_convprice" in df.columns:
        df.loc[df["bond_convprice"] <= 0, "bond_convprice"] = np.nan
        df["bond_convprice"] = df.groupby("Bond_Code", dropna=False)["bond_convprice"].ffill()

    df = df.dropna(subset=["Time", "Date", "Bond_Code", "Stock_Code"])
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

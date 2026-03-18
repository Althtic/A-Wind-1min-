import os
import pandas as pd
from pathlib import Path

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    input_path = script_dir / "concat_data.csv"
    output_dir = Path(r"C:\Users\63585\Desktop\PycharmProjects\pythonProject\C_Bond\核心指标计算")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "clean_data.csv"

    df = pd.read_csv(input_path)
    bond_cols = ["Bond_Open", "Bond_High", "Bond_Low", "Bond_Close", "Bond_Volume", "Bond_Amount"]
    exist_cols = [c for c in bond_cols if c in df.columns]

    if exist_cols:
        df = df.sort_values(["Date", "Time"])
        grp = df.groupby(["Date", "Bond_Code"], dropna=False)[exist_cols]
        df_ffill = grp.ffill()
        df_bfill = grp.bfill()

        df[exist_cols] = df_bfill
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

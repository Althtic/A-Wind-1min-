import os
import glob
import pandas as pd
from pathlib import Path

# ================= 模块 1: 处理 CSV =================
def load_and_prep_csv(csv_path):
    """读取 CSV，清洗代码列，返回 DataFrame"""
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"找不到文件: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # 统一清洗代码列：转为字符串，去空格
    for col in ["Bond_Code", "Stock_Code"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            
    # 确保日期列格式统一
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    elif "Time" in df.columns:
        df["Date"] = pd.to_datetime(df["Time"]).dt.normalize()
        
    return df

def load_and_prep_csv_from_folder(folder_dir):
    """读取文件夹内所有 *.SH.csv 和 *.SZ.csv，合并后清洗，返回 DataFrame"""
    pattern = os.path.join(folder_dir, "*.csv")
    files = [f for f in glob.glob(pattern) if f.endswith(".SH.csv") or f.endswith(".SZ.csv")]
    if not files:
        raise FileNotFoundError(f"文件夹内未找到 *.SH.csv 或 *.SZ.csv: {folder_dir}")
    
    dfs = []
    for f in files:
        dfs.append(load_and_prep_csv(f))
    return pd.concat(dfs, ignore_index=True)

# ================= 模块 2: 处理 Excel (核心难点) =================
def load_and_prep_excel(input_dir, target_year):
    """
    读取 merged_*.xlsx，强制转换日期格式，并修正年份。
    返回包含 [Date, Bond_Code, Stock_Code, bond_convprice] 的 DataFrame
    """
    # 1. 找文件
    files = glob.glob(os.path.join(input_dir, "merged_*.xlsx"))
    if not files:
        print("未找到 merged_*.xlsx 文件，跳过合并。")
        return None
    
    df = pd.read_excel(files[0])
    
    # 2. 定位 Date 列 (不区分大小写)
    date_col = next((c for c in df.columns if str(c).lower() == "date"), None)
    if not date_col:
        raise ValueError("Excel 中未找到 'Date' 列")

    # 3. 转换日期: 专门处理 '9/1/23 12:00' 这种格式
    # %m/%d/%y 能自动识别两位数年份
    df["Date"] = pd.to_datetime(df[date_col], format="%m/%d/%y %H:%M", errors="coerce")
    
    # 如果上述格式失败，尝试通用解析
    if df["Date"].isna().any():
        df["Date"] = pd.to_datetime(df[date_col], errors="coerce")

    # 4. 【关键】年份修正：如果解析出的年份和文件夹年份不一致，强制修正
    # 例如：文件夹是 2025，Excel 里写的是 23 (2023)，这里会 +2 年
    if df["Date"].notna().any():
        current_year = df["Date"].dt.year.mode()[0] # 获取出现最多的年份
        if current_year != target_year:
            diff = target_year - current_year
            df["Date"] = df["Date"] + pd.DateOffset(years=diff)
            print(f"已修正年份: {current_year} -> {target_year}")

    # 5. 标准化日期 (去掉时分秒)
    df["Date"] = df["Date"].dt.normalize()

    # 6. 提取并清洗需要的列
    # 查找 convprice 列
    price_col = next((c for c in df.columns if "convprice" in str(c).lower()), None)
    
    keep_cols = ["Date", "Bond_Code", "Stock_Code"]
    if price_col:
        keep_cols.append(price_col)
        
    df_clean = df[keep_cols].copy()
    
    # 重命名为标准名
    if price_col and price_col != "bond_convprice":
        df_clean.rename(columns={price_col: "bond_convprice"}, inplace=True)
        
    # 清洗代码列
    for col in ["Bond_Code", "Stock_Code"]:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
            
    return df_clean

# ================= 模块 3: 合并与保存 =================
def merge_and_save(df_csv, df_excel, output_path):
    """执行左连接合并并保存"""
    
    if df_excel is None:
        print("没有 Excel 数据，直接保存原始 CSV。")
        df_csv.to_csv(output_path, index=False, encoding="utf-8-sig")
        return

    # 定义合并键
    keys = ["Date", "Bond_Code", "Stock_Code"]
    
    # 检查列是否存在
    missing = [k for k in keys if k not in df_csv.columns or k not in df_excel.columns]
    if missing:
        raise ValueError(f"缺少合并键: {missing}")

    # 执行合并 (Left Join)
    result = pd.merge(df_csv, df_excel, on=keys, how="left")

    result = result.drop_duplicates()
    result = result.dropna(subset=["Time"])
    
    # 保存
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False, encoding="utf-8-sig")
    
    # 简单统计
    count = result["bond_convprice"].notna().sum() if "bond_convprice" in result.columns else 0
    print(f"合并完成！成功匹配 {count} 条转股价格数据。")

# ================= 主程序入口 =================
if __name__ == "__main__":
    base_dir = r"C:\Users\63585\Desktop\PycharmProjects\pythonProject\C_Bond\c_bond_data_1217"
    output_dir = r"C:\Users\63585\Desktop\PycharmProjects\pythonProject\C_Bond\数据清洗与预处理"

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    subdirs = [d for d in Path(base_dir).iterdir() if d.is_dir()]
    daily_files = []
    for folder_path in sorted(subdirs):
        folder_name = folder_path.name
        try:
            target_year = int(folder_name.split("-")[0])
        except:
            target_year = 2025
        output_csv = os.path.join(output_dir, f"{folder_name}.csv")
        daily_files.append(output_csv)
        try:
            df_main = load_and_prep_csv_from_folder(str(folder_path))
            df_extra = load_and_prep_excel(str(folder_path), target_year)
            merge_and_save(df_main, df_extra, output_csv)
            print(f"[{folder_name}] 完成 -> {output_csv}")
        except Exception as e:
            print(f"[{folder_name}] 失败: {e}")

    dfs = []
    for f in daily_files:
        if Path(f).exists():
            dfs.append(pd.read_csv(f))
    if dfs:
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()
        bond_cols = ["Bond_Open", "Bond_High", "Bond_Low", "Bond_Close", "Bond_Volume", "Bond_Amount"]
        exist_cols = [c for c in bond_cols if c in df.columns]
        if exist_cols:
            df = df.sort_values(["Date", "Bond_Code", "Stock_Code", "Time"])
            df[exist_cols] = df.groupby(["Date", "Bond_Code", "Stock_Code"], dropna=False)[exist_cols].ffill()
        df.to_csv(os.path.join(output_dir, "concat_data.csv"), index=False, encoding="utf-8-sig")



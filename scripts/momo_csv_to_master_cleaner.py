# scripts/momo_csv_to_master_cleaner.py
"""
MOMO 主表資料清理工具

功能：
- 根據 JSON mapping 檔案清理 MOMO 主表資料
- 自動轉換資料型態（STRING, DATE, DATETIME, INTEGER, FLOAT, BOOLEAN）
- 批次處理多個 CSV 檔案

輸入：
- data_processed/merged/momo_accounting_orders_cleaned.csv
- data_processed/merged/momo_shipping_orders_cleaned.csv
- config/c1105_momo_fields_mapping.json
- config/a1102_momo_fields_mapping.json

輸出：清理後的 CSV 檔案（覆蓋原檔案）

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import pandas as pd
import json
from pathlib import Path

def load_field_types(mapping_path):
    with open(mapping_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    return {k: v['type'].upper() for k, v in mapping.items()}

def clean_dataframe(df: pd.DataFrame, field_types: dict) -> pd.DataFrame:
    for col, dtype in field_types.items():
        if col not in df.columns:
            continue

        if dtype == "STRING":
            df[col] = df[col].apply(
                lambda x: str(int(float(x))) if pd.notnull(x) and isinstance(x, float) and x.is_integer()
                else str(x) if pd.notnull(x)
                else None
            ).astype("string")

        elif dtype == "DATE":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        elif dtype in ("DATETIME", "TIMESTAMP"):
            df[col] = pd.to_datetime(df[col], errors="coerce")

        elif dtype == "INTEGER":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif dtype == "FLOAT":
            df[col] = pd.to_numeric(df[col], errors="coerce")

        elif dtype == "BOOLEAN":
            df[col] = df[col].astype("boolean")

    return df

def process_file(csv_path: Path, schema_path: Path):
    print(f"🧼 處理檔案: {csv_path.name}")
    df = pd.read_csv(csv_path)
    field_types = load_field_types(schema_path)
    df = clean_dataframe(df, field_types)
    df.to_csv(csv_path, index=False)
    print(f"✅ 已清洗並覆蓋儲存: {csv_path.name}")

def main():
    # 取得目前腳本的相對根目錄
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]

    csv_dir = project_root / "data_processed" / "merged"
    config_dir = project_root / "config"

    targets = [
        {
            "csv": csv_dir / "momo_accounting_orders_cleaned.csv",
            "schema": config_dir / "c1105_momo_fields_mapping.json"
        },
        {
            "csv": csv_dir / "momo_shipping_orders_cleaned.csv",
            "schema": config_dir / "a1102_momo_fields_mapping.json"
        }
    ]

    for t in targets:
        if t["csv"].exists() and t["schema"].exists():
            process_file(t["csv"], t["schema"])
        else:
            print(f"⚠️ 找不到檔案: {t['csv'].relative_to(project_root)} 或 {t['schema'].relative_to(project_root)}")

if __name__ == "__main__":
    main()

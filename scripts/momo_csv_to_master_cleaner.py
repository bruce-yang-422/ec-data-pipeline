"""
momo_csv_to_master_cleaner.py

功能：
- 批次讀取 temp/momo/*.csv 檔案
- 按 momo_fields_mapping.json 定義調整欄位
- 輸出到 momo_master_orders_cleaned.csv

使用：python momo_csv_to_master_cleaner.py
"""

import pandas as pd
import json
from datetime import datetime
import os
from glob import glob

MAPPING_PATH = "config/momo_fields_mapping.json"
SOURCE_DIR = "temp/momo"
OUTPUT_PATH = "data_processed/merged/momo_master_orders_cleaned.csv"

def get_mapping():
    """讀取 momo mapping 設定"""
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    
    columns = sorted(mapping.keys(), key=lambda k: int(mapping[k]["order"]))
    return mapping, columns

def read_csv_files(mapping):
    """讀取所有 CSV 檔案"""
    csv_files = glob(os.path.join(SOURCE_DIR, "*.csv"))
    if not csv_files:
        return pd.DataFrame()
    
    # 從 mapping 建立中英文對應
    zh_to_en = {v["zh_name"]: k for k, v in mapping.items()}
    
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, dtype=str, encoding='utf-8-sig').fillna("")
            
            # 中文欄位轉英文
            df = df.rename(columns=zh_to_en)
            
            # 過濾空行
            if 'order_sn' in df.columns:
                df = df[df['order_sn'].str.strip() != ""]
            
            # 清理數據
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ').str.strip()
                    if col in ['product_sku_main', 'quantity']:
                        df[col] = df[col].str.replace(r'\.0$', '', regex=True)
            
            dfs.append(df)
            print(f"讀取: {file}")
        except Exception as e:
            print(f"讀取失敗: {file} - {e}")
    
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def process_data(df, mapping, columns):
    """處理資料"""
    # 基本欄位
    df['platform'] = 'momo'
    df['processing_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 從訂單編號解析日期
    def parse_order_date(order_sn):
        if isinstance(order_sn, str) and len(order_sn) >= 8:
            date_part = order_sn[:8]  # 取前8碼 YYMMDD
            if date_part.isdigit():
                y, m, d = date_part[:2], date_part[2:4], date_part[4:6]
                year = int(y) + 2000 if int(y) < 50 else int(y) + 1900
                return f"{year:04d}-{int(m):02d}-{int(d):02d}"
        return ""
    
    if 'order_sn' in df.columns:
        df['order_date'] = df['order_sn'].apply(parse_order_date)
    
    # 判斷是否異常單
    def is_abnormal(order_sn):
        if isinstance(order_sn, str) and len(order_sn) > 17:
            # 取後6碼檢查是否為 001-001
            return not order_sn[-7:] == '001-001'
        return False
    
    if 'order_sn' in df.columns:
        df['is_abnormal_order'] = df['order_sn'].apply(is_abnormal)
    
    # 建立去重鍵
    if 'order_sn' in df.columns:
        df['key_for_merge'] = 'momo_' + df['order_sn'].astype(str)
    
    # 確保欄位存在並轉換型態
    for col in columns:
        if col not in df.columns:
            df[col] = ''
        
        # 型態轉換
        if col in mapping:
            data_type = mapping[col].get('type', 'STRING')
            if data_type == 'INTEGER':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            elif data_type == 'FLOAT':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            elif data_type == 'BOOLEAN':
                df[col] = df[col].astype(str).str.lower().isin(['true', '1', 'yes']).astype(bool)
    
    return df[columns]

def save_data(df):
    """儲存資料"""
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # 合併現有資料
    if os.path.exists(OUTPUT_PATH):
        old_df = pd.read_csv(OUTPUT_PATH, dtype=str).fillna("")
        combined = pd.concat([old_df, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['key_for_merge'], keep='last')
    else:
        combined = df
    
    # 排序
    if 'order_date' in combined.columns and 'order_sn' in combined.columns:
        combined = combined.sort_values(['order_date', 'order_sn']).reset_index(drop=True)
        combined = combined[~((combined['order_date'] == '') & (combined['order_sn'] == ''))]
    
    combined.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
    print(f"已儲存: {OUTPUT_PATH} ({len(combined)} 筆)")

def main():
    print("Momo CSV 轉換器")
    
    # 讀取設定
    mapping, columns = get_mapping()
    
    # 讀取檔案
    df = read_csv_files(mapping)
    if df.empty:
        print("沒有找到 CSV 檔案")
        return
    
    print(f"讀取 {len(df)} 筆資料")
    
    # 處理資料
    processed_df = process_data(df, mapping, columns)
    print(f"處理完成 {len(processed_df)} 筆")
    
    # 儲存
    save_data(processed_df)
    
    # 清理 temp 檔案
    for f in os.listdir(SOURCE_DIR):
        if f.lower().endswith('.csv'):
            os.unlink(os.path.join(SOURCE_DIR, f))
    
    print("完成")

if __name__ == "__main__":
    main()
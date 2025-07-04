"""
shopee_csv_to_master_cleaner.py

功能：
- 批次讀取 temp/shopee/*.csv 檔案
- 按 shopee_fields_mapping.json 定義調整欄位
- 輸出到 shopee_master_orders_cleaned.csv

使用：python shopee_csv_to_master_cleaner.py
"""

import pandas as pd
import json
from datetime import datetime
import os
from glob import glob
import re

MAPPING_PATH = "config/shopee_fields_mapping.json"
SOURCE_DIR = "temp/shopee"
OUTPUT_PATH = "data_processed/merged/shopee_master_orders_cleaned.csv"

def get_mapping():
    """讀取 shopee mapping 設定"""
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    
    columns = sorted(mapping.keys(), key=lambda k: int(mapping[k]["order"]))
    en2zh = {k: v["zh_name"] for k, v in mapping.items()}
    
    return mapping, columns, en2zh

def parse_shop_info(filename, df):
    """解析店鋪資訊"""
    # 優先從檔案內容讀取
    if 'shop_name' in df.columns and 'shop_account' in df.columns:
        shop_name = df['shop_name'].dropna().iloc[0] if len(df['shop_name'].dropna()) > 0 else ""
        shop_account = df['shop_account'].dropna().iloc[0] if len(df['shop_account'].dropna()) > 0 else ""
        if shop_name and shop_account:
            return str(shop_name), str(shop_account)
    
    # 從檔名解析
    base = os.path.basename(filename)
    m = re.match(r"(.+?)_([\w\d]+)_Order", base)
    if m:
        return m.group(1), m.group(2)
    
    return "unknown_shop", "unknown_account"

def read_csv_files():
    """讀取所有 CSV 檔案"""
    csv_files = glob(os.path.join(SOURCE_DIR, "*.csv"))
    if not csv_files:
        return pd.DataFrame()
    
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, dtype=str, encoding='utf-8-sig').fillna("")
            
            # 過濾空行（主要欄位都為空的行）
            if 'order_sn' in df.columns:
                df = df[df['order_sn'].str.strip() != ""]
            
            # 清理換行符號和數字格式
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ').str.strip()
                    # 移除數字欄位的 .0 後綴
                    if col in ['product_sku_main', 'product_sku_variation', 'quantity', 'return_quantity']:
                        df[col] = df[col].str.replace(r'\.0$', '', regex=True)
            
            # 解析店鋪資訊
            shop_name, shop_account = parse_shop_info(file, df)
            df["shop_name"] = shop_name
            df["shop_account"] = shop_account
            
            dfs.append(df)
            print(f"讀取: {file}")
        except Exception as e:
            print(f"讀取失敗: {file} - {e}")
    
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def process_data(df, mapping, columns):
    """處理資料"""
    # 產生 item_seq
    if 'order_date' in df.columns and 'order_sn' in df.columns:
        df = df.sort_values(['order_date', 'order_sn']).reset_index(drop=True)
        df['item_seq'] = df.groupby(['order_date', 'order_sn']).cumcount() + 1
    else:
        df['item_seq'] = 1
    
    # 基本欄位
    df['platform'] = 'shopee'
    df['processing_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 建立去重鍵
    if 'product_sku_main' in df.columns and 'product_sku_variation' in df.columns:
        sku_combined = df['product_sku_main'].fillna('') + df['product_sku_variation'].fillna('')
    elif 'product_sku_main' in df.columns:
        sku_combined = df['product_sku_main'].fillna('')
    elif 'product_sku_variation' in df.columns:
        sku_combined = df['product_sku_variation'].fillna('')
    else:
        sku_combined = pd.Series([''] * len(df))
    
    if 'order_sn' in df.columns:
        df['duplicate_key'] = df['order_sn'].astype(str) + '_' + sku_combined.astype(str)
        df['key_for_merge'] = df['order_sn'].astype(str) + '_' + sku_combined.astype(str)
    else:
        df['duplicate_key'] = sku_combined.astype(str)
        df['key_for_merge'] = sku_combined.astype(str)
    
    # 確保所有欄位存在並轉換型態
    for col in columns:
        if col not in df.columns:
            df[col] = ''
        
        # 根據 mapping 轉換資料型態
        if col in mapping:
            data_type = mapping[col].get('data_type', 'string')
            if data_type == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            elif data_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            elif data_type == 'boolean':
                df[col] = df[col].astype(str).str.lower().isin(['true', '1', 'yes']).astype(bool)
            # string 型態保持原樣
    
    return df[columns + ['duplicate_key', 'key_for_merge']]

def save_data(df):
    """儲存資料，新資料覆蓋舊資料"""
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # 合併現有資料
    if os.path.exists(OUTPUT_PATH):
        old_df = pd.read_csv(OUTPUT_PATH, dtype=str).fillna("")
        combined = pd.concat([old_df, df], ignore_index=True)
        # 以新資料覆蓋舊資料（根據 duplicate_key 去重）
        combined = combined.drop_duplicates(subset=['duplicate_key'], keep='last')
    else:
        combined = df
    
    # 重新排序：order_date 升序（最新在下面）
    if 'order_date' in combined.columns and 'order_sn' in combined.columns:
        combined = combined.sort_values(['order_date', 'order_sn']).reset_index(drop=True)
        combined['item_seq'] = combined.groupby(['order_date', 'order_sn']).cumcount() + 1
        # 過濾空訂單
        combined = combined[~((combined['order_date'] == '') & (combined['order_sn'] == ''))]
    
    # 移除 duplicate_key 欄位後儲存
    if 'duplicate_key' in combined.columns:
        combined = combined.drop(columns=['duplicate_key'])
    
    combined.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
    print(f"已儲存: {OUTPUT_PATH} ({len(combined)} 筆)")

def main():
    print("Shopee CSV 轉換器")
    
    # 讀取設定
    mapping, columns, _ = get_mapping()
    
    # 讀取檔案
    df = read_csv_files()
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
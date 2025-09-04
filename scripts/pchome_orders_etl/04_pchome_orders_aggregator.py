import os
import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any

# 路徑設定
INPUT_DIR = r'D:\Projects\python_dev\ec-data-pipeline\temp\pchome'
OUTPUT_DIR = r'D:\Projects\python_dev\ec-data-pipeline\temp\pchome'
SHOPS_MASTER_PATH = r'D:\Projects\python_dev\ec-data-pipeline\config\A02_Shops_Master.json'

os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_shops_master() -> Dict[str, Dict[str, Any]]:
    """載入商店主檔資料"""
    with open(SHOPS_MASTER_PATH, 'r', encoding='utf-8') as f:
        shops_data = json.load(f)
    
    # 建立 platform -> shop_info 的對應表
    shops_dict = {}
    for shop in shops_data['shops']:
        platform = shop['platform']
        shops_dict[platform] = {
            'shop_id': shop.get('shop_id', ''),
            'shop_channel_type': shop.get('shop_channel_type', 'virtual'),
            'shop_business_model': shop.get('shop_business_model', ''),
            'department': shop.get('department', ''),
            'manager': shop.get('manager', '')
        }
    
    return shops_dict

def aggregate_orders():
    """聚合訂單資料並加入商店主檔資訊"""
    # 讀取合併後的訂單資料
    input_file = os.path.join(INPUT_DIR, 'pchome_orders_merged.csv')
    if not os.path.exists(input_file):
        print(f"[ERROR] 找不到輸入檔案: {input_file}")
        return
    
    print(f"[INFO] 讀取訂單資料: {input_file}")
    df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
    print(f"[INFO] 原始資料筆數: {len(df)}")
    
    # 載入商店主檔
    shops_master = load_shops_master()
    print(f"[INFO] 載入商店主檔，共 {len(shops_master)} 個平台")
    
    # 根據 platform 加入商店主檔資訊
    platform = 'PChome'  # PChome 平台的固定值
    if platform in shops_master:
        shop_info = shops_master[platform]
        df['shop_id'] = shop_info['shop_id']
        df['shop_channel_type'] = shop_info['shop_channel_type']
        df['shop_business_model'] = shop_info['shop_business_model']
        df['department'] = shop_info['department']
        df['manager'] = shop_info['manager']
        print(f"[INFO] 已加入商店主檔資訊: {shop_info}")
    else:
        print(f"[WARN] 找不到平台 {platform} 的商店主檔資訊")
        # 設定預設值
        df['shop_id'] = 'PC0001'
        df['shop_channel_type'] = 'virtual'
        df['shop_business_model'] = 'B2B2C'
        df['department'] = '網路部'
        df['manager'] = '科林'
    
    # 資料型態轉換
    numeric_cols = [
        'order_qty', 'quantity', 'cancel_qty', 'price_unit', 'price_total',
        'product_weight_kg', 'weight_total_kg', 'weight_max_kg',
        'package_len', 'package_wid', 'package_hei'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 日期欄位轉換
    date_cols = [
        'order_date', 'ship_date', 'transfer_date', 'preorder_date',
        'return_apply_date', 'return_approve_date'
    ]
    
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 布林欄位轉換
    bool_cols = ['confirm', 'is_merge_box']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().isin(['true', '1', 'yes', '是'])
    
    # 重新排列欄位順序，將新增的商店主檔欄位放在最後
    shop_columns = ['shop_id', 'shop_channel_type', 'shop_business_model', 'department', 'manager']
    
    # 先排列基本訂單欄位
    basic_columns = [
        'platform', 'order_id', 'order_sn', 'item_seq', 
        'order_date', 'order_weekday', 'order_week'
    ]
    
    # 加入其他現有欄位（排除商店主檔欄位）
    existing_cols = [col for col in df.columns if col not in shop_columns and col not in basic_columns]
    final_columns = basic_columns + existing_cols + shop_columns
    
    # 只保留實際存在的欄位
    final_columns = [col for col in final_columns if col in df.columns]
    df = df[final_columns]
    
    # 輸出檔案
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(OUTPUT_DIR, f'pchome_orders_aggregated_{timestamp}.csv')
    
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"[INFO] 已輸出聚合資料: {output_file}")
    print(f"[INFO] 最終資料筆數: {len(df)}")
    print(f"[INFO] 欄位數: {len(df.columns)}")
    
    # 顯示資料摘要
    print("\n=== 資料摘要 ===")
    print(f"平台: {df['platform'].iloc[0] if len(df) > 0 else 'N/A'}")
    print(f"商店ID: {df['shop_id'].iloc[0] if len(df) > 0 else 'N/A'}")
    print(f"商店類型: {df['shop_channel_type'].iloc[0] if len(df) > 0 else 'N/A'}")
    print(f"商業模式: {df['shop_business_model'].iloc[0] if len(df) > 0 else 'N/A'}")
    print(f"部門: {df['department'].iloc[0] if len(df) > 0 else 'N/A'}")
    print(f"負責人: {df['manager'].iloc[0] if len(df) > 0 else 'N/A'}")
    
    if 'order_date' in df.columns and df['order_date'].notna().any():
        order_dates = pd.to_datetime(df['order_date'], errors='coerce')
        min_date = order_dates.min()
        max_date = order_dates.max()
        print(f"訂單日期範圍: {min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else 'N/A'} ~ {max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else 'N/A'}")
    
    if 'price_total' in df.columns:
        total_amount = df['price_total'].sum()
        print(f"總金額: {total_amount:,.0f}")
    
    print(f"商品種類數: {df['product_id'].nunique() if 'product_id' in df.columns else 'N/A'}")
    print(f"訂單數: {df['order_sn'].nunique() if 'order_sn' in df.columns else 'N/A'}")

if __name__ == '__main__':
    aggregate_orders()

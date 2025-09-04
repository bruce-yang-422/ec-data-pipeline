import os
import pandas as pd
import yaml
from datetime import datetime
from typing import Dict, Any

# 路徑設定
INPUT_DIR = r'D:\Projects\python_dev\ec-data-pipeline\temp\pchome'
OUTPUT_DIR = r'D:\Projects\python_dev\ec-data-pipeline\temp\pchome'
PRODUCTS_CONFIG_PATH = r'D:\Projects\python_dev\ec-data-pipeline\config\products.yaml'

os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_products_config() -> Dict[str, Dict[str, Any]]:
    """載入商品主檔資料"""
    with open(PRODUCTS_CONFIG_PATH, 'r', encoding='utf-8') as f:
        products_data = yaml.safe_load(f)
    
    # 建立 vendor_no (條碼) -> product_info 的對應表
    products_dict = {}
    for barcode, product_info in products_data.items():
        if isinstance(product_info, dict):
            products_dict[barcode] = {
                'category_level_1': product_info.get('category_level_1', ''),
                'category_level_2': product_info.get('category_level_2', ''),
                'brand': product_info.get('brand', ''),
                'series': product_info.get('series', ''),
                'pet_type': product_info.get('pet_type', ''),
                'product_name': product_info.get('product_name', ''),
                'item_code': product_info.get('item_code', ''),
                'sku': product_info.get('sku', ''),
                'tags': product_info.get('tags', ''),
                'spec': product_info.get('spec', ''),
                'unit': product_info.get('unit', ''),
                'weight_g': product_info.get('weight_g', ''),
                'package_size': product_info.get('package_size', ''),
                'package_type': product_info.get('package_type', ''),
                'package_qty': product_info.get('package_qty', ''),
                'origin': product_info.get('origin', ''),
                'min_qty': product_info.get('min_qty', ''),
                'msrp': product_info.get('msrp', ''),
                'supplier_price': product_info.get('supplier_price', ''),
                'list_price': product_info.get('list_price', ''),
                'cost': product_info.get('cost', ''),
                'supplier_code': product_info.get('supplier_code', ''),
                'supplier': product_info.get('supplier', ''),
                'supplier_ref': product_info.get('supplier_ref', '')
            }
    
    return products_dict

def enrich_orders_with_products():
    """為訂單資料加入商品主檔資訊"""
    # 讀取聚合後的訂單資料
    input_files = []
    for file in os.listdir(INPUT_DIR):
        if file.startswith('pchome_orders_aggregated_') and file.endswith('.csv'):
            input_files.append(file)
    
    if not input_files:
        print(f"[ERROR] 找不到聚合後的訂單檔案於 {INPUT_DIR}")
        return
    
    # 使用最新的檔案
    latest_file = sorted(input_files)[-1]
    input_file = os.path.join(INPUT_DIR, latest_file)
    
    print(f"[INFO] 讀取訂單資料: {input_file}")
    df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
    print(f"[INFO] 原始資料筆數: {len(df)}")
    
    # 載入商品主檔
    products_master = load_products_config()
    print(f"[INFO] 載入商品主檔，共 {len(products_master)} 個商品")
    
    # 統計匹配情況
    matched_count = 0
    unmatched_vendor_nos = set()
    
    # 根據 vendor_no 加入商品主檔資訊
    for idx, row in df.iterrows():
        vendor_no = str(row.get('vendor_no', '')).strip()
        
        if vendor_no and vendor_no in products_master:
            product_info = products_master[vendor_no]
            matched_count += 1
            
            # 加入商品主檔欄位，但保留原本的 product_name
            for field, value in product_info.items():
                if field == 'product_name':
                    # 保留原本的 product_name，新增 master_product_name
                    df.at[idx, 'master_product_name'] = value
                else:
                    df.at[idx, field] = value
        else:
            if vendor_no:
                unmatched_vendor_nos.add(vendor_no)
            # 設定預設值
            for field in ['category_level_1', 'category_level_2', 'brand', 'series', 'pet_type', 
                         'master_product_name', 'item_code', 'sku', 'tags', 'spec', 'unit', 'weight_g',
                         'package_size', 'package_type', 'package_qty', 'origin', 'min_qty',
                         'msrp', 'supplier_price', 'list_price', 'cost', 'supplier_code',
                         'supplier', 'supplier_ref']:
                if field not in df.columns or pd.isna(df.at[idx, field]):
                    df.at[idx, field] = ''
    
    print(f"[INFO] 成功匹配商品: {matched_count} 筆")
    print(f"[INFO] 未匹配的 vendor_no 數量: {len(unmatched_vendor_nos)}")
    
    if unmatched_vendor_nos:
        print(f"[WARN] 未匹配的 vendor_no 範例: {list(unmatched_vendor_nos)[:10]}")
    
    # 資料型態轉換
    numeric_cols = [
        'weight_g', 'min_qty', 'msrp', 'supplier_price', 'list_price', 'cost'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 重新排列欄位順序，將新增的商品主檔欄位放在商店主檔欄位之前
    product_columns = [
        'category_level_1', 'category_level_2', 'brand', 'series', 'pet_type',
        'master_product_name', 'item_code', 'sku', 'tags', 'spec', 'unit', 'weight_g',
        'package_size', 'package_type', 'package_qty', 'origin', 'min_qty',
        'msrp', 'supplier_price', 'list_price', 'cost', 'supplier_code',
        'supplier', 'supplier_ref'
    ]
    
    shop_columns = ['shop_id', 'shop_channel_type', 'shop_business_model', 'department', 'manager']
    
    # 先排列基本訂單欄位
    basic_columns = [
        'platform', 'order_id', 'order_sn', 'item_seq', 
        'order_date', 'order_weekday', 'order_week'
    ]
    
    # 加入其他現有欄位（排除商品和商店主檔欄位）
    existing_cols = [col for col in df.columns if col not in product_columns and col not in shop_columns and col not in basic_columns]
    final_columns = basic_columns + existing_cols + product_columns + shop_columns
    
    # 只保留實際存在的欄位
    final_columns = [col for col in final_columns if col in df.columns]
    df = df[final_columns]
    
    # 輸出檔案
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(OUTPUT_DIR, f'pchome_orders_product_enriched_{timestamp}.csv')
    
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"[INFO] 已輸出商品豐富化資料: {output_file}")
    print(f"[INFO] 最終資料筆數: {len(df)}")
    print(f"[INFO] 欄位數: {len(df.columns)}")
    
    # 顯示資料摘要
    print("\n=== 資料摘要 ===")
    print(f"平台: {df['platform'].iloc[0] if len(df) > 0 else 'N/A'}")
    print(f"商店ID: {df['shop_id'].iloc[0] if len(df) > 0 else 'N/A'}")
    
    if 'category_level_1' in df.columns:
        category_counts = df['category_level_1'].value_counts()
        print(f"商品類別分布: {dict(category_counts.head())}")
    
    if 'brand' in df.columns:
        brand_counts = df['brand'].value_counts()
        print(f"品牌分布: {dict(brand_counts.head())}")
    
    if 'pet_type' in df.columns:
        pet_type_counts = df['pet_type'].value_counts()
        print(f"寵物類型分布: {dict(pet_type_counts)}")
    
    if 'order_date' in df.columns and df['order_date'].notna().any():
        order_dates = pd.to_datetime(df['order_date'], errors='coerce')
        min_date = order_dates.min()
        max_date = order_dates.max()
        print(f"訂單日期範圍: {min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else 'N/A'} ~ {max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else 'N/A'}")
    
    if 'price_total' in df.columns:
        # 確保 price_total 是數值型態
        df['price_total'] = pd.to_numeric(df['price_total'], errors='coerce')
        total_amount = df['price_total'].sum()
        print(f"總金額: {total_amount:,.0f}")
    
    print(f"商品種類數: {df['product_id'].nunique() if 'product_id' in df.columns else 'N/A'}")
    print(f"訂單數: {df['order_sn'].nunique() if 'order_sn' in df.columns else 'N/A'}")

if __name__ == '__main__':
    enrich_orders_with_products()

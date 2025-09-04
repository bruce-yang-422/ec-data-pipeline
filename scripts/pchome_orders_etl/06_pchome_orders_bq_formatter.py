import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any

# 路徑設定
INPUT_DIR = r'D:\Projects\python_dev\ec-data-pipeline\temp\pchome'
OUTPUT_DIR = r'D:\Projects\python_dev\ec-data-pipeline\data_processed\merged'

os.makedirs(OUTPUT_DIR, exist_ok=True)

def format_for_bigquery():
    """將商品豐富化後的訂單資料轉成適合 BigQuery 的格式"""
    # 讀取商品豐富化後的訂單資料
    input_files = []
    for file in os.listdir(INPUT_DIR):
        if file.startswith('pchome_orders_product_enriched_') and file.endswith('.csv'):
            input_files.append(file)
    
    if not input_files:
        print(f"[ERROR] 找不到商品豐富化後的訂單檔案於 {INPUT_DIR}")
        return
    
    # 使用最新的檔案
    latest_file = sorted(input_files)[-1]
    input_file = os.path.join(INPUT_DIR, latest_file)
    
    print(f"[INFO] 讀取商品豐富化資料: {input_file}")
    df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
    print(f"[INFO] 原始資料筆數: {len(df)}")
    print(f"[INFO] 原始欄位數: {len(df.columns)}")
    
    # BigQuery 資料型態轉換
    print("[INFO] 開始 BigQuery 格式轉換...")
    
    # 1. 字串欄位清理
    string_columns = [
        'platform', 'order_id', 'order_sn', 'item_seq', 'temp_layer', 'ship_order_no',
        'receiver', 'receiver_zip', 'receiver_addr', 'receiver_phone', 'product_name',
        'product_id', 'sku_option', 'product_spec', 'vendor_no', 'remark',
        'category_level_1', 'category_level_2', 'brand', 'series', 'pet_type',
        'master_product_name', 'item_code', 'sku', 'tags', 'spec', 'unit',
        'package_size', 'package_type', 'package_qty', 'origin', 'supplier_code',
        'supplier', 'supplier_ref', 'shop_id', 'shop_channel_type', 'shop_business_model',
        'department', 'manager'
    ]
    
    for col in string_columns:
        if col in df.columns:
            # 清理字串：去除多餘空白、換行符號
            df[col] = df[col].astype(str).str.strip().str.replace('\n', ' ').str.replace('\r', ' ')
            # 空值統一為空字串
            df[col] = df[col].replace(['nan', 'None', 'null'], '')
    
    # 2. 整數欄位轉換
    integer_columns = [
        'order_weekday', 'order_week', 'order_qty', 'quantity', 'cancel_qty',
        'min_qty'
    ]
    
    for col in integer_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
    
    # 3. 浮點數欄位轉換
    float_columns = [
        'weight_total_kg', 'weight_max_kg', 'price_unit', 'price_total',
        'product_weight_kg', 'package_len', 'package_wid', 'package_hei',
        'weight_g', 'msrp', 'supplier_price', 'list_price', 'cost'
    ]
    
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    # 4. 布林欄位轉換
    boolean_columns = ['confirm', 'is_merge_box']
    
    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().isin(['true', '1', 'yes', '是'])
    
    # 5. 日期時間欄位轉換
    # DATE 格式欄位（只保留日期部分）
    date_columns = ['order_date', 'ship_date', 'transfer_date']
    
    for col in date_columns:
        if col in df.columns:
            # 轉換為 datetime，然後只保留日期部分
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            # 將 NaT 轉為 None (BigQuery 的 NULL)
            df[col] = df[col].where(pd.notnull(df[col]), None)
    
    # DATETIME 格式欄位（保留日期和時間）
    datetime_columns = ['preorder_date', 'return_apply_date', 'return_approve_date']
    
    for col in datetime_columns:
        if col in df.columns:
            # 轉換為 datetime，無效值設為 None
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # 將 NaT 轉為 None (BigQuery 的 NULL)
            df[col] = df[col].where(pd.notnull(df[col]), None)
    
    # 6. BigQuery 欄位名稱規範化
    # 移除特殊字元，確保符合 BigQuery 命名規範
    column_mapping = {}
    for col in df.columns:
        new_col = col
        # 替換特殊字元
        new_col = new_col.replace(' ', '_').replace('-', '_').replace('.', '_')
        new_col = new_col.replace('(', '').replace(')', '').replace('[', '').replace(']', '')
        new_col = new_col.replace('（', '').replace('）', '').replace('【', '').replace('】', '')
        # 確保以字母或底線開頭
        if new_col and not new_col[0].isalpha() and new_col[0] != '_':
            new_col = 'col_' + new_col
        # 轉為小寫
        new_col = new_col.lower()
        if new_col != col:
            column_mapping[col] = new_col
    
    # 重新命名欄位
    df = df.rename(columns=column_mapping)
    
    # 7. 處理重複欄位名稱
    # 如果重命名後有重複，加上數字後綴
    seen = {}
    new_columns = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_columns.append(col)
    df.columns = new_columns
    
    # 8. 保留所有欄位，不移除空欄位（BigQuery 可以處理 NULL 值）
    # 將 NaN 值轉為 None (BigQuery 的 NULL)
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].where(pd.notnull(df[col]), None)
    
    # 9. 重新排列欄位順序（保持原始順序，只做 BigQuery 友善的調整）
    # 定義理想的欄位順序
    ideal_order = [
        # 基本訂單資訊
        'platform', 'shop_id', 'order_id', 'order_sn', 'item_seq', 'order_date', 'order_weekday', 'order_week',
        # 訂單詳細資訊
        'temp_layer', 'is_merge_box', 'ship_order_no', 'confirm', 'weight_total_kg', 'weight_max_kg',
        'ship_date', 'transfer_date', 'preorder_date', 'return_apply_date', 'return_approve_date',
        # 收件人資訊
        'receiver', 'receiver_zip', 'receiver_addr', 'receiver_phone',
        # 商品資訊
        'product_name', 'product_id', 'sku_option', 'order_qty', 'quantity', 'cancel_qty',
        'price_unit', 'price_total', 'product_spec', 'vendor_no', 'product_weight_kg',
        'package_len', 'package_wid', 'package_hei', 'remark',
        # 商品主檔資訊
        'category_level_1', 'category_level_2', 'brand', 'series', 'pet_type', 'master_product_name',
        'item_code', 'sku', 'tags', 'spec', 'unit', 'weight_g', 'package_size', 'package_type',
        'package_qty', 'origin', 'min_qty', 'msrp', 'supplier_price', 'list_price', 'cost',
        'supplier_code', 'supplier', 'supplier_ref',
        # 商店主檔資訊
        'shop_channel_type', 'shop_business_model', 'department', 'manager'
    ]
    
    # 先排列理想順序的欄位
    ordered_columns = []
    for col in ideal_order:
        if col in df.columns:
            ordered_columns.append(col)
    
    # 加入其他未列在理想順序中的欄位
    remaining_columns = [col for col in df.columns if col not in ordered_columns]
    final_columns = ordered_columns + sorted(remaining_columns)
    
    df = df[final_columns]
    
    # 10. 輸出檔案
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(OUTPUT_DIR, f'pchome_orders_bq_formatted_{timestamp}.csv')
    
    # 使用 UTF-8 編碼，BigQuery 推薦格式
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"[INFO] 已輸出 BigQuery 格式檔案: {output_file}")
    print(f"[INFO] 最終資料筆數: {len(df)}")
    print(f"[INFO] 最終欄位數: {len(df.columns)}")
    
    # 顯示資料摘要
    print("\n=== BigQuery 格式摘要 ===")
    print(f"平台: {df['platform'].iloc[0] if len(df) > 0 else 'N/A'}")
    print(f"商店ID: {df['shop_id'].iloc[0] if len(df) > 0 else 'N/A'}")
    
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
    
    # 顯示資料型態摘要
    print("\n=== 資料型態摘要 ===")
    dtype_summary = df.dtypes.value_counts()
    for dtype, count in dtype_summary.items():
        print(f"{dtype}: {count} 個欄位")
    
    # 顯示欄位名稱變更摘要
    if column_mapping:
        print(f"\n=== 欄位名稱變更摘要 ===")
        print(f"共 {len(column_mapping)} 個欄位名稱已規範化")
        for old_name, new_name in list(column_mapping.items())[:5]:
            print(f"  {old_name} -> {new_name}")
        if len(column_mapping) > 5:
            print(f"  ... 還有 {len(column_mapping) - 5} 個欄位")

if __name__ == '__main__':
    format_for_bigquery()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PChome BigQuery 上傳器

主要功能：
- 專門上傳 PChome 訂單資料至 BigQuery
- 自動抓取最新的 PChome BigQuery 格式 CSV 檔案 (pchome_orders_bq_formatted_*.csv)
- 從 data_processed/merged 目錄讀取腳本 06 的輸出檔案
- 上傳到 shopee-etl-reporting.yichai_pchome_data.pchome_orders_data
- 自動重複資料檢查與處理
- 完整的日誌記錄與錯誤追蹤
- 支援多種上傳模式 (WRITE_TRUNCATE/APPEND/EMPTY)，預設為覆蓋模式
- 根據 pchome_fields_mapping.json 動態生成 schema

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import argparse
import os
import sys
import pandas as pd
import logging
import glob
import json
from datetime import datetime

# ✅ 將專案根目錄加入 sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# ✅ 使用相對 import
from bigquery_utils import get_bq_client, upload_csv_to_bq, check_duplicate_order_sn
from google.cloud import bigquery

# PChome 專用設定
PCHOME_DATASET = "yichai_pchome_data"
PCHOME_TABLE = "pchome_orders_data"
PCHOME_PROJECT = "shopee-etl-reporting"

def get_csv_pattern():
    """根據當前工作目錄動態設定 CSV 檔案路徑
    
    現在讀取 data_processed/merged 目錄下的腳本 06 輸出檔案
    """
    current_dir = os.getcwd()
    
    # 如果當前目錄是專案根目錄
    if os.path.basename(current_dir) == "ec-data-pipeline":
        return "data_processed/merged/pchome_orders_bq_formatted_*.csv"
    else:
        # 如果從其他目錄執行，使用絕對路徑
        return os.path.join(current_dir, "data_processed/merged/pchome_orders_bq_formatted_*.csv")

def get_latest_csv_file():
    """取得最新的 PChome BigQuery 格式 CSV 檔案"""
    pattern = get_csv_pattern()
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"找不到符合模式的檔案: {pattern}")
    
    # 按修改時間排序，取得最新的檔案
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def load_pchome_schema():
    """載入 PChome 欄位對應表並生成 BigQuery Schema"""
    schema_path = "config/pchome_fields_mapping.json"
    
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"找不到 PChome 欄位對應表: {schema_path}")
    
    with open(schema_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    
    # 生成 BigQuery Schema
    schema_fields = []
    
    # 基本訂單欄位
    basic_fields = {
        'platform': 'STRING',
        'shop_id': 'STRING', 
        'order_id': 'STRING',
        'order_sn': 'STRING',
        'item_seq': 'STRING',
        'order_date': 'DATE',
        'order_weekday': 'INTEGER',
        'order_week': 'INTEGER',
        'temp_layer': 'STRING',
        'is_merge_box': 'BOOLEAN',
        'ship_order_no': 'STRING',
        'confirm': 'BOOLEAN',
        'weight_total_kg': 'FLOAT',
        'weight_max_kg': 'FLOAT',
        'ship_date': 'DATE',
        'transfer_date': 'DATE',
        'preorder_date': 'DATETIME',
        'return_apply_date': 'DATETIME',
        'return_approve_date': 'DATETIME',
        'receiver': 'STRING',
        'receiver_zip': 'STRING',
        'receiver_addr': 'STRING',
        'receiver_phone': 'STRING',
        'product_name': 'STRING',
        'product_id': 'STRING',
        'sku_option': 'STRING',
        'order_qty': 'INTEGER',
        'quantity': 'INTEGER',
        'cancel_qty': 'INTEGER',
        'price_unit': 'FLOAT',
        'price_total': 'FLOAT',
        'product_spec': 'STRING',
        'vendor_no': 'STRING',
        'product_weight_kg': 'FLOAT',
        'package_len': 'FLOAT',
        'package_wid': 'FLOAT',
        'package_hei': 'FLOAT',
        'remark': 'STRING'
    }
    
    # 商品主檔欄位
    product_fields = {
        'category_level_1': 'STRING',
        'category_level_2': 'STRING',
        'brand': 'STRING',
        'series': 'STRING',
        'pet_type': 'STRING',
        'master_product_name': 'STRING',
        'item_code': 'STRING',
        'sku': 'STRING',
        'tags': 'STRING',
        'spec': 'STRING',
        'unit': 'STRING',
        'weight_g': 'FLOAT',
        'package_size': 'STRING',
        'package_type': 'STRING',
        'package_qty': 'STRING',
        'origin': 'STRING',
        'min_qty': 'INTEGER',
        'msrp': 'FLOAT',
        'supplier_price': 'FLOAT',
        'list_price': 'FLOAT',
        'cost': 'FLOAT',
        'supplier_code': 'STRING',
        'supplier': 'STRING',
        'supplier_ref': 'STRING'
    }
    
    # 商店主檔欄位
    shop_fields = {
        'shop_channel_type': 'STRING',
        'shop_business_model': 'STRING',
        'department': 'STRING',
        'manager': 'STRING'
    }
    
    # 合併所有欄位
    all_fields = {**basic_fields, **product_fields, **shop_fields}
    
    # 生成 BigQuery Schema
    for field_name, field_type in all_fields.items():
        schema_fields.append(bigquery.SchemaField(field_name, field_type))
    
    return schema_fields

def setup_logging():
    """設定 BigQuery 上傳器的日誌系統"""
    # 取得專案根目錄
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    logs_dir = os.path.join(project_root, "logs")
    
    # 確保 logs 目錄存在
    os.makedirs(logs_dir, exist_ok=True)
    
    # 設定日誌檔案名稱
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"pchome_bigquery_uploader_{timestamp}.log")
    
    # 設定日誌格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def main():
    """主程式"""
    parser = argparse.ArgumentParser(description='PChome BigQuery 上傳器')
    parser.add_argument('--mode', choices=['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'], 
                       default='WRITE_TRUNCATE', help='上傳模式 (預設: WRITE_TRUNCATE)')
    parser.add_argument('--file', help='指定 CSV 檔案路徑 (預設: 自動找最新檔案)')
    parser.add_argument('--dry-run', action='store_true', help='預覽模式，不實際上傳')
    parser.add_argument('--check-duplicates', action='store_true', help='檢查重複資料')
    
    args = parser.parse_args()
    
    # 設定日誌
    logger = setup_logging()
    
    try:
        # 取得 CSV 檔案
        if args.file:
            csv_file = args.file
            if not os.path.exists(csv_file):
                raise FileNotFoundError(f"指定的檔案不存在: {csv_file}")
        else:
            csv_file = get_latest_csv_file()
        
        logger.info(f"使用 CSV 檔案: {csv_file}")
        
        # 讀取 CSV 檔案
        logger.info("讀取 CSV 檔案...")
        df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
        logger.info(f"CSV 檔案筆數: {len(df)}")
        logger.info(f"CSV 檔案欄位數: {len(df.columns)}")
        
        # 顯示資料摘要
        logger.info("\n=== 資料摘要 ===")
        logger.info(f"平台: {df['platform'].iloc[0] if len(df) > 0 else 'N/A'}")
        logger.info(f"商店ID: {df['shop_id'].iloc[0] if len(df) > 0 else 'N/A'}")
        
        if 'order_date' in df.columns and df['order_date'].notna().any():
            order_dates = pd.to_datetime(df['order_date'], errors='coerce')
            min_date = order_dates.min()
            max_date = order_dates.max()
            logger.info(f"訂單日期範圍: {min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else 'N/A'} ~ {max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else 'N/A'}")
        
        if 'price_total' in df.columns:
            df['price_total'] = pd.to_numeric(df['price_total'], errors='coerce')
            total_amount = df['price_total'].sum()
            logger.info(f"總金額: {total_amount:,.0f}")
        
        logger.info(f"商品種類數: {df['product_id'].nunique() if 'product_id' in df.columns else 'N/A'}")
        logger.info(f"訂單數: {df['order_sn'].nunique() if 'order_sn' in df.columns else 'N/A'}")
        
        # 檢查重複資料
        if args.check_duplicates:
            logger.info("\n=== 檢查重複資料 ===")
            duplicate_count = df.duplicated(subset=['order_id']).sum()
            logger.info(f"重複的 order_id 數量: {duplicate_count}")
            
            if duplicate_count > 0:
                logger.warning("發現重複資料，建議先清理後再上傳")
        
        # 預覽模式
        if args.dry_run:
            logger.info("\n=== 預覽模式 ===")
            logger.info(f"目標專案: {PCHOME_PROJECT}")
            logger.info(f"目標資料集: {PCHOME_DATASET}")
            logger.info(f"目標表格: {PCHOME_TABLE}")
            logger.info(f"上傳模式: {args.mode}")
            logger.info("預覽完成，未實際上傳")
            return
        
        # 取得 BigQuery 客戶端
        logger.info("\n=== 開始上傳至 BigQuery ===")
        key_path = "config/bigquery_uploader_key.json"
        client = get_bq_client(key_path)
        
        # 載入 Schema
        schema = load_pchome_schema()
        logger.info(f"載入 Schema，共 {len(schema)} 個欄位")
        
        # 上傳至 BigQuery
        table_id = f"{PCHOME_PROJECT}.{PCHOME_DATASET}.{PCHOME_TABLE}"
        logger.info(f"上傳至: {table_id}")
        
        result = upload_csv_to_bq(
            client=client,
            csv_path=csv_file,
            dataset_id=PCHOME_DATASET,
            table_id=PCHOME_TABLE,
            schema=schema,
            write_disposition=args.mode,
            logger=logger
        )
        
        if result:
            logger.info("✅ 上傳成功！")
            logger.info(f"上傳筆數: {len(df)}")
        else:
            logger.error("❌ 上傳失敗！")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ 執行失敗: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MOMO 會計訂單 BigQuery 上傳器

主要功能：
- 專門上傳 MOMO 會計訂單資料至 BigQuery
- 自動抓取最新的 MOMO 會計訂單 BigQuery 格式 CSV 檔案 (momo_accounting_orders_bq_formatted_*.csv)
- 從 data_processed/merged 目錄讀取腳本 06 的輸出檔案
- 上傳到 shopee-etl-reporting.yichai_momo_data.c1105_momo_accounting_orders
- 自動重複資料檢查與處理
- 完整的日誌記錄與錯誤追蹤
- 支援多種上傳模式 (WRITE_TRUNCATE/APPEND/EMPTY)，預設為覆蓋模式
- 根據 c1105_momo_fields_mapping.json 動態生成 schema

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

# MOMO 會計訂單專用設定
MOMO_DATASET = "yichai_momo_data"
MOMO_TABLE = "c1105_momo_accounting_orders"
MOMO_PROJECT = "shopee-etl-reporting"

def get_csv_pattern():
    """根據當前工作目錄動態設定 CSV 檔案路徑
    
    現在讀取 data_processed/merged 目錄下的腳本 06 輸出檔案
    """
    current_dir = os.getcwd()
    
    # 如果當前目錄是專案根目錄
    if os.path.basename(current_dir) == "ec-data-pipeline":
        return "data_processed/merged/momo_accounting_orders_bq_formatted_*.csv"
    
    # 如果當前目錄是 scripts/bigquery_uploader
    elif os.path.basename(current_dir) == "bigquery_uploader":
        return "../../data_processed/merged/momo_accounting_orders_bq_formatted_*.csv"
    
    # 其他情況，嘗試相對路徑
    else:
        return "data_processed/merged/momo_accounting_orders_bq_formatted_*.csv"

def get_latest_csv_file():
    """自動找最新的 MOMO 會計訂單 BigQuery 格式 CSV 檔案"""
    pattern = get_csv_pattern()
    csv_files = glob.glob(pattern)
    
    if not csv_files:
        raise FileNotFoundError(f"找不到符合模式的檔案: {pattern}")
    
    # 按修改時間排序，取最新的
    latest_file = max(csv_files, key=os.path.getmtime)
    return latest_file

def setup_logging():
    """設定日誌系統"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"momo_accounting_bq_uploader_{timestamp}.log"
    
    # 確保 logs 目錄存在
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_path = os.path.join(log_dir, log_filename)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def load_field_mapping():
    """載入 C1105 MOMO 欄位對應設定"""
    current_dir = os.getcwd()
    
    # 根據當前目錄設定配置檔案路徑
    if os.path.basename(current_dir) == "bigquery_uploader":
        mapping_path = "../../config/c1105_momo_fields_mapping.json"
    else:
        mapping_path = "config/c1105_momo_fields_mapping.json"
    
    if not os.path.exists(mapping_path):
        raise FileNotFoundError(f"找不到欄位對應檔案: {mapping_path}")
    
    with open(mapping_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    
    return mapping

def clean_csv_duplicate_columns(csv_path, logger):
    """清理CSV檔案中的重複欄位和無效字元"""
    logger.info("檢查並清理重複欄位和無效字元...")
    
    # 讀取CSV檔案（確保特定欄位保持字串格式）
    df = pd.read_csv(csv_path)
    
    # 強制將特定欄位轉換為字串，避免小數點
    string_fields = ['product_manufacturer_code', 'product_sku_main', 'product_barcode', 'product_spec']
    for field in string_fields:
        if field in df.columns:
            # 先轉換為整數（如果是數值），再轉換為字串
            if df[field].dtype in ['int64', 'float64']:
                # 使用 apply 方法確保每個值都被正確轉換
                df[field] = df[field].apply(lambda x: str(int(float(x))) if pd.notna(x) else '')
            else:
                df[field] = df[field].astype(str)
            # 強制轉換為字串類型
            df[field] = df[field].astype(str)
    
    # 處理帳務數字欄位，確保小數點下兩位
    cost_fields = ['product_cost_untaxed', 'platform_product_cost', 'product_original_price', 'product_cost_from_catalog']
    for field in cost_fields:
        if field in df.columns:
            # 轉換為數值，保留小數點下兩位
            df[field] = pd.to_numeric(df[field], errors='coerce').round(2).fillna(0)
            # 確保顯示小數點下兩位（包括整數也要顯示.00）
            df[field] = df[field].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "0.00")
    
    # 清理欄位名稱
    original_columns = df.columns.tolist()
    cleaned_columns = []
    
    for col in original_columns:
        # 替換點號為底線（BigQuery不允許點號）
        cleaned_col = col.replace('.', '_')
        # 替換其他無效字元
        cleaned_col = cleaned_col.replace(' ', '_').replace('-', '_')
        # 確保欄位名稱以字母或底線開頭
        if cleaned_col[0].isdigit():
            cleaned_col = f"col_{cleaned_col}"
        
        cleaned_columns.append(cleaned_col)
        if col != cleaned_col:
            logger.info(f"欄位名稱清理: {col} -> {cleaned_col}")
    
    # 重新命名欄位
    df.columns = cleaned_columns
    
    # 檢查是否還有重複欄位
    unique_columns = []
    seen_columns = set()
    
    for col in cleaned_columns:
        if col in seen_columns:
            logger.warning(f"發現重複欄位: {col}")
            # 為重複欄位添加後綴
            counter = 1
            new_col = f"{col}_{counter}"
            while new_col in seen_columns:
                counter += 1
                new_col = f"{col}_{counter}"
            df = df.rename(columns={col: new_col})
            unique_columns.append(new_col)
            logger.info(f"重複欄位重新命名: {col} -> {new_col}")
        else:
            unique_columns.append(col)
            seen_columns.add(col)
    
    # 重新儲存CSV檔案（確保特定欄位保持字串格式）
    logger.info(f"清理完成: {len(original_columns)} -> {len(unique_columns)} 個欄位")
    temp_path = csv_path.replace('.csv', '_cleaned.csv')
    # 在保存前再次確保特定欄位為字串格式
    for field in string_fields:
        if field in df.columns:
            df[field] = df[field].astype(str)
    
    # 在保存前再次確保帳務數字欄位格式正確
    for field in cost_fields:
        if field in df.columns:
            df[field] = df[field].astype(str)
    
    df.to_csv(temp_path, index=False, encoding='utf-8')
    return temp_path

def generate_bigquery_schema_from_csv(csv_path):
    """根據 CSV 檔案實際欄位生成 BigQuery schema"""
    # 讀取 CSV 檔案的第一行來獲取欄位名稱
    df = pd.read_csv(csv_path, nrows=1)
    columns = df.columns.tolist()
    
    schema = []
    
    for column in columns:
        # 根據欄位名稱推斷資料類型
        # 特定欄位必須保持字串格式，避免小數點
        if column in ['product_manufacturer_code', 'product_sku_main', 'product_barcode', 'product_spec']:
            bq_type = bigquery.SqlTypeNames.STRING
        elif column in ['quantity', 'product_cost_untaxed', 'platform_product_cost', 'product_original_price', 'product_cost_from_catalog',
                     'product_weight_g', 'product_min_qty', 'product_msrp', 'product_price',
                     'product_supplier_price', 'product_list_price', 'single_product_id']:
            bq_type = bigquery.SqlTypeNames.FLOAT
        elif column in ['is_abnormal_order', 'shop_is_ad_shopee_ads_enabled']:
            bq_type = bigquery.SqlTypeNames.BOOLEAN
        elif column in ['order_date', 'actual_shipping_date', 'ship_by_date', 'product_price_date']:
            bq_type = bigquery.SqlTypeNames.DATE
        elif column in ['order_transfer_date', 'bq_processing_timestamp']:
            bq_type = bigquery.SqlTypeNames.DATETIME
        else:
            bq_type = bigquery.SqlTypeNames.STRING
        
        schema.append(bigquery.SchemaField(column, bq_type))
    
    return schema

def validate_csv_file(csv_path, logger):
    """驗證 CSV 檔案格式"""
    try:
        # 讀取前幾行檢查格式
        df = pd.read_csv(csv_path, nrows=5)
        logger.info(f"CSV 檔案驗證成功: {len(df.columns)} 個欄位")
        return True
    except Exception as e:
        logger.error(f"CSV 檔案驗證失敗: {e}")
        return False

def main():
    """主函數"""
    parser = argparse.ArgumentParser(description="MOMO 會計訂單 BigQuery 上傳器")
    parser.add_argument("--mode", choices=["truncate", "append", "empty"], 
                       default="truncate", help="上傳模式 (預設: truncate)")
    parser.add_argument("--csv-file", help="指定 CSV 檔案路徑 (預設: 自動找最新)")
    parser.add_argument("--credential", default="config/bigquery_uploader_key.json",
                       help="BigQuery 認證檔案路徑")
    parser.add_argument("--dry-run", action="store_true", help="僅檢查檔案，不上傳")
    
    args = parser.parse_args()
    
    # 設定日誌
    logger = setup_logging()
    logger.info("=== MOMO 會計訂單 BigQuery 上傳器啟動 ===")
    
    try:
        # 1. 取得 CSV 檔案
        if args.csv_file:
            csv_path = args.csv_file
            logger.info(f"使用指定的 CSV 檔案: {csv_path}")
        else:
            csv_path = get_latest_csv_file()
            logger.info(f"自動找到最新的 CSV 檔案: {csv_path}")
        
        # 檢查檔案是否存在
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV 檔案不存在: {csv_path}")
        
        # 2. 驗證 CSV 檔案
        if not validate_csv_file(csv_path, logger):
            raise ValueError("CSV 檔案格式驗證失敗")
        
        # 3. 清理重複欄位
        cleaned_csv_path = clean_csv_duplicate_columns(csv_path, logger)
        
        # 4. 生成 BigQuery schema（根據清理後的CSV檔案實際欄位）
        schema = generate_bigquery_schema_from_csv(cleaned_csv_path)
        logger.info(f"生成 BigQuery schema: {len(schema)} 個欄位")
        
        # 5. 檢查認證檔案
        if not os.path.exists(args.credential):
            raise FileNotFoundError(f"BigQuery 認證檔案不存在: {args.credential}")
        
        # 6. 如果是 dry-run 模式，只檢查不實際上傳
        if args.dry_run:
            logger.info("=== DRY RUN 模式 ===")
            logger.info(f"CSV 檔案: {csv_path}")
            logger.info(f"目標表格: {MOMO_PROJECT}.{MOMO_DATASET}.{MOMO_TABLE}")
            logger.info(f"上傳模式: {args.mode}")
            logger.info(f"Schema 欄位數: {len(schema)}")
            logger.info("=== DRY RUN 完成 ===")
            return
        
        # 7. 建立 BigQuery 客戶端
        logger.info("建立 BigQuery 客戶端...")
        client = get_bq_client(args.credential)
        
        # 8. 設定上傳模式
        write_disposition_map = {
            "truncate": "WRITE_TRUNCATE",
            "append": "WRITE_APPEND", 
            "empty": "WRITE_EMPTY"
        }
        write_disposition = write_disposition_map[args.mode]
        
        logger.info(f"上傳模式: {write_disposition}")
        
        # 9. 上傳到 BigQuery
        logger.info("開始上傳到 BigQuery...")
        success = upload_csv_to_bq(
            client=client,
            csv_path=cleaned_csv_path,
            dataset_id=MOMO_DATASET,
            table_id=MOMO_TABLE,
            schema=schema,
            write_disposition=write_disposition,
            logger=logger
        )
        
        if success:
            logger.info("✅ MOMO 會計訂單資料上傳成功！")
            logger.info(f"目標表格: {MOMO_PROJECT}.{MOMO_DATASET}.{MOMO_TABLE}")
        else:
            logger.error("❌ MOMO 會計訂單資料上傳失敗！")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ 程式執行失敗: {e}")
        sys.exit(1)
    
    finally:
        logger.info("=== MOMO 會計訂單 BigQuery 上傳器結束 ===")

if __name__ == "__main__":
    main()

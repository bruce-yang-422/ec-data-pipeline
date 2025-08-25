#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETMall BigQuery 上傳器

主要功能：
- 專門上傳 ETMall 訂單資料至 BigQuery
- 自動抓取最新的 ETMall 商品豐富化 CSV 檔案 (03_etmall_orders_product_enriched_*.csv)
- 上傳到 shopee-etl-reporting.yichai_etmall_data.etmall_orders_data
- 自動重複資料檢查與處理
- 完整的日誌記錄與錯誤追蹤
- 支援多種上傳模式 (WRITE_TRUNCATE/APPEND/EMPTY)，預設為覆蓋模式
- 根據 etmall_fields_mapping.json 動態生成 schema

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

# ETMall 專用設定
ETMALL_DATASET = "yichai_etmall_data"
ETMALL_TABLE = "etmall_orders_data"
ETMALL_PROJECT = "shopee-etl-reporting"

def get_csv_pattern():
    """根據當前工作目錄動態設定 CSV 檔案路徑"""
    current_dir = os.getcwd()
    
    # 如果當前目錄是專案根目錄
    if os.path.basename(current_dir) == "ec-data-pipeline":
        return "temp/etmall/etmall_orders_product_enriched_*.csv"
    # 如果當前目錄是 scripts/bigquery_uploader
    elif os.path.basename(current_dir) == "bigquery_uploader":
        return "../../temp/etmall/etmall_orders_product_enriched_*.csv"
    # 其他情況，嘗試相對路徑
    else:
        return "../../temp/etmall/etmall_orders_product_enriched_*.csv"

def get_credential_path():
    """根據當前工作目錄動態設定認證檔案路徑"""
    current_dir = os.getcwd()
    
    # 如果當前目錄是專案根目錄
    if os.path.basename(current_dir) == "ec-data-pipeline":
        return "config/bigquery_uploader_key.json"
    # 如果當前目錄是 scripts/bigquery_uploader
    elif os.path.basename(current_dir) == "bigquery_uploader":
        return "../../config/bigquery_uploader_key.json"
    # 其他情況，嘗試相對路徑
    else:
        return "../../config/bigquery_uploader_key.json"

def get_mapping_path():
    """根據當前工作目錄動態設定欄位映射檔案路徑"""
    current_dir = os.getcwd()
    
    # 如果當前目錄是專案根目錄
    if os.path.basename(current_dir) == "ec-data-pipeline":
        return "config/etmall_fields_mapping.json"
    # 如果當前目錄是 scripts/bigquery_uploader
    elif os.path.basename(current_dir) == "bigquery_uploader":
        return "../../config/etmall_fields_mapping.json"
    # 其他情況，嘗試相對路徑
    else:
        return "../../config/etmall_fields_mapping.json"

def load_field_mapping():
    """載入欄位映射配置"""
    mapping_path = get_mapping_path()
    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"找不到欄位映射檔案：{mapping_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"欄位映射檔案格式錯誤：{e}")

def generate_schema_from_csv_columns(csv_columns: list[str]) -> list[bigquery.SchemaField]:
    """根據實際CSV欄位生成 BigQuery Schema，根據欄位類型設定適當的資料型態"""
    schema: list[bigquery.SchemaField] = []
    
    # 定義欄位類型映射
    field_type_mapping = {
        # 日期類型
        'order_date': 'DATE',
        'order_time': 'TIME',
        'created_at': 'DATETIME',
        'updated_at': 'DATETIME',
        
        # 數字類型
        'quantity': 'INTEGER',
        'product_spec': 'FLOAT',
        'product_weight_g': 'FLOAT',
        
        # 新台幣金額類型
        'unit_price': 'NUMERIC',
        'order_amount': 'NUMERIC',
        'platform_reconciliation_cost': 'NUMERIC',
        'supplier_cost': 'NUMERIC',
        'product_msrp': 'NUMERIC',
        'product_cost': 'NUMERIC',
        'total_amount': 'NUMERIC',
        'discount_amount': 'NUMERIC',
        
        # 布林值類型
        'is_gift': 'BOOLEAN',
        'shop_shop_status': 'BOOLEAN',
        'shop_is_shopee_ad_delivery_enabled': 'BOOLEAN',
        'shop_status': 'BOOLEAN',
        'is_shopee_ad_delivery_enabled': 'BOOLEAN',
        
        # 新增的產品相關欄位
        'category_level_1': 'STRING',
        'category_level_2': 'STRING',
        'brand': 'STRING',
        'series': 'STRING',
        'pet_type': 'STRING',
        'product_name': 'STRING',
        'item_code': 'STRING',
        'sku': 'STRING',
        'tags': 'STRING',
        'spec': 'STRING',
        'unit': 'STRING',
        'origin': 'STRING',
        'supplier_code': 'STRING',
        'supplier': 'STRING',
        
        # 新增的商店相關欄位
        'shop_name': 'STRING',
        'shop_business_model': 'STRING',
        'location': 'STRING',
        'phone': 'STRING',
        'department': 'STRING',
        'manager': 'STRING',
        
        # 其他欄位保持為字串
        'default': 'STRING'
    }
    
    # 為每個實際存在的欄位生成 Schema
    for column_name in csv_columns:
        # 根據欄位名稱決定資料類型
        if column_name in field_type_mapping:
            field_type = field_type_mapping[column_name]
        else:
            field_type = 'STRING'
        
        schema.append(bigquery.SchemaField(column_name, field_type, mode="NULLABLE"))
    
    return schema

def find_latest_etmall_csv():
    """自動抓取最新的 ETMall 產品資料豐富化 CSV 檔案（腳本 10 輸出）"""
    csv_files = glob.glob(get_csv_pattern())
    if not csv_files:
        raise FileNotFoundError(f"找不到符合模式的 CSV 檔案：{get_csv_pattern()}")
    
    # 標準化路徑並按檔案修改時間排序，取最新的
    csv_files = [os.path.normpath(f) for f in csv_files]
    latest_file = max(csv_files, key=os.path.getmtime)
    return latest_file

def setup_logging():
    """設定日誌"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"logs/etmall_to_bigquery_uploader_{timestamp}.log"
    
    # 確保 logs 目錄存在
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def validate_csv_columns(df: pd.DataFrame, field_mapping: dict[str, dict[str, str]]) -> bool:
    """驗證 CSV 檔案的欄位是否符合映射配置"""
    csv_columns = set(df.columns)
    mapping_columns = set(field_mapping.keys())
    
    missing_columns = mapping_columns - csv_columns
    extra_columns = csv_columns - mapping_columns
    
    if missing_columns:
        logging.warning(f"CSV 檔案缺少以下欄位：{missing_columns}")
    
    if extra_columns:
        logging.info(f"CSV 檔案包含額外欄位：{extra_columns}")
    
    return len(missing_columns) == 0

def main():
    """主函數"""
    parser = argparse.ArgumentParser(description="ETMall 訂單資料上傳至 BigQuery")
    parser.add_argument("--credential", default=get_credential_path(),
                       help="GCP 認證 JSON 檔案路徑")
    parser.add_argument("--csv", help="CSV 檔案路徑（不指定則自動抓取最新檔案）")
    parser.add_argument("--project", default=ETMALL_PROJECT, help="BigQuery 專案 ID")
    parser.add_argument("--dataset", default=ETMALL_DATASET, help="BigQuery Dataset 名稱")
    parser.add_argument("--table", default=ETMALL_TABLE, help="BigQuery Table 名稱")
    parser.add_argument("--write_disposition", default="WRITE_TRUNCATE",
                       choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
                       help="上傳模式")
    parser.add_argument("--check_duplicates", action="store_true", default=True,
                       help="檢查 order_sn 重複")
    parser.add_argument("--no_check_duplicates", action="store_true",
                       help="跳過重複檢查")
    
    args = parser.parse_args()
    
    # 設定日誌
    logger = setup_logging()
    logger.info("=== ETMall 訂單資料上傳至 BigQuery ===")
    logger.info(f"目標資料表：{args.project}.{args.dataset}.{args.table}")
    
    # 載入欄位映射（用於驗證）
    try:
        field_mapping = load_field_mapping()
        logger.info("✅ 欄位映射載入成功")
    except Exception as e:
        logger.error(f"❌ 載入欄位映射失敗：{e}")
        return 1
    
    # 自動抓取最新的 CSV 檔案
    if args.csv:
        csv_path = args.csv
        logger.info(f"使用指定 CSV 檔案：{csv_path}")
    else:
        try:
            csv_path = find_latest_etmall_csv()
            logger.info(f"自動抓取最新產品資料豐富化 CSV 檔案（腳本 10 輸出）：{csv_path}")
        except FileNotFoundError as e:
            logger.error(f"❌ 錯誤：{e}")
            return 1
    
    # 檢查檔案是否存在
    if not os.path.exists(csv_path):
        logger.error(f"❌ CSV 檔案不存在：{csv_path}")
        return 1
    
    # 檢查重複設定
    check_duplicates = args.check_duplicates and not args.no_check_duplicates
    
    try:
        # 建立 BigQuery 客戶端
        client = get_bq_client(args.credential)
        logger.info("✅ BigQuery 客戶端建立成功")
        
        # 讀取 CSV 檔案，設定 keep_default_na=False 避免自動識別 NaN
        logger.info("📖 讀取 CSV 檔案...")
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[], na_filter=False)
        logger.info(f"✅ CSV 檔案讀取成功，共 {len(df)} 筆資料")
        
        # 驗證欄位
        if not validate_csv_columns(df, field_mapping):
            logger.warning("⚠️ CSV 檔案欄位與映射配置不完全匹配")
        
        # 檢查重複資料
        if check_duplicates:
            logger.info("🔍 檢查重複資料...")
            duplicate_sns = check_duplicate_order_sn(df)
            if duplicate_sns:
                logger.info(f"發現 {len(duplicate_sns)} 個重複的 order_sn")
            else:
                logger.info("無重複的 order_sn")
        
        # 為 CSV 檔案添加 processing_date 欄位
        logger.info("📝 為 CSV 檔案添加 processing_date 欄位...")
        df['processing_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 處理空值：將所有 nan、None、NULL 等值轉換為空字串
        logger.info("🔧 處理空值，將 nan、None、NULL 轉換為空字串...")
        
        # 先處理 DataFrame 中的空值
        df = df.replace(['nan', 'None', 'NULL', 'NaN', 'NAN', 'null', 'Null'], '')
        
        # 確保所有欄位都是字串類型
        for col in df.columns:
            df[col] = df[col].astype(str)
            # 再次處理，確保轉換後的字串中沒有 'nan' 相關值
            df[col] = df[col].replace(['nan', 'None', 'NULL', 'NaN', 'NAN', 'null', 'Null'], '')
        
        # 檢查是否還有遺漏的空值
        empty_count = 0
        for col in df.columns:
            empty_count += (df[col] == 'nan').sum()
            empty_count += (df[col] == 'None').sum()
            empty_count += (df[col] == 'NULL').sum()
        
        if empty_count > 0:
            logger.warning(f"⚠️ 仍有 {empty_count} 個空值需要處理")
        else:
            logger.info("✅ 所有空值已正確處理")
        
        # 檢查欄位數量
        logger.info(f"添加 processing_date 後，DataFrame 有 {len(df.columns)} 個欄位")
        logger.info(f"前5個欄位：{list(df.columns[:5])}")
        logger.info(f"後5個欄位：{list(df.columns[-5:])}")
        
        # 創建臨時檔案
        temp_csv_path = csv_path.replace('.csv', '_with_processing_date.csv')
        
        # 保存前再次檢查欄位數量
        logger.info(f"保存前，DataFrame 有 {len(df.columns)} 個欄位")
        
        # 使用更安全的方式保存 CSV，確保空值處理
        try:
            df.to_csv(temp_csv_path, index=False, encoding='utf-8-sig', na_rep='')
            logger.info(f"✅ 已創建臨時檔案：{temp_csv_path}")
        except Exception as e:
            logger.error(f"創建臨時檔案失敗：{e}")
            return 1
        
        # 驗證臨時檔案
        try:
            temp_df = pd.read_csv(temp_csv_path, encoding='utf-8-sig')
            logger.info(f"臨時檔案驗證：{len(temp_df.columns)} 個欄位，{len(temp_df)} 行資料")
            logger.info(f"臨時檔案前5個欄位：{list(temp_df.columns[:5])}")
            logger.info(f"臨時檔案後5個欄位：{list(temp_df.columns[-5:])}")
            
            # 檢查 processing_date 欄位是否存在
            if 'processing_date' in temp_df.columns:
                logger.info("✅ processing_date 欄位已正確添加")
            else:
                logger.error("❌ processing_date 欄位遺失")
                return 1
                
            # 根據實際欄位生成 Schema
            schema = generate_schema_from_csv_columns(list(temp_df.columns))
            logger.info(f"✅ 已生成 BigQuery Schema，包含 {len(schema)} 個欄位")
            
            # 顯示 Schema 詳情
            logger.info("📋 BigQuery Schema 詳情：")
            for field in schema:
                logger.info(f"  - {field.name}: {field.field_type} ({field.mode})")
                
        except Exception as e:
            logger.error(f"臨時檔案驗證失敗：{e}")
            return 1
        
        # 上傳資料
        logger.info(f"📤 開始上傳資料...")
        logger.info(f"檔案：{temp_csv_path}")
        logger.info(f"模式：{args.write_disposition}")
        
        result = upload_csv_to_bq(
            client=client,
            csv_path=temp_csv_path,
            dataset_id=args.dataset,
            table_id=args.table,
            schema=schema,
            write_disposition=args.write_disposition
        )
        
        # 清理臨時檔案
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
            logger.info("🧹 已清理臨時檔案")
        
        if result:
            logger.info("✅ 資料上傳成功！")
            logger.info(f"上傳筆數：{result}")
            return 0
        else:
            logger.error("❌ 資料上傳失敗")
            return 1
            
    except Exception as e:
        logger.error(f"❌ 上傳過程中發生錯誤：{str(e)}")
        return 1



if __name__ == "__main__":
    # 直接使用主函數，預設為覆蓋模式
    exit(main())

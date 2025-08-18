#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETMall BigQuery 上傳器

主要功能：
- 專門上傳 ETMall 訂單資料至 BigQuery
- 自動抓取最新的 ETMall CSV 檔案
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
from pathlib import Path

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
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 如果當前目錄是專案根目錄
    if os.path.basename(current_dir) == "ec-data-pipeline":
        return "data_processed/merged/06_etmall_orders_bq_formatted_*.csv"
    # 如果當前目錄是 scripts/bigquery_uploader
    elif os.path.basename(current_dir) == "bigquery_uploader":
        return "../../data_processed/merged/06_etmall_orders_bq_formatted_*.csv"
    # 其他情況，嘗試相對路徑
    else:
        return "../../data_processed/merged/06_etmall_orders_bq_formatted_*.csv"

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

def generate_schema_from_mapping(field_mapping: dict[str, dict[str, str]]) -> list[bigquery.SchemaField]:
    """根據欄位映射生成 BigQuery Schema"""
    schema: list[bigquery.SchemaField] = []
    
    # BigQuery 資料類型映射
    type_mapping = {
        "String": "STRING",
        "Integer": "INTEGER", 
        "Float": "FLOAT",
        "Date": "DATE",
        "Datetime": "DATETIME",
        "Text": "STRING",
        "Boolean": "BOOLEAN"
    }
    
    # 按 order 欄位排序
    sorted_fields = sorted(field_mapping.items(), key=lambda x: int(x[1]['order']))
    
    for field_name, field_info in sorted_fields:
        bq_type = type_mapping.get(field_info['type'], 'STRING')
        # 為了避免資料上傳問題，所有欄位都設為 NULLABLE
        mode = 'NULLABLE'
        
        schema.append(bigquery.SchemaField(field_name, bq_type, mode=mode))
    
    # 添加 processing_date 欄位
    schema.append(bigquery.SchemaField("processing_date", "TIMESTAMP", mode="NULLABLE"))
    
    return schema

def find_latest_etmall_csv():
    """自動抓取最新的 ETMall CSV 檔案"""
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
    
    # 載入欄位映射
    try:
        field_mapping = load_field_mapping()
        logger.info("✅ 欄位映射載入成功")
    except Exception as e:
        logger.error(f"❌ 載入欄位映射失敗：{e}")
        return 1
    
    # 生成 schema
    schema = generate_schema_from_mapping(field_mapping)
    logger.info(f"✅ 已生成 BigQuery Schema，包含 {len(schema)} 個欄位")
    
    # 自動抓取最新的 CSV 檔案
    if args.csv:
        csv_path = args.csv
        logger.info(f"使用指定 CSV 檔案：{csv_path}")
    else:
        try:
            csv_path = find_latest_etmall_csv()
            logger.info(f"自動抓取最新 CSV 檔案：{csv_path}")
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
        
        # 讀取 CSV 檔案
        logger.info("📖 讀取 CSV 檔案...")
        df = pd.read_csv(csv_path, dtype=str)
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
        
        # 修正資料類型問題
        logger.info("🔧 修正資料類型...")
        
        # 根據映射配置處理資料類型
        for field_name, field_info in field_mapping.items():
            if field_name in df.columns:
                field_type = field_info['type']
                
                if field_type == 'Float':
                    # 處理浮點數欄位
                    df[field_name] = df[field_name].fillna('0').astype(float).astype(str)
                elif field_type == 'Integer':
                    # 處理整數欄位
                    df[field_name] = df[field_name].fillna('0').astype(float).astype(int).astype(str)
                elif field_type in ['Date', 'Datetime']:
                    # 處理日期欄位，保持字串格式
                    df[field_name] = df[field_name].fillna('').astype(str)
                else:
                    # 其他欄位保持字串格式，避免科學記號問題
                    df[field_name] = df[field_name].fillna('').astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', regex=False)
        
        # 創建臨時檔案
        temp_csv_path = csv_path.replace('.csv', '_with_processing_date.csv')
        df.to_csv(temp_csv_path, index=False)
        logger.info(f"✅ 已創建臨時檔案：{temp_csv_path}")
        
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

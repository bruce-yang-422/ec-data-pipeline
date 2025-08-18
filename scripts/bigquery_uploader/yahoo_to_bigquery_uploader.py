#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo 訂單 BigQuery 上傳腳本
負責將 yahoo_orders_bq_formatted_*.csv 上傳到 BigQuery
目標表格：shopee-etl-reporting.yichai_yahoo_data.yahoo_orders_data

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from google.cloud import bigquery
from google.oauth2 import service_account

# 設定路徑
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
INPUT_DIR = PROJECT_ROOT / "data_processed" / "merged"
LOGS_DIR = PROJECT_ROOT / "logs"
CONFIG_DIR = PROJECT_ROOT / "config"

# 確保目錄存在
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# BigQuery 設定
PROJECT_ID = "shopee-etl-reporting"
DATASET_ID = "yichai_yahoo_data"
TABLE_ID = "yahoo_orders_data"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# 認證檔案路徑
CREDENTIAL_PATH = CONFIG_DIR / "bigquery_uploader_key.json"

# 設定日誌
def setup_logging():
    """設定日誌"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = LOGS_DIR / f"yahoo_bigquery_uploader_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def get_csv_pattern() -> str:
    """取得 CSV 檔案搜尋模式"""
    return "yahoo_orders_bq_formatted_*.csv"

def find_latest_csv_file(input_dir: Path, pattern: str) -> Optional[Path]:
    """尋找最新的 CSV 檔案"""
    try:
        if not input_dir.exists():
            logging.error(f"輸入目錄不存在：{input_dir}")
            return None
        
        # 尋找所有符合模式的 CSV 檔案
        csv_files = list(input_dir.glob(pattern))
        
        if not csv_files:
            logging.warning(f"在 {input_dir} 中找不到 {pattern} 檔案")
            return None
        
        # 按檔案修改時間排序，取最新的
        latest_file = max(csv_files, key=lambda x: x.stat().st_mtime)
        logging.info(f"找到最新的 CSV 檔案：{latest_file.name}")
        
        return latest_file
        
    except Exception as e:
        logging.error(f"尋找最新 CSV 檔案時發生錯誤：{e}")
        return None

def get_bigquery_client() -> Optional[bigquery.Client]:
    """取得 BigQuery 客戶端"""
    try:
        if CREDENTIAL_PATH.exists():
            credentials = service_account.Credentials.from_service_account_file(
                str(CREDENTIAL_PATH),
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
            logging.info(f"使用認證檔案：{CREDENTIAL_PATH}")
        else:
            # 使用預設認證
            client = bigquery.Client(project=PROJECT_ID)
            logging.info("使用預設認證")
        
        return client
        
    except Exception as e:
        logging.error(f"建立 BigQuery 客戶端時發生錯誤：{e}")
        return None

def generate_schema_from_csv(df: pd.DataFrame) -> List[bigquery.SchemaField]:
    """根據 CSV 資料生成 BigQuery Schema"""
    try:
        schema = []
        
        for column in df.columns:
            # 根據資料內容推斷類型
            sample_data = df[column].dropna().head(100)
            
            if len(sample_data) == 0:
                # 如果沒有資料，預設為 STRING
                schema.append(bigquery.SchemaField(column, "STRING", mode="NULLABLE"))
                continue
            
            # 嘗試推斷類型
            if column in ['order_date', 'return_order_create_date', 'return_completion_date', 
                         'return_case_close_date', 'return_penalty_start_date', 'processing_date']:
                schema.append(bigquery.SchemaField(column, "DATE", mode="NULLABLE"))
            elif column in ['order_transfer_date', 'latest_shipping_date', 'return_processing']:
                schema.append(bigquery.SchemaField(column, "DATETIME", mode="NULLABLE"))
            elif column in ['product_cost', 'cost_subtotal', 'amount_subtotal', 'shipping_fee', 
                           'store_collection_amount', 'weight_g', 'msrp', 'price', 'supplier_price', 
                           'list_price', 'cost', 'quantity']:
                schema.append(bigquery.SchemaField(column, "FLOAT64", mode="NULLABLE"))
            elif column in ['shop_status']:
                schema.append(bigquery.SchemaField(column, "BOOL", mode="NULLABLE"))
            else:
                # 預設為 STRING
                schema.append(bigquery.SchemaField(column, "STRING", mode="NULLABLE"))
        
        logging.info(f"生成 Schema，共 {len(schema)} 個欄位")
        return schema
        
    except Exception as e:
        logging.error(f"生成 Schema 時發生錯誤：{e}")
        return []

def upload_to_bigquery(client: bigquery.Client, csv_file: Path, logger: logging.Logger) -> bool:
    """上傳 CSV 到 BigQuery"""
    try:
        logger.info(f"開始上傳檔案：{csv_file.name}")
        
        # 讀取 CSV 檔案
        logger.info("讀取 CSV 檔案...")
        df = pd.read_csv(csv_file, dtype=str, encoding='utf-8-sig')
        logger.info(f"成功讀取 CSV，共 {len(df)} 行，{len(df.columns)} 個欄位")
        
        # 生成 Schema
        logger.info("生成 BigQuery Schema...")
        schema = generate_schema_from_csv(df)
        if not schema:
            logger.error("無法生成 Schema")
            return False
        
        # 設定作業配置
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # 覆蓋模式
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # 跳過標題行
            autodetect=False,  # 使用自定義 Schema
            field_delimiter=",",
            encoding="UTF-8"
        )
        
        # 開始上傳
        logger.info(f"開始上傳到 BigQuery 表格：{FULL_TABLE_ID}")
        
        with open(csv_file, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                FULL_TABLE_ID,
                job_config=job_config
            )
        
        # 等待作業完成
        logger.info("等待上傳作業完成...")
        job.result()  # 等待作業完成
        
        # 檢查結果
        if job.errors:
            logger.error(f"上傳作業發生錯誤：{job.errors}")
            return False
        
        # 獲取表格資訊
        table = client.get_table(FULL_TABLE_ID)
        logger.info(f"✅ 上傳成功！表格行數：{table.num_rows}")
        
        return True
        
    except Exception as e:
        logger.error(f"上傳到 BigQuery 時發生錯誤：{e}")
        import traceback
        logger.error(f"錯誤詳情：{traceback.format_exc()}")
        return False

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("=== Yahoo 訂單 BigQuery 上傳作業開始 ===")
    
    try:
        # 1. 尋找最新的 CSV 檔案
        logger.info("步驟 1：尋找最新的 CSV 檔案")
        pattern = get_csv_pattern()
        latest_csv = find_latest_csv_file(INPUT_DIR, pattern)
        
        if not latest_csv:
            logger.error("無法找到最新的 CSV 檔案，作業終止")
            return 1
        
        logger.info(f"找到檔案：{latest_csv.name}")
        
        # 2. 建立 BigQuery 客戶端
        logger.info("步驟 2：建立 BigQuery 客戶端")
        client = get_bigquery_client()
        
        if not client:
            logger.error("無法建立 BigQuery 客戶端，作業終止")
            return 1
        
        # 3. 上傳到 BigQuery
        logger.info("步驟 3：上傳到 BigQuery")
        if upload_to_bigquery(client, latest_csv, logger):
            logger.info("✅ BigQuery 上傳作業完成！")
        else:
            logger.error("❌ 上傳失敗")
            return 1
        
        # 4. 輸出結果摘要
        logger.info("=" * 50)
        logger.info("上傳結果摘要")
        logger.info("=" * 50)
        logger.info(f"輸入檔案：{latest_csv.name}")
        logger.info(f"目標表格：{FULL_TABLE_ID}")
        logger.info(f"寫入模式：覆蓋模式 (WRITE_TRUNCATE)")
        logger.info(f"輸入目錄：{INPUT_DIR}")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"作業執行時發生未預期的錯誤：{e}")
        import traceback
        logger.error(f"錯誤詳情：{traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    exit(main())

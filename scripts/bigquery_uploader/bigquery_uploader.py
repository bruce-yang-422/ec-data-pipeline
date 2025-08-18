#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery CSV 上傳器

主要功能：
- 支援單一檔案或批次上傳至 BigQuery
- 自動重複資料檢查與處理
- 互動式操作介面
- 完整的日誌記錄與錯誤追蹤
- 支援多種上傳模式 (WRITE_TRUNCATE/APPEND/EMPTY)

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import argparse
import os
import sys
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# ✅ 將專案根目錄加入 sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# ✅ 使用相對 import
from bigquery_utils import get_bq_client, upload_csv_to_bq, check_duplicate_order_sn
from bq_schemas import (
    c1105_momo_accounting_orders_schema,
    a1102_momo_shipping_orders_schema,
    etmall_orders_schema,
)

# 預設 dataset 與 cleaned 檔案路徑
DEFAULT_DATASET = "yichai_momo_data"
DEFAULT_FILES = {
    "c1105_momo_accounting_orders": "data_processed/merged/momo_accounting_orders_deduplicated.csv",
    "a1102_momo_shipping_orders": "data_processed/merged/momo_shipping_orders_deduplicated.csv",
    "etmall_orders": "data_processed/merged/etmall_orders_bq_formatted_20250807_115715.csv",
}


def setup_logging():
    """設定 BigQuery 上傳器的日誌系統"""
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]  # 向上兩層到達專案根目錄
    logs_dir = project_root / "logs"
    
    # 確保 logs 目錄存在
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    # 設定日誌檔案名稱
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"bigquery_uploader_{timestamp}.log"
    log_path = logs_dir / log_filename
    
    # 設定日誌格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("=== BigQuery 上傳器開始 ===")
    logger.info(f"專案根目錄：{project_root}")
    logger.info(f"日誌檔案：{log_path}")
    
    return logger


def main():
    """BigQuery 上傳器主程式入口點"""
    # 設定日誌
    logger = setup_logging()
    
    parser = argparse.ArgumentParser(description="通用 BigQuery CSV 上傳器")
    parser.add_argument("--credential", type=str, default="config/bigquery_uploader_key.json", help="GCP 認證 JSON 路徑")
    parser.add_argument("--csv", type=str, help="CSV 檔案路徑 (如不指定，將使用預設路徑)")
    parser.add_argument("--dataset", type=str, default=DEFAULT_DATASET, help=f"BigQuery Dataset 名稱 (預設: {DEFAULT_DATASET})")
    parser.add_argument("--table", type=str, help="BigQuery Table 名稱")
    parser.add_argument("--write_disposition", type=str, choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"], default="WRITE_APPEND", help="上傳模式")
    parser.add_argument("--check_duplicates", action="store_true", default=True, help="檢查 order_sn 重複 (預設開啟)")
    parser.add_argument("--no_check_duplicates", action="store_true", help="跳過重複檢查")
    parser.add_argument("--upload_all", action="store_true", help="一鍵上傳所有預設 cleaned 檔案")
    args = parser.parse_args()

    check_duplicates = args.check_duplicates and not args.no_check_duplicates
    logger.info(f"參數設定：dataset={args.dataset}, write_disposition={args.write_disposition}, check_duplicates={check_duplicates}")

    if args.upload_all:
        logger.info("🚀 開始上傳所有預設 cleaned 檔案...")
        for table_name, csv_path in DEFAULT_FILES.items():
            upload_single_file(args.credential, csv_path, args.dataset, table_name, args.write_disposition, check_duplicates, logger)
        logger.info("✅ 所有檔案上傳完成！")
        return

    if not args.table:
        logger.error("❌ 單一上傳模式需指定 --table")
        return

    csv_path = args.csv
    if not csv_path:
        csv_path = DEFAULT_FILES.get(args.table)
        if not csv_path:
            logger.error(f"❌ 未找到 table {args.table} 的預設檔案路徑")
            return
        logger.info(f"📂 使用預設路徑: {csv_path}")

    upload_single_file(args.credential, csv_path, args.dataset, args.table, args.write_disposition, check_duplicates, logger)


def upload_single_file(credential_path: str, csv_path: str, dataset_id: str, table_id: str, write_disposition: str, check_duplicates: bool = True, logger=None) -> None:
    """上傳單一 CSV 檔案至 BigQuery"""
    csv_path = csv_path.replace('/', os.sep)

    if not os.path.exists(csv_path):
        error_msg = f"❌ 找不到 CSV: {csv_path}"
        if logger:
            logger.error(error_msg)
        else:
            print(error_msg)
        return

    schema_dict = {
        "c1105_momo_accounting_orders": c1105_momo_accounting_orders_schema,
        "a1102_momo_shipping_orders": a1102_momo_shipping_orders_schema,
        "etmall_orders": etmall_orders_schema,
    }

    schema = schema_dict.get(table_id)
    if schema is None:
        error_msg = f"❌ 尚未定義 table schema: {table_id}"
        if logger:
            logger.error(error_msg)
        else:
            print(error_msg)
        return

    info_msg = f"📤 準備上傳: {csv_path} -> {dataset_id}.{table_id}"
    if logger:
        logger.info(info_msg)
    else:
        print(info_msg)

    client = get_bq_client(credential_path)

    df = pd.read_csv(csv_path, dtype=str)

    final_write_disposition = write_disposition
    if check_duplicates:
        check_msg = "🔍 檢查 order_sn 重複..."
        if logger:
            logger.info(check_msg)
        else:
            print(check_msg)
        
        duplicates = check_duplicate_order_sn(df)
        if duplicates is not None:
            duplicate_count = len(duplicates)
            warning_msg = f"⚠️ 發現 {duplicate_count} 筆重複 order_sn"
            if logger:
                logger.warning(warning_msg)
            else:
                print(warning_msg)
            
            if write_disposition == "WRITE_APPEND":
                final_write_disposition = "WRITE_TRUNCATE"
                switch_msg = "🔄 自動切換為覆蓋模式 (WRITE_TRUNCATE)"
                if logger:
                    logger.info(switch_msg)
                else:
                    print(switch_msg)
            else:
                mode_msg = f"💡 維持原設定模式: {write_disposition}"
                if logger:
                    logger.info(mode_msg)
                else:
                    print(mode_msg)
        else:
            success_msg = "✅ 無重複 order_sn"
            if logger:
                logger.info(success_msg)
            else:
                print(success_msg)
    else:
        skip_msg = "ℹ️ 跳過重複檢查"
        if logger:
            logger.info(skip_msg)
        else:
            print(skip_msg)

    mode_msg = f"📊 使用模式: {final_write_disposition}"
    if logger:
        logger.info(mode_msg)
    else:
        print(mode_msg)

    upload_csv_to_bq(
        client,
        csv_path,
        dataset_id,
        table_id,
        schema,
        write_disposition=final_write_disposition,
        logger=logger
    )


def interactive_mode():
    """互動式 BigQuery 上傳模式"""
    # 設定日誌
    logger = setup_logging()
    
    print("=== BigQuery 互動式上傳（多平台訂單數據）===")
    print("請選擇要上傳的檔案：")
    print("1. c1105_momo_accounting_orders (Momo 訂單帳務)")
    print("2. a1102_momo_shipping_orders (Momo 訂單出貨)")
    print("3. etmall_orders (ETMall 訂單)")
    print("4. 全部上傳")
    
    choice = input("請輸入數字選擇 [1/2/3/4]：").strip()
    
    # 固定參數
    credential = "config/bigquery_uploader_key.json"
    dataset = "yichai_momo_data"
    write_disposition = "WRITE_TRUNCATE"  # 固定使用 WRITE_TRUNCATE
    check_duplicates = True  # 固定檢查重複
    
    logger.info(f"用戶選擇: {choice}")
    logger.info(f"固定參數: credential={credential}, dataset={dataset}, write_disposition={write_disposition}, check_duplicates={check_duplicates}")
    
    if choice == "1":
        table = "c1105_momo_accounting_orders"
        csv = "data_processed/merged/momo_accounting_orders_deduplicated.csv"
        logger.info(f"準備上傳: {csv} -> {dataset}.{table}")
        print(f"\n準備上傳: {csv} -> {dataset}.{table}")
        upload_single_file(credential, csv, dataset, table, write_disposition, check_duplicates, logger)
    elif choice == "2":
        table = "a1102_momo_shipping_orders"
        csv = "data_processed/merged/momo_shipping_orders_deduplicated.csv"
        logger.info(f"準備上傳: {csv} -> {dataset}.{table}")
        print(f"\n準備上傳: {csv} -> {dataset}.{table}")
        upload_single_file(credential, csv, dataset, table, write_disposition, check_duplicates, logger)
    elif choice == "3":
        table = "etmall_orders"
        csv = "data_processed/merged/etmall_orders_bq_formatted_20250807_115715.csv"
        logger.info(f"準備上傳: {csv} -> {dataset}.{table}")
        print(f"\n準備上傳: {csv} -> {dataset}.{table}")
        upload_single_file(credential, csv, dataset, table, write_disposition, check_duplicates, logger)
    elif choice == "4":
        logger.info("開始上傳所有檔案...")
        print("\n🚀 開始上傳所有檔案...")
        for table_name, csv_path in DEFAULT_FILES.items():
            upload_single_file(credential, csv_path, dataset, table_name, write_disposition, check_duplicates, logger)
        logger.info("所有檔案上傳完成！")
        print("✅ 所有檔案上傳完成！")
    else:
        logger.warning(f"無效選擇: {choice}")
        print("❌ 無效選擇，已取消上傳。")


if __name__ == "__main__":
    # 如果完全沒帶參數，直接進入互動式
    if len(sys.argv) == 1:
        interactive_mode()
    else:
        main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo 訂單 BigQuery 格式轉換腳本
負責將 yahoo_orders_product_enriched_*.csv 轉換成 BigQuery 上傳格式
包含欄位名稱標準化和資料類型轉換

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# 設定路徑
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
TEMP_DIR = PROJECT_ROOT / "temp" / "Yahoo"
OUTPUT_DIR = PROJECT_ROOT / "data_processed" / "merged"
LOGS_DIR = PROJECT_ROOT / "logs"

# 確保目錄存在
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# BigQuery 欄位映射定義
BQ_FIELD_MAPPING = {
    'platform': 'STRING',
    'line_number': 'STRING',
    'order_sn': 'STRING',
    'order_date': 'DATE',
    'order_type': 'STRING',
    'order_status': 'STRING',
    'order_transfer_date': 'DATETIME',
    'latest_shipping_date': 'DATETIME',
    'product_id': 'STRING',
    'product_name': 'STRING',
    'supplier_product_code': 'STRING',
    'product_attribute': 'STRING',
    'product_type': 'STRING',
    'quantity': 'STRING',
    'product_cost': 'FLOAT64',
    'cost_subtotal': 'FLOAT64',
    'amount_subtotal': 'FLOAT64',
    'combo_product': 'STRING',
    'recipient_name': 'STRING',
    'recipient_postal_code': 'STRING',
    'recipient_address': 'STRING',
    'recipient_phone_day': 'STRING',
    'recipient_phone_night': 'STRING',
    'recipient_mobile': 'STRING',
    'recipient_mobile_alt': 'STRING',
    'recipient_phone': 'STRING',
    'orderer_name': 'STRING',
    'orderer_mobile': 'STRING',
    'shipping_method': 'STRING',
    'convenience_store_type': 'STRING',
    'warehouse': 'STRING',
    'shipping_number': 'STRING',
    'delivery_number': 'STRING',
    'shipping_fee': 'FLOAT64',
    'store_collection_amount': 'FLOAT64',
    'pickup_person_name': 'STRING',
    'pickup_person_phone': 'STRING',
    'pickup_person_mobile': 'STRING',
    'pickup_person_postal_code': 'STRING',
    'pickup_person_address': 'STRING',
    'pickup_status': 'STRING',
    'pickup_status_description': 'STRING',
    'return_sequence_number': 'STRING',
    'return_order_number': 'STRING',
    'return_order_create_date': 'DATE',
    'return_completion_date': 'DATE',
    'return_case_close_date': 'DATE',
    'return_penalty_start_date': 'DATE',
    'return_reason': 'STRING',
    'return_reason_note': 'STRING',
    'return_status': 'STRING',
    'return_processing': 'DATETIME',
    'return_warehouse_number': 'STRING',
    'activity_code': 'STRING',
    'supplier_note': 'STRING',
    'shopping_cart_note': 'STRING',
    'processing_date': 'DATE',
    'source_file': 'STRING',
    'shop_id': 'STRING',
    'shop_account': 'STRING',
    'shop_status': 'BOOL',
    'shop_business_model': 'STRING',
    'location': 'STRING',
    'department': 'STRING',
    'manager': 'STRING',
    'category_level_1': 'STRING',
    'category_level_2': 'STRING',
    'brand': 'STRING',
    'series': 'STRING',
    'pet_type': 'STRING',
    'sku': 'STRING',
    'tags': 'STRING',
    'spec': 'STRING',
    'unit': 'STRING',
    'weight_g': 'FLOAT64',
    'package_size': 'STRING',
    'barcode': 'STRING',
    'msrp': 'FLOAT64',
    'price': 'FLOAT64',
    'supplier_price': 'FLOAT64',
    'list_price': 'FLOAT64',
    'cost': 'FLOAT64',
    'stock_status': 'STRING',
    'supplier_code': 'STRING',
    'supplier': 'STRING',
    'supplier_ref': 'STRING'
}

# 設定日誌
def setup_logging():
    """設定日誌"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = LOGS_DIR / f"yahoo_bq_formatter_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def find_latest_product_enriched_file() -> Optional[Path]:
    """尋找 temp/Yahoo 下最新的 yahoo_orders_product_enriched_*.csv 檔案"""
    try:
        if not TEMP_DIR.exists():
            logging.error(f"目錄不存在：{TEMP_DIR}")
            return None
        
        # 尋找所有 yahoo_orders_product_enriched_*.csv 檔案
        pattern = "yahoo_orders_product_enriched_*.csv"
        enriched_files = list(TEMP_DIR.glob(pattern))
        
        if not enriched_files:
            logging.warning(f"在 {TEMP_DIR} 中找不到 {pattern} 檔案")
            return None
        
        # 按檔案修改時間排序，取最新的
        latest_file = max(enriched_files, key=lambda x: x.stat().st_mtime)
        logging.info(f"找到最新的產品豐富檔案：{latest_file.name}")
        
        return latest_file
        
    except Exception as e:
        logging.error(f"尋找最新產品豐富檔案時發生錯誤：{e}")
        return None

def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """根據 BigQuery 欄位映射轉換資料類型"""
    try:
        logging.info("開始轉換資料類型...")
        converted_df = df.copy()
        
        for column, bq_type in BQ_FIELD_MAPPING.items():
            if column in converted_df.columns:
                try:
                    if bq_type == 'STRING':
                        # 轉換為字串，空值保持為空字串，確保數值如 "00" 正確顯示
                        converted_df[column] = converted_df[column].fillna('').astype(str).replace('nan', '').replace('None', '')
                    
                    elif bq_type == 'FLOAT64':
                        # 轉換為浮點數，無法轉換的設為 0.0
                        converted_df[column] = pd.to_numeric(converted_df[column], errors='coerce').fillna(0.0)
                    
                    elif bq_type == 'DATE':
                        # 轉換為日期格式，無法轉換的設為空字串
                        converted_df[column] = pd.to_datetime(converted_df[column], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
                    
                    elif bq_type == 'DATETIME':
                        # 轉換為日期時間格式，無法轉換的設為空字串
                        converted_df[column] = pd.to_datetime(converted_df[column], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
                    
                    elif bq_type == 'BOOL':
                        # 轉換為布林值，無法轉換的設為 False
                        converted_df[column] = converted_df[column].astype(str).str.lower().map({'true': True, 'false': False, '1': True, '0': False}).fillna(False)
                    
                    logging.debug(f"欄位 {column} 轉換為 {bq_type} 完成")
                    
                except Exception as e:
                    logging.warning(f"欄位 {column} 轉換失敗：{e}，保持原始值")
                    continue
            else:
                logging.warning(f"欄位 {column} 在資料中不存在")
        
        logging.info("資料類型轉換完成")
        return converted_df
        
    except Exception as e:
        logging.error(f"轉換資料類型時發生錯誤：{e}")
        return df

def ensure_all_columns(df: pd.DataFrame) -> pd.DataFrame:
    """確保所有 BigQuery 欄位都存在，缺失的用空值填充"""
    try:
        logging.info("檢查並補充缺失欄位...")
        result_df = df.copy()
        
        for column in BQ_FIELD_MAPPING.keys():
            if column not in result_df.columns:
                # 根據 BigQuery 類型設定預設值
                bq_type = BQ_FIELD_MAPPING[column]
                if bq_type == 'FLOAT64':
                    result_df[column] = 0.0
                elif bq_type == 'BOOL':
                    result_df[column] = False
                else:
                    result_df[column] = ''
                logging.info(f"新增缺失欄位：{column} (類型: {bq_type})")
        
        # 重新排列欄位順序，確保與 BQ_FIELD_MAPPING 一致
        ordered_columns = [col for col in BQ_FIELD_MAPPING.keys() if col in result_df.columns]
        result_df = result_df[ordered_columns]
        
        logging.info(f"欄位檢查完成，最終共 {len(result_df.columns)} 個欄位")
        return result_df
        
    except Exception as e:
        logging.error(f"檢查欄位時發生錯誤：{e}")
        return df

def save_bq_formatted_file(df: pd.DataFrame, output_dir: Path, logger: logging.Logger) -> bool:
    """儲存 BigQuery 格式的檔案"""
    try:
        # 生成輸出檔案名稱
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"yahoo_orders_bq_formatted_{timestamp}.csv"
        output_path = output_dir / output_filename
        
        # 儲存檔案
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"BigQuery 格式檔案已儲存至：{output_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"儲存 BigQuery 格式檔案時發生錯誤：{e}")
        return False

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("=== Yahoo 訂單 BigQuery 格式轉換作業開始 ===")
    
    try:
        # 1. 尋找最新的產品豐富檔案
        logger.info("步驟 1：尋找最新的產品豐富檔案")
        latest_enriched_file = find_latest_product_enriched_file()
        
        if not latest_enriched_file:
            logger.error("無法找到最新的產品豐富檔案，作業終止")
            return 1
        
        # 2. 讀取產品豐富檔案
        logger.info("步驟 2：讀取產品豐富檔案")
        try:
            # 嘗試不同的編碼
            encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(latest_enriched_file, dtype=str, encoding=encoding)
                    logger.info(f"使用編碼 {encoding} 成功讀取檔案")
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"編碼 {encoding} 讀取失敗：{e}")
                    continue
            
            if df is None:
                logger.error("無法讀取產品豐富檔案")
                return 1
                
        except Exception as e:
            logger.error(f"讀取產品豐富檔案時發生錯誤：{e}")
            return 1
        
        logger.info(f"成功讀取產品豐富檔案，共 {len(df)} 行，{len(df.columns)} 個欄位")
        logger.info(f"原始欄位：{list(df.columns)}")
        
        # 3. 確保所有必要欄位存在
        logger.info("步驟 3：檢查並補充缺失欄位")
        df_with_all_columns = ensure_all_columns(df)
        
        # 4. 轉換資料類型
        logger.info("步驟 4：轉換資料類型")
        formatted_df = convert_data_types(df_with_all_columns)
        
        # 5. 儲存 BigQuery 格式檔案
        logger.info("步驟 5：儲存 BigQuery 格式檔案")
        if save_bq_formatted_file(formatted_df, OUTPUT_DIR, logger):
            logger.info("✅ BigQuery 格式轉換作業完成！")
        else:
            logger.error("❌ 儲存檔案失敗")
            return 1
        
        # 6. 輸出結果摘要
        logger.info("=" * 50)
        logger.info("處理結果摘要")
        logger.info("=" * 50)
        logger.info(f"輸入檔案：{latest_enriched_file.name}")
        logger.info(f"原始資料：{len(df)} 行，{len(df.columns)} 個欄位")
        logger.info(f"格式化後資料：{len(formatted_df)} 行，{len(formatted_df.columns)} 個欄位")
        logger.info(f"BigQuery 欄位：{len(BQ_FIELD_MAPPING)} 個")
        logger.info(f"輸出目錄：{OUTPUT_DIR}")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"作業執行時發生未預期的錯誤：{e}")
        import traceback
        logger.error(f"錯誤詳情：{traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    exit(main())

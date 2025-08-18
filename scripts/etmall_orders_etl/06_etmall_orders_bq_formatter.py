# scripts/etmall_orders_etl/06_etmall_orders_bq_formatter.py
"""
東森購物訂單資料 BigQuery 格式轉換腳本

功能：
- 自動找到 temp/etmall 下最新的 05_etmall_orders_product_matched_*.csv 檔案
- 根據 BigQuery schema 進行欄位對應和型態轉換
- 將轉換後的資料輸出到 data_processed/merged/ 目錄
- 生成符合 BigQuery 要求的 CSV 檔案

使用方式：
直接執行此腳本
"""

import pandas as pd
from pathlib import Path
import sys
import logging
from datetime import datetime
from typing import Dict
import json

def setup_logging(project_root: Path):
    """
    設定日誌功能，將日誌輸出到檔案和控制台
    並在每次執行前清除舊的日誌檔案
    """
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # 清除舊的日誌檔案
    for log_file in log_dir.glob('etmall_bq_formatter_*.log'):
        try:
            log_file.unlink()
        except OSError as e:
            print(f"錯誤: 無法刪除舊日誌檔案 {log_file} - {e}")
            
    log_filename = f'etmall_bq_formatter_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_path = log_dir / log_filename

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, 'w', 'utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_json_config(file_path: Path) -> dict:
    """
    載入 JSON 配置檔案
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"錯誤：找不到配置檔案 {file_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"錯誤：解析 JSON 檔案失敗 {file_path} - {e}")
        return {}
    except Exception as e:
        logging.exception(f"錯誤：載入配置檔案時發生未知錯誤 {file_path}")
        return {}

def find_latest_product_matched_file(directory: Path) -> Path:
    """
    找到最新的 05_etmall_orders_product_matched 檔案
    """
    pattern = '05_etmall_orders_product_matched_*.csv'
    files = list(directory.glob(pattern))
    
    # 排除暫存檔案（包含 _bq_upload 的檔案）
    files = [f for f in files if '_bq_upload' not in f.name]
    
    if not files:
        raise FileNotFoundError(f'在 {directory} 目錄下找不到符合模式的檔案: {pattern}')
    
    # 根據檔案修改時間排序，取最新的
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    return latest_file

def apply_bigquery_schema(df: pd.DataFrame, mapping: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """
    根據 BigQuery 的欄位要求，對 DataFrame 進行最終的型態轉換
    將中文欄位名稱映射為英文欄位名稱
    """
    df_copy = df.copy()
    
    # 建立中文欄位名稱到英文欄位名稱的映射
    zh_to_en_mapping = {}
    for en_col, config in mapping.items():
        zh_col = config.get('zh_name')
        if zh_col:
            zh_to_en_mapping[zh_col] = en_col
    
    # 重命名中文欄位為英文欄位
    columns_to_rename = {}
    for zh_col in df_copy.columns:
        if zh_col in zh_to_en_mapping:
            columns_to_rename[zh_col] = zh_to_en_mapping[zh_col]
            logging.info(f'將中文欄位 "{zh_col}" 重命名為英文欄位 "{zh_to_en_mapping[zh_col]}"')
    
    df_copy = df_copy.rename(columns=columns_to_rename)
    
    # 確保所有欄位都存在，如果不存在則新增為 None
    for col in mapping.keys():
        if col not in df_copy.columns:
            df_copy[col] = None
            logging.warning(f'新增缺失的欄位: {col}')

    # 重新排列欄位順序
    ordered_columns = list(mapping.keys())
    final_columns = [col for col in ordered_columns if col in df_copy.columns]
    df_copy = df_copy[final_columns]
    
    # 先處理所有欄位的型態轉換
    for col in final_columns:
        col_type = mapping.get(col, {}).get('type')
        try:
            if col_type == 'Integer':
                # 處理 Integer，先轉換為數值，無法轉換的設為 NaN
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                # 將 NaN 轉換為 None，這樣在輸出時會是空字串
                df_copy[col] = df_copy[col].where(pd.notna(df_copy[col]), None)
            elif col_type == 'Float':
                # 處理 Float，先轉換為數值，無法轉換的設為 NaN
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                # 將 NaN 轉換為 None
                df_copy[col] = df_copy[col].where(pd.notna(df_copy[col]), None)
            elif col_type == 'Date':
                # 處理 Date，將其轉換為日期物件，然後格式化為字串
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.strftime('%Y-%m-%d')
                # 將 NaT 轉換為空字串
                df_copy[col] = df_copy[col].fillna('')
            elif col_type == 'TIMESTAMP':
                # 處理 TIMESTAMP，將其轉換為日期時間物件，然後格式化為字串
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                # 將 NaT 轉換為空字串
                df_copy[col] = df_copy[col].fillna('')
            elif col_type == 'String' or col_type == 'Text':
                # 處理 String 和 Text 類型，將 NaN 轉換為空字串
                df_copy[col] = df_copy[col].fillna('')
            else:
                # 其他類型，將 NaN 轉換為空字串
                df_copy[col] = df_copy[col].fillna('')
            
            logging.info(f"欄位 '{col}' 已成功轉換為 BigQuery 要求的 '{col_type}' 型態")
        except Exception as e:
            logging.warning(f"欄位 '{col}' 的型態轉換失敗，保留為字串型態 - {e}")
            df_copy[col] = df_copy[col].astype(str).fillna('')

    return df_copy

def main():
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]
    
    # 設定日誌
    setup_logging(project_root)

    # 設定輸入與輸出目錄
    input_dir = project_root / 'temp' / 'etmall'
    output_dir = project_root / 'data_processed' / 'merged'
    mapping_file = project_root / 'config' / 'etmall_fields_mapping.json'

    # 檢查目錄和配置檔案是否存在
    if not input_dir.exists() or not input_dir.is_dir():
        logging.error(f'錯誤：找不到輸入目錄 {input_dir}')
        sys.exit(1)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    if not mapping_file.exists():
        logging.error(f'錯誤：找不到欄位配置檔案 {mapping_file}')
        sys.exit(1)

    # 清除舊的 BigQuery 格式檔案
    logging.info(f'清除舊的 BigQuery 格式檔案...')
    for old_file in output_dir.glob('06_etmall_orders_bq_formatted_*.csv'):
        try:
            old_file.unlink()
            logging.info(f'已刪除舊檔案：{old_file.name}')
        except OSError as e:
            logging.error(f"錯誤: 無法刪除舊檔案 {old_file.name} - {e}")

    mapping_config = load_json_config(mapping_file)
    if not mapping_config:
        logging.error('無法載入欄位配置，停止執行')
        sys.exit(1)

    logging.info(f'輸入目錄：{input_dir}')
    logging.info(f'輸出目錄：{output_dir}')

    # 找到最新的檔案
    try:
        input_file = find_latest_product_matched_file(input_dir)
        logging.info(f'找到最新檔案：{input_file.name}')
    except FileNotFoundError as e:
        logging.error(f'錯誤：{e}')
        sys.exit(1)

    # 讀取資料
    logging.info(f'\n=== 讀取資料 ===')
    try:
        df = pd.read_csv(input_file, dtype=str)
        logging.info(f'成功讀取檔案，共 {len(df)} 筆資料')
    except Exception as e:
        logging.exception(f'錯誤：讀取檔案失敗：{input_file.name}')
        sys.exit(1)

    # 轉換為 BigQuery 格式
    logging.info(f'\n=== 轉換為 BigQuery 格式 ===')
    final_df = apply_bigquery_schema(df, mapping_config)
    
    # 儲存最終檔案
    output_filename = f'06_etmall_orders_bq_formatted_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    output_path = output_dir / output_filename
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    logging.info(f'\n=== 轉換完成 ===')
    logging.info(f'輸出檔案位置：{output_path}')
    logging.info(f'最終資料筆數：{len(final_df)}')
    logging.info(f'最終欄位數：{len(final_df.columns)}')

if __name__ == '__main__':
    main() 
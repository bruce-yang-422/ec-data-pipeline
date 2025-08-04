# scripts/etmall_orders_etl/03_etmall_merger.py
"""
東森購物訂單資料合併與去重腳本

功能：
- 讀取 temp/etmall 下所有已清理的 CSV 檔案
- 將所有檔案合併為一個 DataFrame
- 根據 'order_line_uid' 欄位去除重複資料
- 根據 BigQuery 欄位要求進行最終型態轉換
- 將最終結果輸出到 temp/etmall/

使用方式：
直接執行此腳本
"""

import pandas as pd
from pathlib import Path
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any
import json

def setup_logging(project_root: Path):
    """
    設定日誌功能，將日誌輸出到檔案和控制台
    並在每次執行前清除舊的日誌檔案
    """
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # 清除舊的日誌檔案
    for log_file in log_dir.glob('etmall_merger_*.log'):
        try:
            log_file.unlink()
        except OSError as e:
            print(f"錯誤: 無法刪除舊日誌檔案 {log_file} - {e}")
            
    log_filename = f'etmall_merger_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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

def apply_bigquery_schema(df: pd.DataFrame, mapping: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """
    根據 BigQuery 的欄位要求，對 DataFrame 進行最終的型態轉換
    """
    df_copy = df.copy()
    
    # 確保所有欄位都存在，如果不存在則新增為 None
    for col in mapping.keys():
        if col not in df_copy.columns:
            df_copy[col] = None

    for col, config in mapping.items():
        if col in df_copy.columns:
            col_type = config.get('type')
            try:
                if col_type == 'INT64':
                    # 處理 INT64，並將無法轉換的設為 NaN
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')
                elif col_type == 'NUMERIC':
                    # 處理 NUMERIC，並將無法轉換的設為 NaN
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                elif col_type == 'DATE':
                    # 處理 DATE，將其轉換為日期物件
                    df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.date
                elif col_type == 'TIMESTAMP':
                    # 處理 TIMESTAMP，將其轉換為日期時間物件
                    df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
                # 'STRING' 類型保持不變
                
                logging.info(f"欄位 '{col}' 已成功轉換為 BigQuery 要求的 '{col_type}' 型態")
            except Exception as e:
                logging.warning(f"欄位 '{col}' 的型態轉換失敗，保留為字串型態 - {e}")
                df_copy[col] = df_copy[col].astype(str)
    
    # 重新排列欄位順序
    ordered_columns = list(mapping.keys())
    final_columns = [col for col in ordered_columns if col in df_copy.columns]
    df_copy = df_copy[final_columns]
    
    # 在輸出前，將所有日期和日期時間物件格式化為字串，並處理 NaN
    for col in final_columns:
        col_type = mapping.get(col, {}).get('type')
        if col_type == 'DATE':
            df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
        elif col_type == 'TIMESTAMP':
            df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    
    # 確保所有 NaN 值都轉換為空字串
    df_copy = df_copy.fillna('')

    return df_copy

def main():
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]
    
    # 設定日誌
    setup_logging(project_root)

    # 設定輸入與輸出目錄
    input_dir = project_root / 'temp' / 'etmall'
    # 修改輸出目錄為 temp/etmall
    output_dir = project_root / 'temp' / 'etmall'
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

    mapping_config = load_json_config(mapping_file)
    if not mapping_config:
        logging.error('無法載入欄位配置，停止執行')
        sys.exit(1)

    logging.info(f'讀取目錄：{input_dir}')
    logging.info(f'輸出目錄：{output_dir}')

    # 搜尋所有 CSV 檔案
    logging.info(f'\n=== 搜尋需要合併的 CSV 檔案 ===')
    csv_files: List[Path] = list(input_dir.glob('*.csv'))

    if not csv_files:
        logging.warning(f'在 {input_dir} 目錄下沒有找到任何 CSV 檔案')
        return

    logging.info(f'找到 {len(csv_files)} 個 CSV 檔案：')
    for file in csv_files:
        logging.info(f'  - {file.name}')

    logging.info(f'\n=== 開始合併與去重 ===')
    
    # 建立一個空的 DataFrame 列表來儲存所有檔案的資料
    dataframes = []

    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, dtype=str)
            dataframes.append(df)
            logging.info(f'已讀取檔案：{file_path.name}')
        except Exception as e:
            logging.exception(f'錯誤：讀取檔案失敗：{file_path.name}')
            continue

    if not dataframes:
        logging.error('沒有成功讀取任何檔案，合併失敗')
        return

    # 合併所有 DataFrame
    merged_df = pd.concat(dataframes, ignore_index=True)
    logging.info(f'已合併所有檔案，總共 {len(merged_df)} 筆資料')

    # 根據 'order_line_uid' 進行去重
    if 'order_line_uid' in merged_df.columns:
        original_count = len(merged_df)
        merged_df.drop_duplicates(subset=['order_line_uid'], keep='first', inplace=True)
        dropped_count = original_count - len(merged_df)
        logging.info(f'已根據 "order_line_uid" 刪除 {dropped_count} 筆重複資料')
    else:
        logging.warning('未找到 "order_line_uid" 欄位，無法進行去重')
        
    logging.info(f'\n=== 轉換為 BigQuery 要求的欄位型態 ===')
    final_df = apply_bigquery_schema(merged_df, mapping_config)
    
    # 儲存最終的合併檔案
    output_filename = f'etmall_orders_merged_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    output_path = output_dir / output_filename
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    logging.info(f'\n=== 合併完成 ===')
    logging.info(f'最終合併後檔案位置：{output_path}')
    logging.info(f'最終資料筆數：{len(final_df)}')

if __name__ == '__main__':
    main()

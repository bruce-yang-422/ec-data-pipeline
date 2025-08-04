# scripts/etmall_orders_etl/02_etmall_orders_cleaner.py
"""
東森購物訂單資料最終清理與合併腳本

功能：
- 讀取由 etmall_xlsx_to_csv.py 產生的 CSV 檔案
- 根據 config/etmall_fields_mapping.json 進行最終的欄位清理與型態轉換
- 建立 '訂單唯一鍵' (order_sn_line_number)
- 數值欄位（數量、售價、成本）保留數字，其餘數字欄位處理為字串
- 日期欄位格式統一為 YYYY-MM-DD
- 將所有清理後的資料合併並輸出到 temp/etmall/
- 在執行後會清除輸出目錄中的舊合併檔案

使用方式：
直接執行此腳本，它會自動處理 data_raw/etmall/ 目錄下所有 .csv 檔案（不含 backup 子目錄），
並將合併後的結果輸出到 temp/etmall/
"""

import pandas as pd
from pathlib import Path
import sys
import re
import json
from datetime import datetime
from typing import Dict, Any, List
import logging

def setup_logging(project_root: Path):
    """
    設定日誌功能，將日誌輸出到檔案和控制台
    並在每次執行前清除舊的日誌檔案
    """
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)

    # 清除舊的日誌檔案
    for log_file in log_dir.glob('etmall_orders_cleaner_*.log'):
        try:
            log_file.unlink()
        except OSError as e:
            print(f"錯誤: 無法刪除舊日誌檔案 {log_file} - {e}")

    log_filename = f'etmall_orders_cleaner_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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

def clean_text(text: Any) -> str:
    """
    清理文字，去除特殊符號、換行符號和多餘空白
    """
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text)
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 此處保留所有標點符號，但去除 emoji 和其他非 ASCII/中文的字元
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text)
    
    return text

def apply_clean_and_transform(df: pd.DataFrame, mapping_config: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """
    根據 mapping 檔案對 DataFrame 進行清理和型別轉換
    """
    # 將 DataFrame 的中文欄位轉換為英文欄位，以利後續處理
    zh_to_en_map = {val['zh_name']: key for key, val in mapping_config.items()}
    df.rename(columns=zh_to_en_map, inplace=True)
    
    # 確保所有 mapping 中的欄位都存在，如果不存在則新增為空欄位
    for en_name in mapping_config.keys():
        if en_name not in df.columns:
            df[en_name] = ''
            logging.warning(f"在檔案中新增缺失的欄位：'{en_name}'")
    
    # 對整個 DataFrame 進行空值處理，並將所有 'nan' 字串替換為空字串
    df = df.fillna('').astype(str).replace('nan', '')

    # 將 'order_date' 欄位的值設定為 'shipping_request_date' 的值
    if 'order_date' in df.columns and 'shipping_request_date' in df.columns:
        df['order_date'] = df['shipping_request_date']
        logging.info("已將 'order_date' 欄位的值設定為 'shipping_request_date'")
    
    # 處理 'platform' 欄位，如果為空則填入 'etmall'
    if 'platform' in df.columns:
        df['platform'] = df['platform'].apply(lambda x: 'etmall' if not x else x)
        logging.info("已將 'platform' 欄位為空的資料填入 'etmall'")

    for col, config in mapping_config.items():
        if col in df.columns:
            # 清理所有文字欄位
            df[col] = df[col].apply(clean_text)
            
            col_type = config.get('type')
            try:
                if col_type == 'INT64' and col in ['quantity']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
                elif col_type == 'NUMERIC' and col in ['unit_price', 'cost']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                elif col_type == 'DATE' or col_type == 'TIMESTAMP':
                    # 增強日期解析，使用一個更強健的正規表示式來擷取日期
                    date_pattern = r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})'
                    df[col] = df[col].str.extract(date_pattern, expand=False).fillna('')
                    converted_series = pd.to_datetime(df[col], errors='coerce')
                    df[col] = converted_series.dt.strftime('%Y-%m-%d').fillna('')
                else:
                    df[col] = df[col].astype(str).replace('nan', '')
            except Exception as e:
                logging.error(f"欄位 '{col}' 的型別轉換失敗 ({col_type}) - {e}")
                df[col] = df[col].astype(str).replace('nan', '')

    # 建立訂單唯一鍵
    if 'order_sn' in df.columns and 'line_number' in df.columns:
        df['order_line_uid'] = df['order_sn'].astype(str) + '_' + df['line_number'].astype(str)
        logging.info("已生成 'order_line_uid' 欄位")
        
    # 重新排列欄位順序
    ordered_columns = list(mapping_config.keys())
    final_columns = [col for col in ordered_columns if col in df.columns]
    df = df[final_columns]

    return df

def main():
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]
    
    # 設定日誌
    setup_logging(project_root)
    
    # 設定輸入與輸出目錄
    input_dir = project_root / 'data_raw' / 'etmall'
    # 輸出目錄
    output_dir = project_root / 'temp' / 'etmall'
    
    # 檢查目錄是否存在
    if not input_dir.exists() or not input_dir.is_dir():
        logging.error(f'錯誤：找不到輸入目錄 {input_dir}')
        sys.exit(1)
    
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    
    # 載入欄位對應配置
    mapping_file = project_root / 'config' / 'etmall_fields_mapping.json'
    mapping_config = load_json_config(mapping_file)
    if not mapping_config:
        logging.error(f'錯誤：無法載入欄位對應配置，停止執行')
        sys.exit(1)

    # 在開始處理前，清除舊的合併檔案
    logging.info(f'清除舊的合併檔案...')
    for old_file in output_dir.glob('etmall_orders_cleaned_*.csv'):
        try:
            old_file.unlink()
            logging.info(f'已刪除舊檔案：{old_file.name}')
        except OSError as e:
            logging.error(f"錯誤: 無法刪除舊合併檔案 {old_file.name} - {e}")
            
    logging.info(f'讀取目錄：{input_dir}')
    logging.info(f'輸出目錄：{output_dir}')
    
    # 搜尋所有 etmall 相關的 CSV 檔案
    logging.info(f'\n=== 搜尋需要清理的 CSV 檔案 ===')
    csv_files: List[Path] = []
    
    try:
        csv_files.extend(input_dir.glob('東森購物_*.csv'))
    except Exception as e:
        logging.exception(f'錯誤：搜尋檔案時發生錯誤')
        sys.exit(1)
        
    if not csv_files:
        logging.warning(f'在 {input_dir} 目錄下沒有找到任何需要清理的 CSV 檔案')
        return
        
    logging.info(f'找到 {len(csv_files)} 個 CSV 檔案：')
    for file in csv_files:
        logging.info(f'  - {file.name}')
    
    logging.info(f'\n=== 開始清理與合併 ===')
    
    # 建立一個空的 DataFrame 來儲存所有清理後的資料
    cleaned_dfs = []
    
    for csv_file in csv_files:
        logging.info(f'\n處理檔案：{csv_file.name}')
        try:
            df = pd.read_csv(csv_file, dtype=str)
            df_cleaned = apply_clean_and_transform(df, mapping_config)
            cleaned_dfs.append(df_cleaned)
            logging.info(f'檔案 {csv_file.name} 清理完成')
        except Exception as e:
            logging.exception(f'錯誤：清理檔案 {csv_file.name} 失敗')
            continue
            
    if not cleaned_dfs:
        logging.error('沒有成功清理任何檔案，停止執行')
        return
    
    # 合併所有清理後的 DataFrame
    logging.info(f'\n=== 合併所有檔案 ===')
    final_df = pd.concat(cleaned_dfs, ignore_index=True)
    logging.info(f'合併完成，總共 {len(final_df)} 筆資料')
    
    # 刪除重複的資料
    if 'order_sn' in final_df.columns and 'order_line_uid' in final_df.columns:
        original_count = len(final_df)
        final_df.drop_duplicates(subset=['order_sn', 'order_line_uid'], keep='first', inplace=True)
        dropped_count = original_count - len(final_df)
        logging.info(f'已刪除 {dropped_count} 筆重複資料')
    
    # 儲存最終的合併檔案
    output_filename = f'etmall_orders_cleaned_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    output_path = output_dir / output_filename
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    logging.info(f'\n=== 清理完成 ===')
    logging.info(f'已輸出合併後檔案：{output_path}')


if __name__ == '__main__':
    main()

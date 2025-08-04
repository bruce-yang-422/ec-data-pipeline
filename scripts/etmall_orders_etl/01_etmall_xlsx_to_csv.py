# scripts/etmall_orders_etl/01_etmall_xlsx_to_csv.py
"""
東森購物訂單資料轉換工具

功能：
- 將東森購物的 Excel 訂單報表轉換為乾淨的 CSV 格式
- 支援多種編碼格式的 Excel 檔案讀取
- 自動處理資料型態轉換與文字清理
- 針對「贈品」欄位進行特殊清理，保留標點符號
- 根據出貨指示日自動命名輸出檔案
- 將原始檔案移到 backup 資料夾

輸入：data_raw/etmall/**/*.xls, data_raw/etmall/**/*.xlsx 檔案
輸出：同路徑的 CSV 檔案（根據出貨指示日命名）
備份：data_raw/etmall/backup/ 資料夾

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import pandas as pd
from pathlib import Path
import sys
import shutil
import re
import json
from datetime import datetime
from typing import Optional, Tuple, List, Any, Dict
import logging

def setup_logging(project_root: Path):
    """
    設定日誌功能，將日誌輸出到檔案和控制台
    並在每次執行前清除舊的日誌檔案
    """
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # 清除舊的日誌檔案
    for log_file in log_dir.glob('etmall_xlsx_to_csv_*.log'):
        try:
            log_file.unlink()
        except OSError as e:
            print(f"錯誤: 無法刪除舊日誌檔案 {log_file} - {e}")
            
    log_filename = f'etmall_xlsx_to_csv_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_path = log_dir / log_filename

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, 'w', 'utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

# 直接在腳本中定義欄位配置
FIELD_CONFIG = {
    "訂單號碼": {"type": "STRING"},
    "訂單項次": {"type": "INT64"},
    "併單序號": {"type": "STRING"},
    "送貨單號": {"type": "STRING"},
    "銷售編號": {"type": "STRING"},
    "商品編號": {"type": "STRING"},
    "商品名稱": {"type": "STRING"},
    "顏色": {"type": "STRING"},
    "款式": {"type": "STRING"},
    "廠商商品號碼": {"type": "STRING"},
    "訂單類別": {"type": "STRING"},
    "數量": {"type": "INT64"},
    "售價": {"type": "NUMERIC"},
    "成本": {"type": "NUMERIC"},
    "客戶名稱": {"type": "STRING"},
    "客戶電話": {"type": "STRING"},
    "室內電話": {"type": "STRING"},
    "配送地址": {"type": "STRING"},
    "貨運公司": {"type": "STRING"},
    "配送單號": {"type": "STRING"},
    "出貨指示日": {"type": "DATE"},
    "要求配送日": {"type": "DATE"},
    "要求配送時間": {"type": "STRING"},
    "備註": {"type": "STRING"},
    "贈品": {"type": "STRING"},
    "廠商配送訊息": {"type": "STRING"},
    "預計入庫日": {"type": "DATE"},
    "預計配送日": {"type": "DATE"},
    "通路別": {"type": "STRING"},
    "訂單類別代號": {"type": "STRING"},
    "公司別": {"type": "STRING"},
}

def clean_text(text: Any) -> str:
    """
    清理文字，去除特殊符號、換行符號和多餘空白
    """
    if pd.isna(text) or text is None:
        return ""
    
    # 轉換為字串
    text = str(text)
    
    # 去除換行符號
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # 去除多餘的空白
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 保留中文字元、英文字母、數字、半形標點符號和空格
    text = re.sub(r'[^\u4e00-\u9fff\w\s\.\,\:\;\?\!\-\(\)\[\]\{\}\/]', '', text)
    
    return text

def clean_gift_info(text: Any) -> str:
    """
    專門用於清理「贈品」欄位，保留所有標點符號
    """
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text)
    
    # 去除換行符號
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # 去除多餘的空白
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 移除 unicode 字符 (如表情符號)
    # 此正則表達式會保留大部分非控制字元，確保標點符號被保留
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text)
    
    return text


def apply_mapping_and_clean(df: pd.DataFrame, mapping: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """
    根據 mapping 檔案對 DataFrame 進行清理和型別轉換
    """
    # 確保所有欄位都存在，如果不存在則新增為空欄位
    for zh_name in mapping.keys():
        if zh_name not in df.columns:
            df[zh_name] = ''
            logging.warning(f"在 Excel 檔案中新增缺失的欄位：'{zh_name}'")

    # 對整個 DataFrame 進行空值處理，並將所有 'nan' 字串替換為空字串
    df = df.fillna('').astype(str).replace('nan', '')
    
    # 定義需要轉換為數字的欄位
    numeric_cols = ['數量', '售價', '成本']

    for col, config in mapping.items():
        if col in df.columns:
            # 針對「贈品」欄位使用專門的清理函數
            if col == '贈品':
                df[col] = df[col].apply(clean_gift_info)
            else:
                # 對所有欄位進行文字清理
                df[col] = df[col].apply(clean_text)
            
            col_type = config.get('type')
            try:
                if col_type == 'INT64' and col in numeric_cols:
                    converted_series = pd.to_numeric(df[col], errors='coerce')
                    df[col] = converted_series.astype('Int64', errors='ignore').astype(str).replace('<NA>', '')
                elif col_type == 'NUMERIC' and col in numeric_cols:
                    converted_series = pd.to_numeric(df[col], errors='coerce')
                    df[col] = converted_series.astype(str).replace('nan', '')
                elif col_type == 'DATE' or col_type == 'TIMESTAMP':
                    # 使用正規表示式擷取日期部分 (YYYY/MM/DD)
                    date_pattern = r'(\d{4}/\d{1,2}/\d{1,2})'
                    df[col] = df[col].str.extract(date_pattern, expand=False).fillna('')
                    
                    # 將擷取到的日期字串轉換為 YYYY-MM-DD 格式
                    converted_series = pd.to_datetime(df[col], errors='coerce', format='%Y/%m/%d')
                    df[col] = converted_series.dt.strftime('%Y-%m-%d').fillna('')
                else:
                    df[col] = df[col].replace('nan', '')
            except Exception as e:
                logging.error(f"欄位 '{col}' 的型別轉換失敗 ({col_type}) - {e}")
                df[col] = df[col].astype(str).replace('nan', '')
    
    # 確保 order_line_uid 被正確生成
    # 這裡假設你的原始 Excel 檔案已經有「訂單號碼」和「訂單項次」
    if '訂單號碼' in df.columns and '訂單項次' in df.columns:
        df['訂單唯一鍵'] = df['訂單號碼'].astype(str) + '-' + df['訂單項次'].astype(str)
        logging.info("已生成 '訂單唯一鍵' 欄位")
    
    # 重新排列欄位順序，與 FIELD_CONFIG 中的順序一致
    ordered_columns = list(mapping.keys())
    final_columns = [col for col in ordered_columns if col in df.columns]
    df = df[final_columns]
    
    return df

def extract_ship_date_from_dataframe(df: pd.DataFrame, excel_file_path: Path) -> Optional[str]:
    """
    從 DataFrame 中提取出貨指示日，若失敗則使用檔案修改日期
    """
    possible_cols = ['出貨指示日']
    
    for col in possible_cols:
        if col in df.columns:
            # 使用正規表示式擷取日期部分
            date_pattern = r'(\d{4}/\d{1,2}/\d{1,2})'
            extracted_dates = df[col].astype(str).str.extract(date_pattern, expand=False)
            
            df_temp = pd.to_datetime(extracted_dates, errors='coerce', format='%Y/%m/%d')
            valid_dates = df_temp.dropna()
            
            if not valid_dates.empty:
                earliest_date = valid_dates.min()
                return earliest_date.strftime('%Y%m%d')
    
    logging.warning(f'無法從 DataFrame 提取出貨指示日，將嘗試使用檔案修改日期：{excel_file_path.name}')
    try:
        # 使用檔案的修改日期
        file_mtime = datetime.fromtimestamp(excel_file_path.stat().st_mtime)
        logging.info(f'成功從檔案中提取修改日期：{file_mtime.strftime("%Y%m%d")}')
        return file_mtime.strftime('%Y%m%d')
    except Exception as e:
        logging.error(f'無法從檔案中提取修改日期：{e}')
        logging.warning(f'無法從 DataFrame 或檔案中提取出貨指示日，將使用當前日期')
        return datetime.now().strftime('%Y%m%d')

def generate_filename(shop_name: str, ship_date: str, sequence: int) -> str:
    """
    生成檔案名稱
    """
    return f"{shop_name}_{ship_date}_{sequence:03d}"

def convert_and_backup_file(input_path: Path, output_dir: Path, backup_dir: Path, shop_name: str, sequence: int, mapping: Dict[str, Dict[str, str]]) -> Tuple[bool, Optional[str]]:
    """
    將單一 xlsx/xls 檔案轉換為 csv，並備份原始檔案
    返回：(是否成功, 生成的檔案名稱)
    """
    try:
        logging.info(f'讀取：{input_path}')
        df = pd.read_excel(input_path, dtype=str)
        logging.info(f'共 {len(df)} 筆資料，{len(df.columns)} 欄')
        
        logging.info('應用欄位映射並清理資料中...')
        df_cleaned = apply_mapping_and_clean(df, mapping)
        logging.info('資料清理與轉換完成')
        
        ship_date = extract_ship_date_from_dataframe(df_cleaned, input_path)
        
        output_filename = generate_filename(shop_name, ship_date, sequence) + '.csv'
        output_path = output_dir / output_filename
        df_cleaned.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f'已輸出 CSV：{output_path}')
        
        backup_filename = generate_filename(shop_name, ship_date, sequence) + input_path.suffix
        backup_path = backup_dir / backup_filename
        
        shutil.move(str(input_path), str(backup_path))
        logging.info(f'已備份原始檔案：{backup_path}')
        
        return True, output_filename
        
    except Exception as e:
        logging.exception(f'轉換或備份失敗：{input_path}')
        return False, None

def main():
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]
    
    # 設定日誌
    setup_logging(project_root)
    
    # 設定輸入與輸出目錄
    input_dir = project_root / 'data_raw' / 'etmall'
    backup_dir = input_dir / 'backup'
    
    if not input_dir.exists() or not input_dir.is_dir():
        logging.error(f'錯誤：找不到目錄 {input_dir}')
        sys.exit(1)
        
    backup_dir.mkdir(exist_ok=True)
    
    logging.info(f'處理目錄：{input_dir}')
    logging.info(f'專案根目錄：{project_root}')
    logging.info(f'輸出目錄：{input_dir} (乾淨的 CSV)')
    logging.info(f'備份目錄：{backup_dir} (原始檔案)')
    
    shop_name = "東森購物"
    
    logging.info(f'\n=== 搜尋 Excel 檔案 ===')
    excel_files: List[Path] = []
    
    try:
        # 使用 pathlib.Path.rglob 來遞迴尋找檔案
        for ext in ['*.xls', '*.xlsx']:
            for file_path in input_dir.rglob(ext):
                # 排除 backup 資料夾
                if 'backup' not in file_path.parts:
                    excel_files.append(file_path)
    except Exception as e:
        logging.exception(f'錯誤：搜尋檔案時發生錯誤')
        sys.exit(1)
    
    if not excel_files:
        logging.warning(f'在 {input_dir} 目錄下沒有找到任何 xls/xlsx 檔案')
        return
    
    logging.info(f'找到 {len(excel_files)} 個 Excel 檔案：')
    for file in excel_files:
        logging.info(f'  - {file}')
    
    logging.info(f'\n=== 開始轉換 ===')
    logging.info(f'商店名稱：{shop_name}')
    
    success_count = 0
    total_count = len(excel_files)
    
    # 引入基於出貨日期的流水號計數器
    date_sequence = {}
    
    for excel_file in excel_files:
        logging.info(f'\n處理檔案 {success_count + 1}/{total_count}：{excel_file.name}')
        
        if not excel_file.exists() or not excel_file.is_file():
            logging.warning(f'檔案不存在或不是檔案，跳過：{excel_file}')
            continue
        
        try:
            temp_df = pd.read_excel(excel_file, dtype=str)
            ship_date = extract_ship_date_from_dataframe(temp_df, excel_file)
        except Exception as e:
            logging.error(f'讀取檔案失敗：{excel_file} - {e}')
            # 讀取失敗時，無法取得日期，使用當前日期來確保流水號不衝突
            ship_date = datetime.now().strftime('%Y%m%d')
        
        # 根據出貨日期更新流水號
        if ship_date not in date_sequence:
            date_sequence[ship_date] = 1
        else:
            date_sequence[ship_date] += 1
        
        sequence = date_sequence[ship_date]
        
        success, _ = convert_and_backup_file(excel_file, input_dir, backup_dir, shop_name, sequence, FIELD_CONFIG)
        
        if success:
            success_count += 1
        else:
            logging.error(f'檔案轉換失敗：{excel_file}')
    
    logging.info(f'\n=== 轉換完成 ===')
    logging.info(f'成功轉換並備份：{success_count}/{total_count} 個檔案')
    logging.info(f'乾淨的 CSV 檔案位置：{input_dir}')
    logging.info(f'備份檔案位置：{backup_dir}')

if __name__ == '__main__':
    main()

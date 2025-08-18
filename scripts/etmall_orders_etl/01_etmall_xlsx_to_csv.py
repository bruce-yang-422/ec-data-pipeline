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

from datetime import datetime
from typing import Optional, Tuple, List, Any, Dict
import logging
import hashlib

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

# 報表類型識別關鍵字
REPORT_TYPE_IDENTIFIERS = {
    "訂單明細報表": ["客戶名稱", "客戶電話", "室內電話", "配送地址", "出貨指示日"],
    "訂單銷售報表": ["訂單日期", "訂單編號", "項次", "配送狀態", "訂單狀態"]
}

# 訂單明細報表欄位配置（原有格式）
ORDER_DETAIL_FIELD_CONFIG = {
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
    "出貨指示日": {"type": "TIMESTAMP"},
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

# 訂單銷售報表欄位配置（新格式）
ORDER_SALES_FIELD_CONFIG = {
    "訂單日期": {"type": "TIMESTAMP"},
    "訂單編號": {"type": "STRING"},
    "項次": {"type": "INT64"},
    "配送狀態": {"type": "STRING"},
    "訂單狀態": {"type": "STRING"},
    "商品屬性": {"type": "STRING"},
    "銷售編號": {"type": "STRING"},
    "子商品銷售編號": {"type": "STRING"},
    "子商品商品編號": {"type": "STRING"},
    "配送方式": {"type": "STRING"},
    "商品名稱": {"type": "STRING"},
    "顏色": {"type": "STRING"},
    "款式": {"type": "STRING"},
    "售價": {"type": "NUMERIC"},
    "成本": {"type": "NUMERIC"},
    "數量": {"type": "INT64"},
    "通路": {"type": "STRING"},
    "配送確認日": {"type": "DATE"},
    "公司": {"type": "STRING"},
}

# 欄位對應：訂單銷售報表 -> 訂單明細報表（統一格式）
SALES_TO_DETAIL_MAPPING = {
    "訂單日期": "出貨指示日",
    "訂單編號": "訂單號碼", 
    "項次": "訂單項次",
    "銷售編號": "銷售編號",
    "子商品商品編號": "商品編號",
    "商品名稱": "商品名稱",
    "顏色": "顏色",
    "款式": "款式",
    "售價": "售價",
    "成本": "成本",
    "數量": "數量",
    "通路": "通路別",
    "公司": "公司別",
    "配送方式": "貨運公司",
    "商品屬性": "訂單類別",
}

def detect_report_type(df: pd.DataFrame) -> Tuple[str, Dict[str, Dict[str, str]]]:
    """
    根據 DataFrame 的欄位名稱識別報表類型
    
    Returns:
        報表類型名稱和對應的欄位配置
    """
    columns = set(df.columns)
    
    for report_type, identifiers in REPORT_TYPE_IDENTIFIERS.items():
        # 檢查是否包含該報表類型的關鍵欄位
        if all(identifier in columns for identifier in identifiers):
            if report_type == "訂單明細報表":
                logging.info(f"識別為：{report_type}（包含客戶資訊）")
                return report_type, ORDER_DETAIL_FIELD_CONFIG
            elif report_type == "訂單銷售報表":
                logging.info(f"識別為：{report_type}（不包含客戶資訊）")
                return report_type, ORDER_SALES_FIELD_CONFIG
    
    # 如果無法識別，預設為訂單明細報表
    logging.warning("無法識別報表類型，預設為訂單明細報表")
    return "訂單明細報表", ORDER_DETAIL_FIELD_CONFIG

def clean_sales_report_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    對訂單銷售報表進行基本的資料清理，保留原始格式
    特別處理：將「訂單日期」拆分為「訂單日期」和「下單時間」兩個欄位
    """
    df_cleaned = df.copy()
    
    # 處理訂單日期欄位拆分
    if '訂單日期' in df_cleaned.columns:
        logging.info('正在拆分訂單日期欄位為日期和時間...')
        
        # 將訂單日期轉換為 datetime
        df_cleaned['訂單日期_原始'] = df_cleaned['訂單日期'].astype(str)
        
        # 嘗試解析日期時間
        try:
            # 處理不同的日期時間格式
            datetime_series = pd.to_datetime(df_cleaned['訂單日期_原始'], errors='coerce')
            
            # 拆分為日期和時間
            df_cleaned['訂單日期'] = datetime_series.dt.date.astype(str)  # 只要日期部分
            df_cleaned['下單時間'] = datetime_series.dt.time.astype(str)  # 只要時間部分
            
            # 移除臨時欄位
            df_cleaned = df_cleaned.drop('訂單日期_原始', axis=1)
            
            # 重新排列欄位順序，讓下單時間緊接在訂單日期後面
            cols = df_cleaned.columns.tolist()
            order_date_idx = cols.index('訂單日期')
            order_time_idx = cols.index('下單時間')
            
            # 移除下單時間從原位置
            cols.pop(order_time_idx)
            # 插入到訂單日期後面
            cols.insert(order_date_idx + 1, '下單時間')
            
            df_cleaned = df_cleaned[cols]
            
            logging.info(f'成功拆分訂單日期欄位，共處理 {len(df_cleaned)} 筆資料')
            
        except Exception as e:
            logging.warning(f'日期時間拆分時發生錯誤：{e}，保留原始格式')
    
    # 基本的資料清理
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            # 清理文字欄位
            df_cleaned[col] = df_cleaned[col].astype(str).apply(clean_text)
    
    # 移除完全空白的列
    df_cleaned = df_cleaned.dropna(how='all')
    
    # 重置索引
    df_cleaned = df_cleaned.reset_index(drop=True)
    
    return df_cleaned

def clean_detail_report_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    對訂單明細報表進行基本的資料清理，保留原始格式
    特別處理：將包含日期時間的欄位拆分為日期和時間兩個欄位
    """
    df_cleaned = df.copy()
    
    # 定義需要拆分的日期時間欄位
    datetime_fields = [
        '出貨指示日',
        '要求配送日',
        '預計入庫日',
        '預計配送日'
    ]
    
    # 處理每個日期時間欄位
    for field in datetime_fields:
        if field in df_cleaned.columns:
            logging.info(f'正在拆分 {field} 欄位為日期和時間...')
            
            # 將欄位轉換為字串
            df_cleaned[f'{field}_原始'] = df_cleaned[field].astype(str)
            
            # 嘗試解析日期時間
            try:
                # 處理不同的日期時間格式
                datetime_series = pd.to_datetime(df_cleaned[f'{field}_原始'], errors='coerce')
                
                # 拆分為日期和時間
                df_cleaned[field] = datetime_series.dt.date.astype(str)  # 只要日期部分
                df_cleaned[f'{field}_時間'] = datetime_series.dt.time.astype(str)  # 只要時間部分
                
                # 移除臨時欄位
                df_cleaned = df_cleaned.drop(f'{field}_原始', axis=1)
                
                # 重新排列欄位順序，讓時間欄位緊接在日期欄位後面
                cols = df_cleaned.columns.tolist()
                date_idx = cols.index(field)
                time_idx = cols.index(f'{field}_時間')
                
                # 移除時間欄位從原位置
                cols.pop(time_idx)
                # 插入到日期欄位後面
                cols.insert(date_idx + 1, f'{field}_時間')
                
                df_cleaned = df_cleaned[cols]
                
                logging.info(f'成功拆分 {field} 欄位，共處理 {len(df_cleaned)} 筆資料')
                
            except Exception as e:
                logging.warning(f'{field} 日期時間拆分時發生錯誤：{e}，保留原始格式')
    
    # 基本的資料清理
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            # 清理文字欄位
            df_cleaned[col] = df_cleaned[col].astype(str).apply(clean_text)
    
    # 移除完全空白的列
    df_cleaned = df_cleaned.dropna(how='all')
    
    # 重置索引
    df_cleaned = df_cleaned.reset_index(drop=True)
    
    return df_cleaned

def convert_sales_to_detail_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    將訂單銷售報表格式轉換為訂單明細報表格式（統一格式）
    """
    df_converted = df.copy()
    
    # 重新命名欄位
    rename_mapping = {}
    for sales_col, detail_col in SALES_TO_DETAIL_MAPPING.items():
        if sales_col in df_converted.columns:
            rename_mapping[sales_col] = detail_col
    
    df_converted = df_converted.rename(columns=rename_mapping)
    logging.info(f"重新命名欄位：{list(rename_mapping.items())}")
    
    # 補齊缺失的欄位（設為空值）
    missing_fields = set(ORDER_DETAIL_FIELD_CONFIG.keys()) - set(df_converted.columns)
    for field in missing_fields:
        df_converted[field] = ""
        logging.info(f"補齊缺失欄位：{field}")
    
    return df_converted

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


def apply_mapping_and_clean(df: pd.DataFrame, mapping: Dict[str, Dict[str, str]], report_type: str = "訂單明細報表") -> pd.DataFrame:
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
            col_type = config.get('type')
            
            # 針對日期和時間欄位，先進行特殊處理，避免資料遺失
            if col_type in ['DATE', 'TIMESTAMP']:
                # 先嘗試直接轉換日期，不進行文字清理
                try:
                    if col_type == 'DATE':
                        # 嘗試多種日期格式
                        date_formats = ['%Y/%m/%d', '%Y-%m-%d', '%Y.%m.%d', '%Y%m%d']
                        converted_series = pd.to_datetime(df[col], errors='coerce', format='%Y/%m/%d')
                        
                        # 如果第一個格式失敗，嘗試其他格式
                        if converted_series.isna().all():
                            for fmt in date_formats[1:]:
                                converted_series = pd.to_datetime(df[col], errors='coerce', format=fmt)
                                if not converted_series.isna().all():
                                    break
                        
                        df[col] = converted_series.dt.strftime('%Y-%m-%d').fillna('')
                        
                    elif col_type == 'TIMESTAMP':
                        # 嘗試多種日期時間格式
                        datetime_formats = [
                            '%Y/%m/%d %H:%M:%S', '%Y/%m/%d %H:%M', '%Y/%m/%d',
                            '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d',
                            '%Y.%m.%d %H:%M:%S', '%Y.%m.%d %H:%M', '%Y.%m.%d'
                        ]
                        
                        converted_series = pd.to_datetime(df[col], errors='coerce', format='%Y/%m/%d %H:%M:%S')
                        
                        # 如果第一個格式失敗，嘗試其他格式
                        if converted_series.isna().all():
                            for fmt in datetime_formats[1:]:
                                converted_series = pd.to_datetime(df[col], errors='coerce', format=fmt)
                                if not converted_series.isna().all():
                                    break
                        
                        # 格式化輸出
                        if converted_series.isna().all():
                            df[col] = ''
                        else:
                            # 檢查是否有時間部分
                            has_time = df[col].str.contains(r'\d{1,2}:\d{1,2}', na=False)
                            df[col] = converted_series.dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
                            # 如果原始資料沒有時間，只保留日期部分
                            df.loc[~has_time, col] = converted_series.dt.strftime('%Y-%m-%d 00:00:00').fillna('')
                    
                    logging.info(f"欄位 '{col}' 日期處理完成，成功轉換 {len(df[col].str.strip().ne(''))} 筆資料")
                    
                except Exception as e:
                    logging.error(f"欄位 '{col}' 的日期轉換失敗 ({col_type}) - {e}")
                    # 如果日期轉換失敗，進行基本的文字清理
                    df[col] = df[col].apply(clean_text)
            
            else:
                # 針對非日期欄位，進行正常的文字清理
                try:
                    if col == '贈品':
                        df[col] = df[col].apply(clean_gift_info)
                    else:
                        df[col] = df[col].apply(clean_text)
                    
                    # 處理數字欄位
                    if col_type == 'INT64' and col in numeric_cols:
                        converted_series = pd.to_numeric(df[col], errors='coerce')
                        df[col] = converted_series.astype('Int64', errors='ignore').astype(str).replace('<NA>', '')
                    elif col_type == 'NUMERIC' and col in numeric_cols:
                        converted_series = pd.to_numeric(df[col], errors='coerce')
                        df[col] = converted_series.astype(str).replace('nan', '')
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
    
    # 新增 platform 欄位，固定為 'etmall'
    df['platform'] = 'etmall'
    logging.info("已新增 'platform' 欄位，設定為 'etmall'")
    
    # 重新排列欄位順序，與 FIELD_CONFIG 中的順序一致
    ordered_columns = list(mapping.keys())
    final_columns = [col for col in ordered_columns if col in df.columns]
    df = df[final_columns]
    
    return df

def extract_ship_date_range_from_dataframe(df: pd.DataFrame, excel_file_path: Path) -> Tuple[str, str]:
    """
    從 DataFrame 中提取出貨指示日的範圍（最早起始日和最晚結束日）
    返回：(最早日期, 最晚日期)
    """
    possible_cols = ['出貨指示日', '訂單日期']
    
    for col in possible_cols:
        if col in df.columns:
            # 嘗試多種日期格式
            date_patterns = [
                r'(\d{4}/\d{1,2}/\d{1,2})',  # 2024/1/1
                r'(\d{4}-\d{1,2}-\d{1,2})',  # 2024-1-1
                r'(\d{4}\.\d{1,2}\.\d{1,2})',  # 2024.1.1
                r'(\d{4}\d{2}\d{2})',  # 20240101
            ]
            
            for pattern in date_patterns:
                extracted_dates = df[col].astype(str).str.extract(pattern, expand=False)
                df_temp = pd.to_datetime(extracted_dates, errors='coerce')
                valid_dates = df_temp.dropna()
                
                if not valid_dates.empty:
                    earliest_date = valid_dates.min()
                    latest_date = valid_dates.max()
                    logging.info(f'成功從欄位 "{col}" 提取出貨指示日範圍：{earliest_date.strftime("%Y%m%d")} 到 {latest_date.strftime("%Y%m%d")}')
                    return earliest_date.strftime('%Y%m%d'), latest_date.strftime('%Y%m%d')
    
    logging.warning(f'無法從 DataFrame 提取出貨指示日，將嘗試使用檔案修改日期：{excel_file_path.name}')
    try:
        # 使用檔案的修改日期
        file_mtime = datetime.fromtimestamp(excel_file_path.stat().st_mtime)
        current_date = file_mtime.strftime('%Y%m%d')
        logging.info(f'成功從檔案中提取修改日期：{current_date}')
        return current_date, current_date
    except Exception as e:
        logging.error(f'無法從檔案中提取修改日期：{e}')
        logging.warning(f'無法從 DataFrame 或檔案中提取出貨指示日，將使用當前日期')
        current_date = datetime.now().strftime('%Y%m%d')
        return current_date, current_date

def generate_filename(shop_name: str, earliest_date: str, latest_date: str, sequence: int, report_type: str = "訂單明細報表") -> str:
    """
    生成檔案名稱，根據報表類型使用不同的命名邏輯
    """
    if report_type == "訂單銷售報表":
        return f"01_{shop_name}_銷售報表_{earliest_date}_{latest_date}_{sequence:03d}"
    else:
        # 訂單明細報表使用原有命名邏輯
        return f"01_{shop_name}_{earliest_date}_{latest_date}_{sequence:03d}"

def convert_and_backup_file(input_path: Path, output_dir: Path, backup_dir: Path, shop_name: str, sequence: int) -> Tuple[bool, Optional[str]]:
    """
    將單一 xlsx/xls 檔案轉換為 csv，並備份原始檔案
    返回：(是否成功, 生成的檔案名稱)
    """
    try:
        logging.info(f'讀取：{input_path}')
        df = pd.read_excel(input_path, dtype=str)
        logging.info(f'共 {len(df)} 筆資料，{len(df.columns)} 欄')
        
        # 識別報表類型
        report_type, field_config = detect_report_type(df)
        
        if report_type == "訂單銷售報表":
            logging.info('訂單銷售報表保留原始格式...')
            # 對於訂單銷售報表，只做基本的資料清理，不做格式轉換
            df_cleaned = clean_sales_report_data(df)
        else:
            logging.info('訂單明細報表，進行日期時間欄位拆分...')
            # 對於訂單明細報表，先進行日期時間欄位拆分，再應用欄位映射
            df_cleaned = clean_detail_report_data(df)
            df_cleaned = apply_mapping_and_clean(df_cleaned, field_config, report_type)
        
        logging.info('資料清理與轉換完成')
        
        ship_date_range = extract_ship_date_range_from_dataframe(df_cleaned, input_path)
        earliest_date, latest_date = ship_date_range
        
        # 根據報表類型生成檔案名稱模式
        if report_type == "訂單銷售報表":
            file_pattern = f"{shop_name}_銷售報表_{earliest_date}_{latest_date}_*.csv"
        else:
            file_pattern = f"{shop_name}_{earliest_date}_{latest_date}_*.csv"
        
        # 檢查是否已存在相同內容的檔案
        content_hash = hashlib.md5(df_cleaned.to_string(index=False).encode('utf-8')).hexdigest()[:8]
        existing_files = list(output_dir.glob(file_pattern))
        
        for existing_file in existing_files:
            try:
                existing_df = pd.read_csv(existing_file, dtype=str)
                existing_content_hash = hashlib.md5(existing_df.to_string(index=False).encode('utf-8')).hexdigest()[:8]
                
                if content_hash == existing_content_hash:
                    logging.warning(f'發現相同內容的檔案已存在：{existing_file.name}')
                    logging.warning(f'跳過處理：{input_path.name}')
                    
                    # 仍然備份原始檔案，但使用現有檔案的名稱
                    backup_filename = existing_file.stem + input_path.suffix
                    backup_path = backup_dir / backup_filename
                    
                    shutil.move(str(input_path), str(backup_path))
                    logging.info(f'已備份原始檔案：{backup_path}')
                    
                    return True, existing_file.name
            except Exception as e:
                logging.warning(f'檢查檔案 {existing_file} 時發生錯誤：{e}')
                continue
        
        output_filename = generate_filename(shop_name, earliest_date, latest_date, sequence, report_type) + '.csv'
        output_path = output_dir / output_filename
        df_cleaned.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f'已輸出 CSV：{output_path}')
        
        backup_filename = generate_filename(shop_name, earliest_date, latest_date, sequence, report_type) + input_path.suffix
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
    
    # 引入基於日期範圍的流水號計數器
    date_range_sequence = {}
    
    for excel_file in excel_files:
        logging.info(f'\n處理檔案 {success_count + 1}/{total_count}：{excel_file.name}')
        
        if not excel_file.exists() or not excel_file.is_file():
            logging.warning(f'檔案不存在或不是檔案，跳過：{excel_file}')
            continue
        
        try:
            temp_df = pd.read_excel(excel_file, dtype=str)
            ship_date_range = extract_ship_date_range_from_dataframe(temp_df, excel_file)
            earliest_date, latest_date = ship_date_range
        except Exception as e:
            logging.error(f'讀取檔案失敗：{excel_file} - {e}')
            # 讀取失敗時，使用當前日期來確保流水號不衝突
            current_date = datetime.now().strftime('%Y%m%d')
            earliest_date, latest_date = current_date, current_date
        
        # 根據日期範圍更新流水號
        date_range_key = f"{earliest_date}_{latest_date}"
        if date_range_key not in date_range_sequence:
            date_range_sequence[date_range_key] = 1
        else:
            date_range_sequence[date_range_key] += 1
        
        sequence = date_range_sequence[date_range_key]
        logging.info(f'檔案流水號：{sequence}')
        
        success, _ = convert_and_backup_file(excel_file, input_dir, backup_dir, shop_name, sequence)
        
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

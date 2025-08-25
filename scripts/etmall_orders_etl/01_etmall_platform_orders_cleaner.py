#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單清洗腳本 - 三步驟版

第一步：轉檔 - 把所有檔案都轉成 .csv，檔名加上8碼流水號
第二步：閱讀內容 - 刪除重複檔案
第三步：重新命名 - 根據命名規則重新命名
"""

import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
import hashlib
import re

def setup_logging() -> None:
    """設定日誌"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def step1_convert_all_files_to_csv(data_raw_dir: Path) -> List[Path]:
    """第一步：轉檔 - 把所有檔案都轉成 .csv，檔名加上8碼流水號"""
    logging.info("=== 第一步：轉檔 ===")
    
    # 尋找所有檔案
    all_files = []
    for pattern in ["*.xls", "*.xlsx", "*.csv"]:
        files = list(data_raw_dir.glob(pattern))
        all_files.extend(files)
    
    if not all_files:
        logging.info("沒有找到需要處理的檔案")
        return []
    
    logging.info(f"找到 {len(all_files)} 個檔案需要轉換")
    
    converted_files = []
    
    for file_path in all_files:
        try:
            logging.info(f"轉換檔案：{file_path.name}")
            
            # 讀取檔案內容
            if file_path.suffix.lower() in ['.xls', '.xlsx']:
                # Excel 檔案
                if file_path.suffix.lower() == '.xlsx':
                    df = pd.read_excel(file_path, engine='openpyxl')
                else:
                    df = pd.read_excel(file_path, engine='xlrd')
            else:
                # CSV 檔案
                df = pd.read_csv(file_path, encoding='utf-8-sig')
            
            # 強制所有欄位轉換為字串類型
            for col in df.columns:
                df[col] = df[col].astype(str)
            
            # 處理空值
            df = df.replace(['nan', 'None', 'NULL', 'null', 'NaN', 'NAN', 'NaT'], '')
            df = df.fillna('')
            
            # 去除換行符號和多餘空白
            for col in df.columns:
                df[col] = df[col].str.replace(r'\n|\r|\r\n', ' ', regex=True)
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                df[col] = df[col].str.strip()
            
            # 生成新檔名：原檔名 + 8碼流水號 + .csv
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            new_filename = f"{file_path.stem}_{timestamp}.csv"
            new_csv_path = data_raw_dir / new_filename
            
            # 儲存為 CSV
            df.to_csv(new_csv_path, index=False, encoding='utf-8-sig', na_rep='')
            logging.info(f"已轉換為：{new_filename}")
            
            # 刪除原始檔案
            file_path.unlink()
            logging.info(f"已刪除原始檔案：{file_path.name}")
            
            converted_files.append(new_csv_path)
            
        except Exception as e:
            logging.error(f"轉換檔案 {file_path.name} 時發生錯誤：{e}")
            continue
    
    logging.info(f"第一步完成，成功轉換 {len(converted_files)} 個檔案")
    return converted_files

def get_file_content_hash(file_path: Path) -> str:
    """計算檔案內容的雜湊值"""
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        # 將 DataFrame 轉換為字串並計算雜湊
        content_str = df.to_string(index=False)
        return hashlib.md5(content_str.encode('utf-8')).hexdigest()
    except Exception as e:
        logging.warning(f"無法讀取檔案 {file_path.name} 計算雜湊：{e}")
        return ""

def step2_remove_duplicate_files(data_raw_dir: Path) -> List[Path]:
    """第二步：閱讀內容 - 刪除重複檔案"""
    logging.info("=== 第二步：刪除重複檔案 ===")
    
    # 尋找所有 CSV 檔案
    csv_files = list(data_raw_dir.glob("*.csv"))
    
    if not csv_files:
        logging.info("沒有找到 CSV 檔案")
        return []
    
    logging.info(f"找到 {len(csv_files)} 個 CSV 檔案")
    
    # 按檔案大小分組
    size_groups = {}
    for file_path in csv_files:
        size = file_path.stat().st_size
        if size not in size_groups:
            size_groups[size] = []
        size_groups[size].append(file_path)
    
    # 處理每個大小組
    unique_files = []
    deleted_count = 0
    
    for size, files in size_groups.items():
        if len(files) == 1:
            # 只有一個檔案，直接保留
            unique_files.append(files[0])
            continue
        
        # 多個檔案，檢查內容
        logging.info(f"檔案大小 {size} 有 {len(files)} 個檔案，檢查內容重複")
        
        # 計算每個檔案的內容雜湊
        hash_groups = {}
        for file_path in files:
            content_hash = get_file_content_hash(file_path)
            if content_hash:
                if content_hash not in hash_groups:
                    hash_groups[content_hash] = []
                hash_groups[content_hash].append(file_path)
        
        # 處理每個雜湊組
        for content_hash, hash_files in hash_groups.items():
            if len(hash_files) == 1:
                # 只有一個檔案，保留
                unique_files.append(hash_files[0])
            else:
                # 多個檔案，保留第一個，刪除其餘
                unique_files.append(hash_files[0])
                for file_path in hash_files[1:]:
                    file_path.unlink()
                    deleted_count += 1
                    logging.info(f"已刪除重複檔案：{file_path.name}")
    
    logging.info(f"第二步完成，刪除 {deleted_count} 個重複檔案，保留 {len(unique_files)} 個唯一檔案")
    return unique_files

def detect_file_type(df: pd.DataFrame) -> str:
    """根據欄位判斷檔案類型"""
    columns = list(df.columns)
    
    # 訂單出貨報表特徵欄位（前8欄）
    order_report_indicators = ['訂單號碼', '訂單項次', '併單序號', '送貨單號', '銷售編號', '商品編號', '商品名稱', '顏色']
    
    # 銷售報表特徵欄位（前8欄）
    sales_report_indicators = ['訂單日期', '訂單編號', '項次', '配送狀態', '訂單狀態', '商品屬性', '銷售編號', '子商品銷售編號']
    
    # 檢查是否為訂單出貨報表
    order_match = sum(1 for col in columns[:8] if col in order_report_indicators)
    if order_match >= 6:  # 至少6個欄位匹配
        return 'order_report'
    
    # 檢查是否為銷售報表
    sales_match = sum(1 for col in columns[:8] if col in sales_report_indicators)
    if sales_match >= 6:  # 至少6個欄位匹配
        return 'sales_report'
    
    # 無法判斷，預設為一般訂單
    return 'general_order'

def extract_date_range(df: pd.DataFrame, file_type: str) -> Tuple[str, str]:
    """提取日期範圍"""
    try:
        if file_type == 'sales_report':
            # 銷售報表：使用訂單日期
            if '訂單日期' in df.columns:
                date_col = '訂單日期'
            else:
                # 使用檔案修改時間
                return datetime.now().strftime('%Y%m%d'), datetime.now().strftime('%Y%m%d')
        else:
            # 訂單出貨報表：使用出貨指示日
            if '出貨指示日' in df.columns:
                date_col = '出貨指示日'
            else:
                # 使用檔案修改時間
                return datetime.now().strftime('%Y%m%d'), datetime.now().strftime('%Y%m%d')
        
        # 提取日期範圍
        dates = pd.to_datetime(df[date_col], errors='coerce').dropna()
        if len(dates) > 0:
            min_date = dates.min().strftime('%Y%m%d')
            max_date = dates.max().strftime('%Y%m%d')
            return min_date, max_date
        else:
            # 無法提取日期，使用檔案修改時間
            return datetime.now().strftime('%Y%m%d'), datetime.now().strftime('%Y%m%d')
            
    except Exception as e:
        logging.warning(f'無法提取日期範圍：{e}')
        # 使用檔案修改時間
        return datetime.now().strftime('%Y%m%d'), datetime.now().strftime('%Y%m%d')

def step3_rename_files_by_rules(data_raw_dir: Path, unique_files: List[Path]) -> None:
    """第三步：根據命名規則重新命名"""
    logging.info("=== 第三步：重新命名檔案 ===")
    
    for file_path in unique_files:
        try:
            logging.info(f"重新命名檔案：{file_path.name}")
            
            # 讀取檔案內容
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            
            # 判斷檔案類型
            file_type = detect_file_type(df)
            logging.info(f"檔案類型：{file_type}")
            
            # 提取日期範圍
            min_date, max_date = extract_date_range(df, file_type)
            logging.info(f"日期範圍：{min_date} 到 {max_date}")
            
            # 生成標準化檔名
            if file_type == 'sales_report':
                new_filename = f"01_東森購物_銷售報表_{min_date}_{max_date}_001.csv"
            elif file_type == 'order_report':
                new_filename = f"01_東森購物_訂單出貨報表_{min_date}_{max_date}_001.csv"
            else:
                new_filename = f"01_東森購物_{min_date}_{max_date}_001.csv"
            
            # 檢查檔名是否已存在，尋找可用的流水號
            final_filename = find_available_filename(new_filename, data_raw_dir)
            
            # 重新命名檔案
            new_file_path = data_raw_dir / final_filename
            file_path.rename(new_file_path)
            logging.info(f"已重新命名為：{final_filename}")
            
        except Exception as e:
            logging.error(f"重新命名檔案 {file_path.name} 時發生錯誤：{e}")
            continue
    
    logging.info("第三步完成：檔案重新命名完成")

def find_available_filename(base_filename: str, data_dir: Path) -> str:
    """尋找可用的檔名"""
    if not (data_dir / base_filename).exists():
        return base_filename
    
    # 檔案已存在，尋找可用的流水號
    name_without_ext = base_filename.replace('.csv', '')
    counter = 2
    while True:
        new_filename = f"{name_without_ext}_{counter:02d}.csv"
        if not (data_dir / new_filename).exists():
            return new_filename
        counter += 1

def main() -> None:
    """主函數 - 三步驟處理"""
    setup_logging()
    
    # 取得專案根目錄
    project_root = Path(__file__).resolve().parents[2]
    data_raw_dir = project_root / 'data_raw' / 'etmall'
    
    logging.info(f'專案根目錄：{project_root}')
    logging.info(f'資料來源目錄：{data_raw_dir}')
    
    # 第一步：轉檔
    converted_files = step1_convert_all_files_to_csv(data_raw_dir)
    
    # 第二步：刪除重複檔案
    unique_files = step2_remove_duplicate_files(data_raw_dir)
    
    # 第三步：重新命名
    step3_rename_files_by_rules(data_raw_dir, unique_files)
    
    logging.info('腳本執行完成！')

if __name__ == '__main__':
    main()
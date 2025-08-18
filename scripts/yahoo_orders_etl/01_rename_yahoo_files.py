#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo 訂單檔案重新命名腳本
負責 data_raw/Yahoo 下批次 orders.csv, delivery, sps orders, retgood 重新命名及備份作業
使用 "轉單日" 當檔案名稱日期參數
命名格式：yahoo_{報表類型}_{轉單日起始日}_{轉單日結束日}.csv

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import sys
import shutil
import logging
import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

# 設定路徑
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
RAW_DIR = PROJECT_ROOT / "data_raw" / "Yahoo"
BACKUP_DIR = PROJECT_ROOT / "data_raw" / "Yahoo" / "backup"
OUTPUT_DIR = PROJECT_ROOT / "data_raw" / "Yahoo"
LOGS_DIR = PROJECT_ROOT / "logs"

# 確保目錄存在
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# 設定日誌
def setup_logging():
    """設定日誌"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = LOGS_DIR / f"yahoo_rename_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def detect_file_type(file_path: Path) -> str:
    """根據檔案內容偵測檔案類型"""
    try:
        if not file_path.exists():
            return 'unknown'
        
        # 嘗試不同的編碼讀取檔案
        encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, dtype=str, encoding=encoding, nrows=5)  # 只讀取前5行
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logging.debug(f"編碼 {encoding} 讀取失敗：{e}")
                continue
        
        if df is None or df.empty:
            logging.warning(f"無法讀取檔案內容：{file_path}")
            return 'unknown'
        
        # 將欄位名稱轉換為小寫字串進行比較
        columns = [str(col).lower() for col in df.columns]
        
        # 首先嘗試從檔案名稱判斷（優先使用檔案名稱，因為某些檔案類型欄位結構相似）
        filename_lower = file_path.name.lower()
        if 'spstorders' in filename_lower or 'sps_orders' in filename_lower:
            return 'sps_orders'
        elif 'retgood' in filename_lower:
            return 'retgood'
        elif 'delivery' in filename_lower:
            return 'delivery'
        elif 'torders' in filename_lower or 'orders' in filename_lower:
            return 'orders'
        
        # 如果檔案名稱無法判斷，則根據欄位內容判斷
        # 檢查是否有退貨相關欄位（優先檢查，因為 retgood 檔案可能也有其他欄位）
        elif '退貨單號' in columns or '退貨單序號' in columns:
            return 'retgood'
        
        # 檢查是否有超商相關欄位
        elif '超商類型' in columns:
            return 'sps_orders'
        
        # 檢查是否有收件人相關欄位（delivery 檔案）
        elif '收件人姓名' in columns and '收件人地址' in columns:
            return 'delivery'
        
        # 檢查是否有訂單相關欄位（orders 檔案）
        elif '訂單編號' in columns and '商品名稱' in columns:
            return 'orders'
        
        # 如果都無法判斷，記錄欄位資訊並返回 unknown
        logging.warning(f"無法判斷檔案類型，欄位：{columns}")
        return 'unknown'
        
    except Exception as e:
        logging.error(f"偵測檔案類型時發生錯誤：{e}")
        return 'unknown'

def extract_transfer_dates_from_csv(file_path: Path) -> Tuple[Optional[str], Optional[str]]:
    """從 CSV 檔案中提取轉單日範圍"""
    try:
        # 嘗試不同的編碼
        encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, dtype=str, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logging.warning(f"編碼 {encoding} 讀取失敗：{e}")
                continue
        
        if df is None:
            logging.error(f"無法讀取檔案：{file_path}")
            return None, None
        
        # 尋找轉單日欄位
        transfer_date_columns = []
        for col in df.columns:
            if '轉單日' in str(col):
                transfer_date_columns.append(col)
        
        if not transfer_date_columns:
            logging.warning(f"找不到轉單日欄位：{file_path}")
            return None, None
        
        # 使用第一個找到的轉單日欄位
        transfer_date_col = transfer_date_columns[0]
        logging.info(f"使用轉單日欄位：{transfer_date_col}")
        
        # 提取轉單日並轉換為日期格式
        transfer_dates = []
        for date_str in df[transfer_date_col].dropna():
            if pd.isna(date_str) or str(date_str).strip() == '':
                continue
            
            try:
                # 嘗試解析日期格式 (2025/08/13 10:13)
                if '/' in str(date_str):
                    date_part = str(date_str).split(' ')[0]  # 取日期部分
                    parsed_date = datetime.strptime(date_part, '%Y/%m/%d')
                    transfer_dates.append(parsed_date)
                else:
                    # 嘗試其他格式
                    parsed_date = pd.to_datetime(date_str, errors='coerce')
                    if pd.notna(parsed_date):
                        transfer_dates.append(parsed_date)
            except Exception as e:
                logging.debug(f"無法解析日期：{date_str}, 錯誤：{e}")
                continue
        
        if not transfer_dates:
            logging.warning(f"無法從轉單日欄位提取有效日期：{file_path}")
            return None, None
        
        # 計算日期範圍
        min_date = min(transfer_dates)
        max_date = max(transfer_dates)
        
        # 格式化為 YYYYMMDD
        min_date_str = min_date.strftime('%Y%m%d')
        max_date_str = max_date.strftime('%Y%m%d')
        
        logging.info(f"轉單日範圍：{min_date_str} ~ {max_date_str}")
        return min_date_str, max_date_str
        
    except Exception as e:
        logging.error(f"提取轉單日時發生錯誤：{e}")
        return None, None

def extract_transfer_dates_from_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """從檔案名稱中提取轉單日"""
    # 嘗試從檔案名稱中提取日期 (例如：20250814torders.csv)
    date_pattern = r'(\d{8})'
    dates = re.findall(date_pattern, filename)
    
    if dates:
        if len(dates) == 1:
            # 只有一個日期，起始和結束都是同一天
            date_str = dates[0]
            logging.info(f"從檔案名稱提取單一日期：{date_str}")
            return date_str, date_str
        elif len(dates) >= 2:
            # 多個日期，取最小和最大
            dates_sorted = sorted(dates)
            min_date = dates_sorted[0]
            max_date = dates_sorted[-1]
            logging.info(f"從檔案名稱提取日期範圍：{min_date} ~ {max_date}")
            return min_date, max_date
    
    logging.warning(f"無法從檔案名稱提取日期：{filename}")
    return None, None

def generate_new_filename(file_path: Path, file_type: str, min_date: str, max_date: str) -> str:
    """生成新的檔案名稱"""
    # 即使只有一天，也顯示完整的日期範圍格式
    return f"yahoo_{file_type}_{min_date}_{max_date}.csv"

def generate_backup_filename(file_type: str, min_date: str, max_date: str) -> str:
    """生成備份檔案名稱，採用與重新命名相同的邏輯"""
    # 備份檔案名稱格式：yahoo_{報表類型}_{轉單日起始日}_{轉單日結束日}_backup.csv
    return f"yahoo_{file_type}_{min_date}_{max_date}_backup.csv"

def is_file_duplicate(file_path: Path, backup_dir: Path, file_type: str, min_date: str, max_date: str, logger: logging.Logger) -> bool:
    """檢查是否已有相同內容的備份檔案"""
    try:
        # 生成標準化的備份檔案名稱
        backup_filename = generate_backup_filename(file_type, min_date, max_date)
        
        # 檢查是否已存在相同名稱的備份檔案
        existing_backup = backup_dir / backup_filename
        if existing_backup.exists():
            logger.info(f"發現重複備份檔案：{backup_filename}")
            return True
        
        # 檢查是否有其他備份檔案包含相同的日期範圍和檔案類型
        for existing_file in backup_dir.glob(f"*_{file_type}_{min_date}_{max_date}.*"):
            logger.info(f"發現重複日期範圍的備份檔案：{existing_file.name}")
            return True
            
        return False
        
    except Exception as e:
        logger.warning(f"檢查重複檔案時發生錯誤：{e}")
        return False

def process_file(file_path: Path, logger: logging.Logger) -> bool:
    """處理單個檔案"""
    try:
        filename = file_path.name
        logger.info(f"處理檔案：{filename}")
        
        # 偵測檔案類型
        file_type = detect_file_type(file_path)
        logger.info(f"檔案類型：{file_type}")
        
        # 提取轉單日
        min_date, max_date = extract_transfer_dates_from_csv(file_path)
        
        if min_date is None or max_date is None:
            # 如果從 CSV 無法提取，嘗試從檔案名稱提取
            min_date, max_date = extract_transfer_dates_from_filename(filename)
            
        if min_date is None or max_date is None:
            # 如果都無法提取，使用當前日期
            current_date = datetime.now().strftime('%Y%m%d')
            logger.warning(f"無法提取轉單日，使用當前日期：{current_date}")
            min_date = max_date = current_date
        
        # 生成新檔案名稱
        new_filename = generate_new_filename(file_path, file_type, min_date, max_date)
        new_file_path = OUTPUT_DIR / new_filename
        
        # 檢查新檔案是否已存在
        if new_file_path.exists():
            logger.warning(f"目標檔案已存在：{new_filename}")
            # 如果檔案已存在，直接覆蓋（因為我們使用轉單日作為唯一標識）
            logger.info(f"將覆蓋已存在的檔案：{new_filename}")
        
        # 檢查是否需要備份（避免重複備份）
        if is_file_duplicate(file_path, BACKUP_DIR, file_type, min_date, max_date, logger):
            logger.info(f"檔案 {filename} 已有備份，跳過備份步驟")
        else:
            # 生成標準化的備份檔案名稱
            backup_filename = generate_backup_filename(file_type, min_date, max_date)
            backup_path = BACKUP_DIR / backup_filename
            
            # 備份原始檔案
            shutil.copy2(file_path, backup_path)
            logger.info(f"原始檔案已備份至：{backup_path}")
        
        # 重新命名檔案
        shutil.move(file_path, new_file_path)
        logger.info(f"檔案已重新命名為：{new_filename}")
        
        return True
        
    except Exception as e:
        logger.error(f"處理檔案 {file_path} 時發生錯誤：{e}")
        return False

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("=== Yahoo 訂單檔案重新命名作業開始 ===")
    
    # 檢查原始目錄
    if not RAW_DIR.exists():
        logger.error(f"原始目錄不存在：{RAW_DIR}")
        return 1
    
    # 尋找所有 CSV 檔案
    csv_files = list(RAW_DIR.glob("*.csv"))
    if not csv_files:
        logger.warning(f"在 {RAW_DIR} 中找不到 CSV 檔案")
        return 0
    
    logger.info(f"找到 {len(csv_files)} 個 CSV 檔案")
    
    # 過濾掉備份目錄中的檔案
    csv_files = [f for f in csv_files if not str(f).startswith(str(BACKUP_DIR))]
    logger.info(f"排除備份目錄後，需處理 {len(csv_files)} 個檔案")
    
    # 處理每個檔案
    success_count = 0
    failed_count = 0
    
    for file_path in csv_files:
        if process_file(file_path, logger):
            success_count += 1
        else:
            failed_count += 1
    
    # 輸出結果摘要
    logger.info("=" * 50)
    logger.info("處理結果摘要")
    logger.info("=" * 50)
    logger.info(f"成功處理：{success_count} 個檔案")
    logger.info(f"處理失敗：{failed_count} 個檔案")
    logger.info(f"總計：{len(csv_files)} 個檔案")
    logger.info(f"備份目錄：{BACKUP_DIR}")
    logger.info("=" * 50)
    
    if failed_count > 0:
        logger.warning("有檔案處理失敗，請檢查日誌")
        return 1
    
    logger.info("✅ 所有檔案處理完成！")
    return 0

if __name__ == "__main__":
    exit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物檔案歸檔腳本 - 按檔案名稱時間移動到年月資料夾

將 data_raw\etmall 下的檔案按檔案名稱中的時間移動到對應的年月資料夾
"""

import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import re
import shutil

def setup_logging():
    """設定日誌"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/etmall_archiver.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def extract_date_from_filename(filename: str) -> datetime:
    """從檔名中提取日期"""
    # 嘗試從檔名中提取日期
    date_patterns = [
        r'(\d{8})',  # YYYYMMDD
        r'(\d{4})_(\d{2})',  # YYYY_MM
        r'(\d{4})-(\d{2})',  # YYYY-MM
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, filename)
        if match:
            if len(match.groups()) == 1:
                # YYYYMMDD 格式
                date_str = match.group(1)
                if len(date_str) == 8:
                    return datetime.strptime(date_str, '%Y%m%d')
            elif len(match.groups()) == 2:
                # YYYY_MM 或 YYYY-MM 格式
                year = match.group(1)
                month = match.group(2)
                return datetime.strptime(f"{year}{month}01", '%Y%m%d')
    
    # 如果無法從檔名提取，返回 None
    return None

def compare_file_content(file1_path: Path, file2_path: Path) -> bool:
    """比較兩個檔案的內容是否相同"""
    try:
        # 讀取兩個檔案的內容
        if file1_path.suffix.lower() in ['.xls', '.xlsx']:
            df1 = pd.read_excel(file1_path)
        else:
            df1 = pd.read_csv(file1_path, encoding='utf-8')
        
        if file2_path.suffix.lower() in ['.xls', '.xlsx']:
            df2 = pd.read_excel(file2_path)
        else:
            df2 = pd.read_csv(file2_path, encoding='utf-8')
        
        # 比較 DataFrame 是否相同
        return df1.equals(df2)
        
    except Exception as e:
        logging.warning(f"無法比較檔案內容 {file1_path} vs {file2_path}: {e}")
        return False

def detect_file_type(file_path: Path) -> str:
    """檢測檔案類型"""
    # 首先根據檔名判斷（優先級最高）
    filename = file_path.name.lower()
    if '訂單出貨報表' in filename or '出貨' in filename:
        return "daily_shipping_orders"
    elif '銷售報表' in filename or '銷售' in filename:
        return "sales_report"
    
    # 如果檔名無法判斷，再根據檔案內容判斷
    try:
        if file_path.suffix.lower() in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path, nrows=5)
        else:
            df = pd.read_csv(file_path, nrows=5, encoding='utf-8')
        
        columns = [str(col).strip() for col in df.columns]
        
        # 訂單出貨報表判定：包含特定欄位
        order_report_indicators = [
            '訂單號碼', '訂單項次', '併單序號', '送貨單號', '銷售編號', 
            '商品編號', '商品名稱', '顏色', '款式', '廠商商品號碼', 
            '訂單類別', '數量', '售價', '成本', '客戶名稱', '客戶電話', 
            '室內電話', '配送地址', '貨運公司', '配送單號', '出貨指示日', 
            '要求配送日', '要求配送時間', '備註', '贈品', '廠商配送訊息', 
            '預計入庫日', '預計配送日', '通路別', '訂單類別代號', '公司別'
        ]
        
        # 銷售報表判定：包含特定欄位
        sales_report_indicators = [
            '訂單日期', '訂單編號', '項次', '配送狀態', '訂單狀態', 
            '商品屬性', '銷售編號', '子商品銷售編號', '子商品商品編號', 
            '配送方式', '商品名稱', '顏色', '款式', '售價', '成本', 
            '數量', '通路', '配送確認日', '公司'
        ]
        
        # 計算匹配數量
        order_match_count = sum(1 for indicator in order_report_indicators if any(indicator in col for col in columns))
        sales_match_count = sum(1 for indicator in sales_report_indicators if any(indicator in col for col in columns))
        
        # 根據匹配數量判斷類型
        if order_match_count >= 6:
            return "daily_shipping_orders"
        elif sales_match_count >= 6:
            return "sales_report"
        else:
            # 如果內容也無法判斷，預設為訂單出貨報表
            return "daily_shipping_orders"
            
    except Exception as e:
        logging.warning(f"無法檢測檔案類型 {file_path}: {e}")
        # 預設為訂單出貨報表
        return "daily_shipping_orders"

def archive_files_to_folders(logger: logging.Logger):
    """將檔案按類型移動到對應的年月資料夾"""
    base_path = Path("data_raw/etmall")
    
    if not base_path.exists():
        logger.error(f"目錄不存在: {base_path}")
        return
    
    # 獲取所有檔案（排除 archive 和 backup 目錄）
    files = []
    for file_path in base_path.rglob("*"):
        if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.xls', '.xlsx']:
            # 排除 archive 和 backup 目錄中的檔案
            if 'archive' not in file_path.parts and 'backup' not in file_path.parts:
                files.append(file_path)
    
    logger.info(f"找到 {len(files)} 個檔案需要歸檔")
    
    created_folders = set()
    moved_count = 0
    skipped_count = 0
    duplicate_content_count = 0
    
    for file_path in files:
        try:
            # 從檔名提取日期
            file_date = extract_date_from_filename(file_path.name)
            if not file_date:
                logger.warning(f"無法從檔名提取日期，跳過: {file_path.name}")
                skipped_count += 1
                continue
            
            # 檢測檔案類型
            file_type = detect_file_type(file_path)
            logger.info(f"📋 檔案類型: {file_path.name} -> {file_type}")
            
            # 確定歸檔目錄：按類型/年/月組織（銷售報表只按年）
            if file_type == "sales_report":
                # 銷售報表：只按年歸檔
                archive_dir = base_path / file_type / str(file_date.year)
            else:
                # 訂單出貨報表：按年/月歸檔
                archive_dir = base_path / file_type / str(file_date.year) / f"{file_date.month:02d}"
            
            # 創建歸檔目錄（如果不存在）
            if not archive_dir.exists():
                archive_dir.mkdir(parents=True, exist_ok=True)
                created_folders.add(str(archive_dir))
                logger.info(f"📁 創建歸檔目錄: {archive_dir}")
            
            # 檢查目標資料夾是否已有同名檔案
            target_file_path = archive_dir / file_path.name
            if target_file_path.exists():
                # 檢查內容是否重複
                if compare_file_content(file_path, target_file_path):
                    logger.warning(f"⚠️ 內容重複，跳過: {file_path.name} (與 {target_file_path} 內容相同)")
                    duplicate_content_count += 1
                    continue
                else:
                    # 內容不同，生成新的檔名
                    counter = 1
                    while (archive_dir / f"{file_path.stem}_{counter:03d}{file_path.suffix}").exists():
                        counter += 1
                    target_file_path = archive_dir / f"{file_path.stem}_{counter:03d}{file_path.suffix}"
                    logger.info(f"📝 內容不同，使用新檔名: {target_file_path.name}")
            
            # 移動檔案到歸檔目錄
            shutil.move(str(file_path), str(target_file_path))
            
            logger.info(f"✅ 已移動: {file_path.name} -> {target_file_path}")
            moved_count += 1
            
        except Exception as e:
            logger.error(f"移動失敗 {file_path}: {e}")
            skipped_count += 1
    
    logger.info(f"✅ 檔案歸檔完成！")
    logger.info(f"   - 成功移動: {moved_count} 個檔案")
    logger.info(f"   - 內容重複跳過: {duplicate_content_count} 個檔案")
    logger.info(f"   - 其他跳過: {skipped_count} 個檔案")
    logger.info(f"   - 創建資料夾: {len(created_folders)} 個")
    
    # 顯示創建的歸檔目錄結構
    logger.info(f"📁 歸檔目錄結構:")
    for type_dir in sorted(base_path.iterdir()):
        if type_dir.is_dir() and type_dir.name not in ['backup', 'archive']:
            logger.info(f"   {type_dir.name}/")
            type_total = 0
            for year_dir in sorted(type_dir.iterdir()):
                if year_dir.is_dir():
                    if type_dir.name == "sales_report":
                        # 銷售報表：只顯示年份和檔案數量
                        year_files = len(list(year_dir.glob("*")))
                        if year_files > 0:
                            logger.info(f"     {year_dir.name}: {year_files} 個檔案")
                            type_total += year_files
                    else:
                        # 訂單出貨報表：顯示年/月和檔案數量
                        year_total = 0
                        for month_dir in sorted(year_dir.iterdir()):
                            if month_dir.is_dir():
                                month_files = len(list(month_dir.glob("*")))
                                if month_files > 0:
                                    logger.info(f"       {year_dir.name}/{month_dir.name}: {month_files} 個檔案")
                                    year_total += month_files
                        if year_total > 0:
                            logger.info(f"     {year_dir.name}/: 總計 {year_total} 個檔案")
                            type_total += year_total
            if type_total > 0:
                logger.info(f"   {type_dir.name}/: 總計 {type_total} 個檔案")

def main():
    """主函數"""
    logger = setup_logging()
    
    logger.info("🚀 開始執行東森購物檔案歸檔腳本")
    
    try:
        archive_files_to_folders(logger)
        logger.info("✅ 歸檔腳本執行完成")
        
    except Exception as e:
        logger.error(f"❌ 歸檔腳本執行失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
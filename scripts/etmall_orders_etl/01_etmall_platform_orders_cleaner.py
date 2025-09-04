#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單清洗腳本 - 三步驟版

第一步：轉檔 - 把所有檔案都轉成 .csv，檔名加上8碼流水號
第二步：閱讀內容 - 刪除重複檔案
第三步：重新命名 - 根據命名規則重新命名

ETL 流程說明：
01_etmall_platform_orders_cleaner.py    - 平台訂單檔案清洗和重新命名
02_etmall_files_archiver.py            - 檔案歸檔到年月資料夾
02_01_etmall_order_report_cleaner.py   - 訂單報表清洗（英文欄位，輸出到temp）
03_etmall_shipping_orders_merger.py    - 訂單出貨報表合併
03_01_etmall_order_report_merger.py    - 訂單報表合併（加入item_no和order_line_uid）
04_etmall_sales_report_merger.py       - 銷售報表合併（包含訂單報表，日期時間分離）
05_etmall_orders_deduplicator.py       - 訂單去重處理
06_etmall_orders_merger.py             - 最終訂單合併（統一日期格式）
07_etmall_orders_datetime_processor.py - 日期時間處理
08_etmall_orders_field_mapper.py       - 欄位映射轉換
09_etmall_orders_shop_enricher.py      - 商店資料豐富
10_etmall_orders_product_enricher.py   - 產品資料豐富
"""

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

def translate_platform_order_report_columns(df: pd.DataFrame) -> pd.DataFrame:
    """翻譯大平台貼單檔案的欄位名稱"""
    try:
        # 定義欄位映射
        column_mapping = {
            'Unnamed: 0': 'delivery_company',  # 空值欄位填入 delivery_company
            '訂單編號': 'order_sn',
            '出貨商品編號': 'seller_product_sn',
            '顏色': 'color',
            '款式': 'style',
            '商品名稱': 'product_name_platform',
            '數量': 'quantity',
            '單價': 'unit_price',
            '提貨人姓名': 'customer_name',
            '提貨人郵遞區號': 'customer_postal_code',
            '提貨人地址': 'shipping_address',
            '提貨人日間電話': 'customer_day_phone',
            '提貨人夜間電話': 'customer_night_phone',
            '提貨人行動電話': 'customer_phone',
            '出貨客戶(平台)名稱': 'platform',
            '出貨客戶(平台)網址': 'platform_url',
            '出貨客戶(平台)客服電話': 'platform_service_phone',
            '備註': 'note',
            '付款方式': 'payment_method',
            '代收總金額': 'total_collection_amount',
            '送達日': 'delivery_date',
            '1:早,2:午,3:晚': 'delivery_time_slot',
            '件數': 'package_count',
            '末五碼': 'last_five_digits',
            '付款方式.1': 'payment_method_alt',
            '發票編號': 'invoice_number',
            '訂單金額': 'order_amount',
            '對帳金額': 'cost_to_platform',
            '成本': 'cost',
            '利潤': 'profit',
            '額外運費': 'extra_shipping_fee',
            '對帳用品編號': 'reconciliation_product_code',
            'Unnamed: 32': 'unnamed_32',
            '訂單日期': 'order_date'
        }
        
        # 翻譯欄位名稱
        df_renamed = df.rename(columns=column_mapping)
        
        # 記錄翻譯結果
        translated_columns = []
        for chinese_col, english_col in column_mapping.items():
            if chinese_col in df.columns:
                translated_columns.append(f"{chinese_col} -> {english_col}")
        
        if translated_columns:
            logging.info("欄位翻譯完成:")
            for translation in translated_columns:
                logging.info(f"  {translation}")
        else:
            logging.info("沒有需要翻譯的欄位")
        
        return df_renamed
        
    except Exception as e:
        logging.error(f"翻譯欄位時發生錯誤: {str(e)}")
        return df

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

def detect_file_type(df: pd.DataFrame, filename: str) -> str:
    """根據欄位和檔名判斷檔案類型"""
    columns = list(df.columns)
    
    # 檢查是否為「大平台貼單」檔案
    if '大平台貼單' in filename or filename.startswith('Etmall_Order_Report_'):
        return 'platform_order_report'
    
    # 大平台貼單特徵欄位
    platform_order_indicators = ['訂單編號', '出貨商品編號', '提貨人姓名', '提貨人地址', '提貨人行動電話', '出貨客戶(平台)名稱', '備註', '對帳金額']
    
    # 訂單出貨報表特徵欄位（前8欄）
    order_report_indicators = ['訂單號碼', '訂單項次', '併單序號', '送貨單號', '銷售編號', '商品編號', '商品名稱', '顏色']
    
    # 銷售報表特徵欄位（前8欄）
    sales_report_indicators = ['訂單日期', '訂單編號', '項次', '配送狀態', '訂單狀態', '商品屬性', '銷售編號', '子商品銷售編號']
    
    # 檢查是否為大平台貼單
    platform_match = sum(1 for col in columns if col in platform_order_indicators)
    if platform_match >= 6:  # 至少6個欄位匹配
        return 'platform_order_report'
    
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

def extract_date_from_filename(filename: str) -> Tuple[str, str]:
    """從檔名提取日期"""
    # 尋找 YYYYMM 格式的日期
    match = re.search(r'(\d{4})(\d{2})', filename)
    if match:
        year = match.group(1)
        month = match.group(2)
        return year, month
    return None, None

def extract_date_range(df: pd.DataFrame, file_type: str, filename: str) -> Tuple[str, str]:
    """提取日期範圍"""
    try:
        # 如果是「大平台貼單」檔案，優先從檔名提取日期
        if file_type == 'platform_order_report':
            year, month = extract_date_from_filename(filename)
            if year and month:
                # 使用檔名中的年月，日期設為該月第一天和最後一天
                from datetime import date
                start_date = date(int(year), int(month), 1)
                if int(month) == 12:
                    end_date = date(int(year) + 1, 1, 1) - date.resolution
                else:
                    end_date = date(int(year), int(month) + 1, 1) - date.resolution
                return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')
        
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
            file_type = detect_file_type(df, file_path.name)
            logging.info(f"檔案類型：{file_type}")
            
            # 提取日期範圍
            min_date, max_date = extract_date_range(df, file_type, file_path.name)
            logging.info(f"日期範圍：{min_date} 到 {max_date}")
            
            # 生成標準化檔名
            if file_type == 'platform_order_report':
                # 「大平台貼單」檔案：重新命名為 Etmall_Order_Report_YYYYMM.csv
                year, month = extract_date_from_filename(file_path.name)
                if year and month:
                    new_filename = f"Etmall_Order_Report_{year}{month}.csv"
                else:
                    # 無法從檔名提取日期，使用一般命名規則
                    new_filename = f"01_東森購物_大平台貼單_{min_date}_{max_date}_001.csv"
                
                # 翻譯欄位名稱
                df = translate_platform_order_report_columns(df)
                logging.info("已翻譯大平台貼單檔案的欄位名稱")
                    
            elif file_type == 'sales_report':
                new_filename = f"01_東森購物_銷售報表_{min_date}_{max_date}_001.csv"
                
            elif file_type == 'order_report':
                new_filename = f"01_東森購物_訂單出貨報表_{min_date}_{max_date}_001.csv"
                
            else:
                new_filename = f"01_東森購物_{min_date}_{max_date}_001.csv"
            
            # 檢查檔名是否已存在，尋找可用的流水號
            final_filename = find_available_filename(new_filename, data_raw_dir)
            
            # 保存翻譯後的 DataFrame（如果是 platform_order_report）
            if file_type == 'platform_order_report':
                new_file_path = data_raw_dir / final_filename
                df.to_csv(new_file_path, index=False, encoding='utf-8-sig', na_rep='')
                # 刪除原始檔案
                file_path.unlink()
                logging.info(f"已重新命名並翻譯為：{final_filename}")
            else:
                # 其他類型檔案直接重新命名
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
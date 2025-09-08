#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單合併腳本
合併銷售報表和出貨報表，以銷售報表為主檔，order_line_uid 為 key 匹配
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import glob

# 設定專案根目錄
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(project_root / 'logs' / 'etmall_orders_merger.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def find_latest_merged_files(temp_dir: Path) -> tuple:
    """
    尋找最新的合併檔案
    
    Args:
        temp_dir: 臨時目錄
        
    Returns:
        tuple: (銷售報表檔案路徑, 出貨報表檔案路徑)
    """
    sales_report_pattern = str(temp_dir / "etmall_sales_report_merged_*.csv")
    shipping_orders_pattern = str(temp_dir / "etmall_shipping_orders_merged_*.csv")
    
    # 尋找銷售報表檔案
    sales_files = glob.glob(sales_report_pattern)
    if not sales_files:
        logging.error(f"找不到銷售報表檔案：{sales_report_pattern}")
        return None, None
    
    # 尋找出貨報表檔案
    shipping_files = glob.glob(shipping_orders_pattern)
    if not shipping_files:
        logging.error(f"找不到出貨報表檔案：{shipping_orders_pattern}")
        return None, None
    
    # 選擇最新的檔案（按檔案名稱中的時間戳）
    latest_sales_file = max(sales_files, key=lambda x: Path(x).stem.split('_')[-1])
    latest_shipping_file = max(shipping_files, key=lambda x: Path(x).stem.split('_')[-1])
    
    logging.info(f"找到最新銷售報表檔案：{Path(latest_sales_file).name}")
    logging.info(f"找到最新出貨報表檔案：{Path(latest_shipping_file).name}")
    
    return latest_sales_file, latest_shipping_file

def load_and_validate_files(sales_file: str, shipping_file: str) -> tuple:
    """
    載入並驗證檔案
    
    Args:
        sales_file: 銷售報表檔案路徑
        shipping_file: 出貨報表檔案路徑
        
    Returns:
        tuple: (銷售報表DataFrame, 出貨報表DataFrame)
    """
    try:
        # 載入銷售報表
        logging.info("載入銷售報表...")
        sales_df = pd.read_csv(sales_file, encoding='utf-8-sig', dtype=str)
        logging.info(f"銷售報表載入成功：{len(sales_df)} 筆，{len(sales_df.columns)} 欄位")
        
        # 載入出貨報表
        logging.info("載入出貨報表...")
        shipping_df = pd.read_csv(shipping_file, encoding='utf-8-sig', dtype=str)
        logging.info(f"出貨報表載入成功：{len(shipping_df)} 筆，{len(shipping_df.columns)} 欄位")
        
        # 檢查必要的欄位
        if 'order_line_uid' not in sales_df.columns:
            logging.error("銷售報表缺少 order_line_uid 欄位")
            return None, None
            
        if 'order_line_uid' not in shipping_df.columns:
            logging.error("出貨報表缺少 order_line_uid 欄位")
            return None, None
        
        return sales_df, shipping_df
        
    except Exception as e:
        logging.error(f"載入檔案失敗：{str(e)}")
        return None, None

def merge_orders(sales_df: pd.DataFrame, shipping_df: pd.DataFrame, output_dir: Path) -> str:
    """
    合併訂單資料
    
    Args:
        sales_df: 銷售報表DataFrame
        shipping_df: 出貨報表DataFrame
        output_dir: 輸出目錄
        
    Returns:
        str: 輸出檔案路徑
    """
    logging.info("開始合併訂單資料...")
    
    # 建立輸出目錄
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 以銷售報表為主檔，複製所有欄位
    merged_df = sales_df.copy()
    logging.info(f"主檔（銷售報表）資料筆數：{len(merged_df)} 筆")
    
    # 找出銷售報表中為空值的欄位
    empty_columns = []
    for col in merged_df.columns:
        if merged_df[col].isna().all() or (merged_df[col] == '').all():
            empty_columns.append(col)
    
    logging.info(f"發現空值欄位：{len(empty_columns)} 個")
    if empty_columns:
        logging.info(f"空值欄位列表：{empty_columns[:10]}...")  # 只顯示前10個
    
    # 檢查出貨報表中是否有這些空值欄位
    available_columns = []
    for col in empty_columns:
        if col in shipping_df.columns:
            available_columns.append(col)
    
    logging.info(f"出貨報表中可用的欄位：{len(available_columns)} 個")
    if available_columns:
        logging.info(f"可用欄位列表：{available_columns[:10]}...")  # 只顯示前10個
    
    # 建立出貨報表的複合索引（以 訂單號碼 + 項次 為 key）
    shipping_df['composite_key'] = shipping_df['訂單號碼'].astype(str) + '_' + shipping_df['訂單項次'].astype(str)
    shipping_indexed = shipping_df.set_index('composite_key')
    logging.info(f"出貨報表複合索引建立完成：{len(shipping_indexed)} 筆")
    
    # 統計匹配情況
    matched_count = 0
    filled_count = 0
    
    # 顯示一些範例進行比較
    sales_sample = merged_df['order_line_uid'].head(5).tolist()
    shipping_sample = list(shipping_indexed.index[:5])
    logging.info(f"銷售報表 order_line_uid 範例：{sales_sample}")
    logging.info(f"出貨報表複合索引範例：{shipping_sample}")
    
    # 逐筆處理銷售報表資料
    for idx, row in merged_df.iterrows():
        order_sn = str(row['order_sn'])
        item_no = str(row['item_no'])
        composite_key = f"{order_sn}_{item_no}"
        
        # 檢查出貨報表中是否有對應的複合索引
        if composite_key in shipping_indexed.index:
            matched_count += 1
            shipping_row = shipping_indexed.loc[composite_key]
            
            # 填補空值欄位
            for col in available_columns:
                if pd.isna(merged_df.at[idx, col]) or merged_df.at[idx, col] == '':
                    if not pd.isna(shipping_row[col]) and shipping_row[col] != '':
                        merged_df.at[idx, col] = shipping_row[col]
                        filled_count += 1
    
    # 分析訂單號碼範圍
    sales_order_sns = set(merged_df['order_sn'].astype(str))
    shipping_order_sns = set(shipping_df['訂單號碼'].astype(str))
    
    # 找出重疊的訂單號碼
    overlapping_orders = sales_order_sns.intersection(shipping_order_sns)
    logging.info(f"銷售報表訂單號碼範圍：{min(sales_order_sns)} - {max(sales_order_sns)}")
    logging.info(f"出貨報表訂單號碼範圍：{min(shipping_order_sns)} - {max(shipping_order_sns)}")
    logging.info(f"重疊的訂單號碼：{len(overlapping_orders)} 個")
    
    if overlapping_orders:
        logging.info(f"重疊訂單範例：{list(overlapping_orders)[:5]}")
    
    logging.info(f"匹配成功的訂單：{matched_count} 筆")
    logging.info(f"填補的空值：{filled_count} 個")
    
    # 處理 item_no 格式（個位數前面補0）
    if 'item_no' in merged_df.columns:
        merged_df['item_no'] = merged_df['item_no'].astype(str).str.zfill(2)
        logging.info("已處理 item_no 格式（個位數前面補0）")
    
    # 按照 order_sn 和 item_no 排序
    if 'order_sn' in merged_df.columns and 'item_no' in merged_df.columns:
        # 轉換為數值型態進行排序
        merged_df['order_sn_numeric'] = pd.to_numeric(merged_df['order_sn'], errors='coerce')
        merged_df['item_no_numeric'] = pd.to_numeric(merged_df['item_no'], errors='coerce')
        
        # 排序
        merged_df = merged_df.sort_values(['order_sn_numeric', 'item_no_numeric'], ascending=[True, True])
        
        # 移除臨時欄位
        merged_df = merged_df.drop(['order_sn_numeric', 'item_no_numeric'], axis=1)
        
        logging.info("已按照 order_sn 和 item_no 排序（由小到大）")
    
    # 生成輸出檔案名稱
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"etmall_orders_merged_{timestamp}.csv"
    output_path = output_dir / output_filename
    
    # 儲存合併後的資料
    try:
        merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f"✅ 合併完成：{output_filename}")
        logging.info(f"   輸出位置：{output_path}")
        logging.info(f"   最終資料筆數：{len(merged_df)} 筆")
        logging.info(f"   欄位數量：{len(merged_df.columns)} 個")
        
        return str(output_path)
        
    except Exception as e:
        logging.error(f"儲存檔案失敗：{str(e)}")
        return None

def cleanup_input_files(sales_file: str, shipping_file: str):
    """
    清理輸入檔案
    
    Args:
        sales_file: 銷售報表檔案路徑
        shipping_file: 出貨報表檔案路徑
    """
    deleted_count = 0
    files_to_delete = [Path(sales_file), Path(shipping_file)]
    
    for file_path in files_to_delete:
        try:
            if file_path.exists():
                file_path.unlink()
                logging.info(f"已刪除輸入檔案：{file_path.name}")
                deleted_count += 1
        except Exception as e:
            logging.warning(f"刪除檔案失敗：{file_path.name} - {str(e)}")
    
    logging.info(f"總共刪除 {deleted_count} 個輸入檔案")

def cleanup_old_merged_files(temp_dir: Path, keep_latest: bool = True):
    """
    清理 temp\\etmall 目錄下的舊合併檔案，只保留最新的
    
    Args:
        temp_dir: temp\\etmall 目錄路徑
        keep_latest: 是否保留最新的檔案
    """
    if not temp_dir.exists():
        logging.warning(f"目錄不存在：{temp_dir}")
        return
    
    # 尋找所有合併檔案
    merged_files = []
    
    # 尋找銷售報表合併檔案
    sales_files = list(temp_dir.glob("etmall_sales_report_merged_*.csv"))
    merged_files.extend(sales_files)
    
    # 尋找出貨報表合併檔案
    shipping_files = list(temp_dir.glob("etmall_shipping_orders_merged_*.csv"))
    merged_files.extend(shipping_files)
    
    # 尋找訂單合併檔案
    order_files = list(temp_dir.glob("etmall_orders_merged_*.csv"))
    merged_files.extend(order_files)
    
    if not merged_files:
        logging.info("沒有找到需要清理的合併檔案")
        return
    
    # 按修改時間排序，最新的在最後
    merged_files.sort(key=lambda x: x.stat().st_mtime)
    
    if keep_latest:
        # 保留最新的檔案
        files_to_delete = merged_files[:-1]  # 除了最後一個（最新的）
        files_to_keep = merged_files[-1:]    # 只保留最新的
    else:
        # 刪除所有檔案
        files_to_delete = merged_files
        files_to_keep = []
    
    deleted_count = 0
    for file_path in files_to_delete:
        try:
            if file_path.exists():
                file_path.unlink()
                logging.info(f"已刪除舊檔案：{file_path.name}")
                deleted_count += 1
        except Exception as e:
            logging.warning(f"刪除檔案失敗：{file_path.name} - {str(e)}")
    
    if files_to_keep:
        logging.info(f"保留最新檔案：{files_to_keep[0].name}")
    
    logging.info(f"總共刪除 {deleted_count} 個舊合併檔案")

def main():
    """主函數"""
    logging.info("=" * 50)
    logging.info("開始執行東森購物訂單合併腳本")
    logging.info("=" * 50)
    
    # 設定路徑
    temp_dir = project_root / "temp" / "etmall"
    output_dir = project_root / "temp" / "etmall"
    
    logging.info(f"專案根目錄：{project_root}")
    logging.info(f"臨時目錄：{temp_dir}")
    logging.info(f"輸出目錄：{output_dir}")
    
    # 尋找最新的合併檔案
    sales_file, shipping_file = find_latest_merged_files(temp_dir)
    
    if not sales_file or not shipping_file:
        logging.error("找不到必要的合併檔案，程式結束")
        return
    
    # 載入並驗證檔案
    sales_df, shipping_df = load_and_validate_files(sales_file, shipping_file)
    
    if sales_df is None or shipping_df is None:
        logging.error("檔案載入失敗，程式結束")
        return
    
    # 合併訂單資料
    output_path = merge_orders(sales_df, shipping_df, output_dir)
    
    if output_path:
        logging.info("=" * 50)
        logging.info("📊 合併結果總結：")
        logging.info(f"   - 主檔（銷售報表）：{Path(sales_file).name}")
        logging.info(f"   - 輔檔（出貨報表）：{Path(shipping_file).name}")
        logging.info(f"   - 輸出檔案：{Path(output_path).name}")
        logging.info(f"   - 輸出位置：{output_path}")
        logging.info("✅ 訂單合併完成！")
        logging.info("=" * 50)
        
        # 清理 temp 目錄下的處理後檔案
        logging.info("開始清理 temp 目錄下的處理後檔案...")
        cleanup_input_files(sales_file, shipping_file)
        
        # 清理 temp\etmall 目錄下的舊合併檔案，只保留最新的
        logging.info("開始清理 temp\\etmall 目錄下的舊合併檔案...")
        cleanup_old_merged_files(temp_dir, keep_latest=True)
    else:
        logging.error("❌ 合併失敗！")

if __name__ == "__main__":
    main()

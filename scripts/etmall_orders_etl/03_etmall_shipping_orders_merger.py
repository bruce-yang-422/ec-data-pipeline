#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單出貨報表合併腳本
合併 data_raw/etmall/daily_shipping_orders 目錄下的所有 CSV 檔案
並在 "訂單項次" 和 "併單序號" 之間新增 "訂單ID" 欄位
訂單ID 格式：訂單號碼 + "_" + 訂單項次 (例如：231241430_01)
"""

import os
import pandas as pd
import glob
from pathlib import Path
import logging
from datetime import datetime

# 設定日誌
def setup_logging():
    """設定日誌配置"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"etmall_shipping_orders_merger_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def find_csv_files(base_dir):
    """遞迴搜尋指定目錄下的所有 CSV 檔案"""
    csv_files = []
    
    # 搜尋所有子目錄
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    return sorted(csv_files)

def process_csv_file(file_path, logger):
    """處理單一 CSV 檔案"""
    try:
        logger.info(f"正在處理檔案: {file_path}")
        
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # 檢查必要欄位是否存在
        required_columns = ['訂單號碼', '訂單項次', '併單序號']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"檔案 {file_path} 缺少必要欄位: {missing_columns}")
            return None
        
        # 新增訂單ID欄位
        # 將訂單項次轉換為兩位數格式 (例如：1 -> 01, 2 -> 02)
        df['訂單ID'] = df['訂單號碼'].astype(str) + '_' + df['訂單項次'].astype(str).str.zfill(2)
        
        # 重新排列欄位順序：在 "訂單項次" 和 "併單序號" 之間插入 "訂單ID"
        columns = list(df.columns)
        order_index = columns.index('訂單項次')
        
        # 移除訂單ID欄位（如果已存在）
        if '訂單ID' in columns:
            columns.remove('訂單ID')
        
        # 在訂單項次後面插入訂單ID
        columns.insert(order_index + 1, '訂單ID')
        
        # 重新排列欄位
        df = df[columns]
        
        logger.info(f"檔案 {file_path} 處理完成，共 {len(df)} 筆資料")
        return df
        
    except Exception as e:
        logger.error(f"處理檔案 {file_path} 時發生錯誤: {str(e)}")
        return None

def merge_csv_files(csv_files, logger):
    """合併所有 CSV 檔案"""
    all_dataframes = []
    total_rows = 0
    
    for file_path in csv_files:
        df = process_csv_file(file_path, logger)
        if df is not None:
            all_dataframes.append(df)
            total_rows += len(df)
    
    if not all_dataframes:
        logger.error("沒有成功處理任何 CSV 檔案")
        return None
    
    # 合併所有資料框
    logger.info("正在合併所有資料框...")
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    logger.info(f"合併完成！總共處理 {len(csv_files)} 個檔案，合計 {total_rows} 筆資料")
    return merged_df

def save_merged_file(merged_df, output_dir, logger):
    """儲存合併後的檔案"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"etmall_shipping_orders_merged_{timestamp}.csv"
    
    try:
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"合併檔案已儲存至: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"儲存檔案時發生錯誤: {str(e)}")
        return None

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("開始執行東森購物訂單出貨報表合併腳本")
    
    # 設定路徑
    base_dir = "data_raw/etmall/daily_shipping_orders"
    output_dir = "temp/etmall"
    
    # 檢查輸入目錄是否存在
    if not os.path.exists(base_dir):
        logger.error(f"輸入目錄不存在: {base_dir}")
        return
    
    # 搜尋所有 CSV 檔案
    logger.info(f"正在搜尋目錄: {base_dir}")
    csv_files = find_csv_files(base_dir)
    
    if not csv_files:
        logger.warning(f"在目錄 {base_dir} 中未找到任何 CSV 檔案")
        return
    
    logger.info(f"找到 {len(csv_files)} 個 CSV 檔案")
    
    # 處理並合併所有 CSV 檔案
    merged_df = merge_csv_files(csv_files, logger)
    
    if merged_df is None:
        logger.error("合併失敗")
        return
    
    # 顯示合併後的欄位資訊
    logger.info("合併後的欄位順序:")
    for i, col in enumerate(merged_df.columns):
        logger.info(f"  {i+1:2d}. {col}")
    
    # 顯示前幾筆資料的訂單ID範例
    logger.info("訂單ID 範例:")
    sample_data = merged_df[['訂單號碼', '訂單項次', '訂單ID']].head(10)
    for _, row in sample_data.iterrows():
        logger.info(f"  訂單號碼: {row['訂單號碼']}, 訂單項次: {row['訂單項次']}, 訂單ID: {row['訂單ID']}")
    
    # 儲存合併後的檔案
    output_file = save_merged_file(merged_df, output_dir, logger)
    
    if output_file:
        logger.info("腳本執行完成！")
        logger.info(f"輸出檔案: {output_file}")
        logger.info(f"總資料筆數: {len(merged_df)}")
    else:
        logger.error("腳本執行失敗")

if __name__ == "__main__":
    main()

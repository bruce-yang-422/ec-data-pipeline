#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單去重腳本
找尋 03 和 04 腳本的最新檔案，並以訂單ID為key進行去重處理
分別輸出不同的檔案
"""

import os
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import glob

# 設定日誌
def setup_logging():
    """設定日誌配置"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"etmall_orders_deduplicator_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def find_latest_file(pattern, logger):
    """找尋符合模式的最新檔案"""
    try:
        files = glob.glob(pattern)
        if not files:
            logger.warning(f"未找到符合模式的檔案: {pattern}")
            return None
        
        # 按檔案修改時間排序，取最新的
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"找到最新檔案: {latest_file}")
        return latest_file
        
    except Exception as e:
        logger.error(f"找尋檔案時發生錯誤: {str(e)}")
        return None

def load_and_deduplicate_file(file_path, file_type, logger):
    """載入檔案並進行去重處理"""
    try:
        logger.info(f"正在載入 {file_type} 檔案: {file_path}")
        
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"原始資料筆數: {len(df)}")
        
        # 檢查是否有訂單ID欄位
        if 'order_line_uid' not in df.columns:
            logger.error(f"檔案 {file_path} 缺少order_line_uid欄位")
            return None
        
        # 以訂單ID為key進行去重，保留最後一筆
        df_dedup = df.drop_duplicates(subset=['order_line_uid'], keep='last')
        logger.info(f"去重後資料筆數: {len(df_dedup)}")
        logger.info(f"去重減少筆數: {len(df) - len(df_dedup)}")
        
        return df_dedup
        
    except Exception as e:
        logger.error(f"處理檔案 {file_path} 時發生錯誤: {str(e)}")
        return None

def save_deduplicated_file(df, output_dir, file_type, logger):
    """儲存去重後的檔案"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"etmall_{file_type}_deduplicated_{timestamp}.csv"
    
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"{file_type} 去重檔案已儲存至: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"儲存 {file_type} 檔案時發生錯誤: {str(e)}")
        return None

def analyze_duplicates(df, file_type, logger):
    """分析重複資料"""
    try:
        # 統計重複的訂單ID
        duplicate_counts = df['order_line_uid'].value_counts()
        duplicates = duplicate_counts[duplicate_counts > 1]
        
        if len(duplicates) > 0:
            logger.info(f"{file_type} 重複訂單ID統計:")
            logger.info(f"  重複的訂單ID數量: {len(duplicates)}")
            logger.info(f"  前10個重複最多的訂單ID:")
            for order_id, count in duplicates.head(10).items():
                logger.info(f"    {order_id}: {count} 筆")
        else:
            logger.info(f"{file_type} 沒有重複的訂單ID")
            
    except Exception as e:
        logger.error(f"分析重複資料時發生錯誤: {str(e)}")

def cleanup_temp_files(temp_dir, logger, latest_output_files):
    """清理 temp/etmall 目錄下的所有腳本輸出檔案，只保留最新的去重檔案"""
    try:
        logger.info("開始清理臨時檔案...")
        
        # 要刪除的檔案模式
        patterns_to_delete = [
            "etmall_shipping_orders_merged_*.csv",      # 03 腳本輸出
            "etmall_sales_report_merged_*.csv",         # 04 腳本輸出
            "etmall_shipping_orders_deduplicated_*.csv", # 05 腳本過去輸出
            "etmall_sales_report_deduplicated_*.csv"     # 05 腳本過去輸出
        ]
        
        deleted_files = []
        for pattern in patterns_to_delete:
            file_pattern = os.path.join(temp_dir, pattern)
            files_to_delete = glob.glob(file_pattern)
            
            for file_path in files_to_delete:
                # 跳過最新生成的去重檔案
                file_path_str = str(file_path)
                should_skip = False
                for latest_file in latest_output_files:
                    latest_file_str = str(latest_file)
                    # 使用檔案名進行比較，而不是完整路徑
                    if os.path.basename(latest_file_str) == os.path.basename(file_path_str):
                        logger.info(f"保留最新檔案: {file_path}")
                        should_skip = True
                        break
                
                if should_skip:
                    continue
                    
                try:
                    os.remove(file_path)
                    deleted_files.append(file_path)
                    logger.info(f"已刪除檔案: {file_path}")
                except Exception as e:
                    logger.warning(f"刪除檔案失敗 {file_path}: {str(e)}")
        
        if deleted_files:
            logger.info(f"清理完成，共刪除 {len(deleted_files)} 個檔案")
        else:
            logger.info("沒有找到需要清理的檔案")
            
    except Exception as e:
        logger.error(f"清理檔案時發生錯誤: {str(e)}")

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("開始執行東森購物訂單去重腳本")
    
    # 設定路徑
    temp_dir = "temp/etmall"
    output_dir = "temp/etmall"
    
    # 初始化變數
    shipping_output = None
    sales_output = None
    
    # 檢查目錄是否存在
    if not os.path.exists(temp_dir):
        logger.error(f"目錄不存在: {temp_dir}")
        return
    
    # 找尋最新的檔案
    logger.info("正在找尋最新的檔案...")
    
    # 找尋 03 腳本的最新檔案 (shipping_orders)
    shipping_pattern = os.path.join(temp_dir, "etmall_shipping_orders_merged_*.csv")
    latest_shipping_file = find_latest_file(shipping_pattern, logger)
    
    # 找尋 04 腳本的最新檔案 (sales_report)
    sales_pattern = os.path.join(temp_dir, "etmall_sales_report_merged_*.csv")
    latest_sales_file = find_latest_file(sales_pattern, logger)
    
    if not latest_shipping_file and not latest_sales_file:
        logger.error("未找到任何可處理的檔案")
        return
    
    # 處理 shipping_orders 檔案
    if latest_shipping_file:
        logger.info("=" * 50)
        logger.info("處理訂單出貨報表檔案")
        logger.info("=" * 50)
        
        # 載入並去重
        shipping_df = load_and_deduplicate_file(latest_shipping_file, "shipping_orders", logger)
        
        if shipping_df is not None:
            # 分析重複資料
            analyze_duplicates(shipping_df, "shipping_orders", logger)
            
            # 儲存去重後的檔案
            shipping_output = save_deduplicated_file(shipping_df, output_dir, "shipping_orders", logger)
            
            if shipping_output:
                logger.info(f"訂單出貨報表去重完成，輸出檔案: {shipping_output}")
                logger.info(f"原始筆數: {len(pd.read_csv(latest_shipping_file))}")
                logger.info(f"去重後筆數: {len(shipping_df)}")
    
    # 處理 sales_report 檔案
    if latest_sales_file:
        logger.info("=" * 50)
        logger.info("處理銷售報表檔案")
        logger.info("=" * 50)
        
        # 載入並去重
        sales_df = load_and_deduplicate_file(latest_sales_file, "sales_report", logger)
        
        if sales_df is not None:
            # 分析重複資料
            analyze_duplicates(sales_df, "sales_report", logger)
            
            # 儲存去重後的檔案
            sales_output = save_deduplicated_file(sales_df, output_dir, "sales_report", logger)
            
            if sales_output:
                logger.info(f"銷售報表去重完成，輸出檔案: {sales_output}")
                logger.info(f"原始筆數: {len(pd.read_csv(latest_sales_file))}")
                logger.info(f"去重後筆數: {len(sales_df)}")
    
    # 收集最新生成的去重檔案路徑
    latest_output_files = []
    if shipping_output:
        latest_output_files.append(str(shipping_output))
    if sales_output:
        latest_output_files.append(str(sales_output))
    
    # 清理臨時檔案
    logger.info("=" * 50)
    logger.info("開始清理臨時檔案")
    logger.info("=" * 50)
    cleanup_temp_files(temp_dir, logger, latest_output_files)
    
    logger.info("=" * 50)
    logger.info("腳本執行完成！")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()

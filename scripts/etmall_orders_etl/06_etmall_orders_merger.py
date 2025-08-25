#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單合併腳本
根據訂單ID合併 05 腳本輸出的最新檔案
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
    log_file = log_dir / f"etmall_orders_merger_{timestamp}.log"
    
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

def load_file(file_path, file_type, logger):
    """載入檔案"""
    try:
        logger.info(f"正在載入 {file_type} 檔案: {file_path}")
        
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"{file_type} 資料筆數: {len(df)}")
        
        # 檢查是否有訂單ID欄位
        if '訂單ID' not in df.columns:
            logger.error(f"檔案 {file_path} 缺少訂單ID欄位")
            return None
        
        return df
        
    except Exception as e:
        logger.error(f"載入檔案 {file_path} 時發生錯誤: {str(e)}")
        return None

def merge_dataframes(shipping_df, sales_df, logger):
    """根據訂單ID合併兩個資料框"""
    try:
        logger.info("開始根據訂單ID合併資料...")
        
        # 檢查兩個資料框的訂單ID數量
        shipping_order_ids = set(shipping_df['訂單ID'].unique())
        sales_order_ids = set(sales_df['訂單ID'].unique())
        
        logger.info(f"訂單出貨報表唯一訂單ID數量: {len(shipping_order_ids)}")
        logger.info(f"銷售報表唯一訂單ID數量: {len(sales_order_ids)}")
        
        # 計算交集和差集
        common_order_ids = shipping_order_ids.intersection(sales_order_ids)
        shipping_only = shipping_order_ids - sales_order_ids
        sales_only = sales_order_ids - shipping_order_ids
        
        logger.info(f"共同訂單ID數量: {len(common_order_ids)}")
        logger.info(f"僅在訂單出貨報表中的訂單ID數量: {len(shipping_only)}")
        logger.info(f"僅在銷售報表中的訂單ID數量: {len(sales_only)}")
        
        # 使用左連接合併，以銷售報表為主
        # 先檢查是否有重複的欄位名稱
        sales_columns = set(sales_df.columns)
        shipping_columns = set(shipping_df.columns)
        
        # 找出重複的欄位名稱（除了訂單ID）
        duplicate_columns = sales_columns.intersection(shipping_columns) - {'訂單ID'}
        
        if duplicate_columns:
            logger.info(f"發現重複欄位: {list(duplicate_columns)}")
            # 為訂單出貨報表的重複欄位加上後綴
            shipping_df_renamed = shipping_df.rename(columns={
                col: f"{col}_shipping" for col in duplicate_columns
            })
        else:
            shipping_df_renamed = shipping_df
        
        # 執行合併
        merged_df = pd.merge(
            sales_df, 
            shipping_df_renamed, 
            on='訂單ID', 
            how='left'
        )
        
        logger.info(f"合併後資料筆數: {len(merged_df)}")
        
        # 刪除不需要的欄位
        columns_to_drop = [
            '銷售編號_shipping',
            '商品名稱_shipping', 
            '顏色_shipping',
            '款式_shipping',
            '公司',
            '公司別',
            '訂單號碼',
            '訂單項次',
            '併單序號'
        ]
        
        # 檢查哪些欄位存在於合併後的資料框中
        existing_columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
        
        if existing_columns_to_drop:
            merged_df = merged_df.drop(columns=existing_columns_to_drop)
            logger.info(f"已刪除欄位: {existing_columns_to_drop}")
        else:
            logger.info("沒有找到需要刪除的欄位")
        
        # 檢查合併結果
        merged_order_ids = set(merged_df['訂單ID'].unique())
        logger.info(f"合併後唯一訂單ID數量: {len(merged_order_ids)}")
        
        # 根據訂單日期 > 訂單編號 > 項次 順序排列
        try:
            # 確保日期格式正確
            merged_df['訂單日期'] = pd.to_datetime(merged_df['訂單日期'], errors='coerce')
            
            # 排序：訂單日期 > 訂單編號 > 項次
            merged_df = merged_df.sort_values(
                by=['訂單日期', '訂單編號', '項次'], 
                ascending=[True, True, True]  # 日期升序（舊的在前，新的在後），編號和項次升序
            )
            
            logger.info("資料已按照 訂單日期 > 訂單編號 > 項次 順序排列")
            
        except Exception as e:
            logger.warning(f"排序時發生錯誤，保持原始順序: {str(e)}")
        
        return merged_df
        
    except Exception as e:
        logger.error(f"合併資料時發生錯誤: {str(e)}")
        return None

def save_merged_file(df, output_dir, logger):
    """儲存合併後的檔案"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"etmall_orders_merged_{timestamp}.csv"
    
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"合併檔案已儲存至: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"儲存合併檔案時發生錯誤: {str(e)}")
        return None

def analyze_merge_results(merged_df, logger):
    """分析合併結果"""
    try:
        logger.info("=" * 50)
        logger.info("合併結果分析")
        logger.info("=" * 50)
        
        # 統計欄位數量
        total_columns = len(merged_df.columns)
        
        # 銷售報表欄位（沒有後綴的主要欄位）
        sales_columns = [
            '商品名稱', '顏色', '款式', '售價', '成本', '數量', '銷售編號',
            '子商品銷售編號', '子商品商品編號'
        ]
        sales_column_count = len([col for col in merged_df.columns if col in sales_columns])
        
        # 訂單出貨報表欄位（有_shipping後綴）
        shipping_columns = len([col for col in merged_df.columns if col.endswith('_shipping')])
        
        # 其他欄位（包括訂單ID等共同欄位）
        other_columns = total_columns - shipping_columns - sales_column_count
        
        logger.info(f"總欄位數量: {total_columns}")
        logger.info(f"銷售報表主要欄位數量: {sales_column_count}")
        logger.info(f"訂單出貨報表欄位數量: {shipping_columns}")
        logger.info(f"其他欄位數量: {other_columns}")
        
        # 顯示欄位清單
        logger.info("欄位清單:")
        for i, col in enumerate(merged_df.columns, 1):
            logger.info(f"  {i:2d}. {col}")
        
        # 檢查空值
        null_counts = merged_df.isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0]
        
        if len(columns_with_nulls) > 0:
            logger.info("包含空值的欄位:")
            for col, count in columns_with_nulls.items():
                logger.info(f"  {col}: {count} 筆空值")
        else:
            logger.info("所有欄位都沒有空值")
            
    except Exception as e:
        logger.error(f"分析合併結果時發生錯誤: {str(e)}")

def cleanup_temp_files(temp_dir, logger, latest_output_files):
    """清理 temp/etmall 目錄下的所有腳本輸出檔案，只保留最新的合併檔案"""
    try:
        logger.info("開始清理臨時檔案...")
        
        # 要刪除的檔案模式
        patterns_to_delete = [
            "etmall_shipping_orders_deduplicated_*.csv", # 05 腳本輸出
            "etmall_sales_report_deduplicated_*.csv",    # 05 腳本輸出
            "etmall_orders_merged_*.csv"                 # 06 腳本過去輸出
        ]
        
        deleted_files = []
        for pattern in patterns_to_delete:
            file_pattern = os.path.join(temp_dir, pattern)
            files_to_delete = glob.glob(file_pattern)
            
            for file_path in files_to_delete:
                # 跳過最新生成的合併檔案
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
    logger.info("開始執行東森購物訂單合併腳本")
    
    # 設定路徑
    temp_dir = "temp/etmall"
    output_dir = "temp/etmall"
    
    # 檢查目錄是否存在
    if not os.path.exists(temp_dir):
        logger.error(f"目錄不存在: {temp_dir}")
        return
    
    # 找尋最新的檔案
    logger.info("正在找尋最新的檔案...")
    
    # 找尋 05 腳本的最新檔案 (sales_report) - 主檔案
    sales_pattern = os.path.join(temp_dir, "etmall_sales_report_deduplicated_*.csv")
    latest_sales_file = find_latest_file(sales_pattern, logger)
    
    # 找尋 05 腳本的最新檔案 (shipping_orders) - 附加檔案
    shipping_pattern = os.path.join(temp_dir, "etmall_shipping_orders_deduplicated_*.csv")
    latest_shipping_file = find_latest_file(shipping_pattern, logger)
    
    if not latest_shipping_file and not latest_sales_file:
        logger.error("未找到任何可處理的檔案")
        return
    
    if not latest_sales_file:
        logger.error("未找到銷售報表檔案")
        return
    
    if not latest_shipping_file:
        logger.error("未找到訂單出貨報表檔案")
        return
    
    # 載入檔案
    logger.info("=" * 50)
    logger.info("載入檔案")
    logger.info("=" * 50)
    
    sales_df = load_file(latest_sales_file, "銷售報表", logger)
    shipping_df = load_file(latest_shipping_file, "訂單出貨報表", logger)
    
    if shipping_df is None or sales_df is None:
        logger.error("檔案載入失敗")
        return
    
    # 合併資料
    logger.info("=" * 50)
    logger.info("合併資料")
    logger.info("=" * 50)
    
    merged_df = merge_dataframes(shipping_df, sales_df, logger)
    
    if merged_df is None:
        logger.error("資料合併失敗")
        return
    
    # 分析合併結果
    analyze_merge_results(merged_df, logger)
    
    # 儲存合併後的檔案
    logger.info("=" * 50)
    logger.info("儲存合併檔案")
    logger.info("=" * 50)
    
    merged_output = save_merged_file(merged_df, output_dir, logger)
    
    if merged_output is None:
        logger.error("儲存合併檔案失敗")
        return
    
    # 收集最新生成的合併檔案路徑
    latest_output_files = [str(merged_output)]
    
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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單報表合併腳本
合併 temp/etmall/Order_Report 目錄下的所有 CSV 檔案
輸出到 temp/etmall 目錄
合併完成後移除 temp/etmall/Order_Report 目錄及其內容
"""

import os
import pandas as pd
import glob
from pathlib import Path
import logging
from datetime import datetime
import shutil
import time

# 設定日誌
def setup_logging():
    """設定日誌配置"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"etmall_order_report_merger_{timestamp}.log"
    
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
        
        # 讀取 CSV 檔案，強制所有欄位為字串類型
        df = pd.read_csv(file_path, encoding='utf-8-sig', dtype=str)
        
        # 將中文欄位轉換為英文欄位
        df = convert_columns_to_english(df, logger)
        
        logger.info(f"檔案 {file_path} 處理完成，共 {len(df)} 筆資料")
        logger.info(f"欄位: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"處理檔案 {file_path} 時發生錯誤: {str(e)}")
        return None

def convert_columns_to_english(df, logger):
    """將中文欄位轉換為英文欄位"""
    try:
        logger.info("正在將中文欄位轉換為英文欄位...")
        
        # 定義中英文欄位對應
        column_mapping = {
            '平台': 'platform',
            '訂單日期': 'order_date',
            '訂單編號': 'order_sn',
            '項次': 'item_no',
            '訂單ID': 'order_line_uid',
            '賣家商品編號': 'seller_product_sn',
            '商品名稱': 'product_name_platform',
            '數量': 'quantity',
            '單價': 'unit_price',
            '客戶姓名': 'customer_name',
            '客戶電話': 'customer_phone',
            '配送地址': 'shipping_address',
            '備註': 'note',
            '平台成本': 'cost_to_platform',
            '配送公司': 'delivery_company'
        }
        
        # 重新命名欄位
        df_renamed = df.rename(columns=column_mapping)
        
        # 記錄欄位轉換
        converted_columns = []
        for chinese_col, english_col in column_mapping.items():
            if chinese_col in df.columns:
                converted_columns.append(f"{chinese_col} -> {english_col}")
        
        if converted_columns:
            logger.info("欄位轉換完成:")
            for conversion in converted_columns:
                logger.info(f"  {conversion}")
        else:
            logger.info("沒有需要轉換的中文欄位")
        
        return df_renamed
        
    except Exception as e:
        logger.error(f"欄位轉換時發生錯誤: {str(e)}")
        return df

def merge_csv_files(csv_files, logger):
    """合併所有 CSV 檔案"""
    all_dataframes = []
    total_rows = 0
    processed_files = 0
    
    for file_path in csv_files:
        df = process_csv_file(file_path, logger)
        if df is not None:
            all_dataframes.append(df)
            total_rows += len(df)
            processed_files += 1
    
    if not all_dataframes:
        logger.error("沒有成功處理任何 CSV 檔案")
        return None
    
    # 合併所有資料框
    logger.info("正在合併所有資料框...")
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    # 清理記憶體，確保檔案句柄被釋放
    del all_dataframes
    
    logger.info(f"合併完成！總共處理 {processed_files} 個檔案，合計 {total_rows} 筆資料")
    
    # 新增 item_no 欄位：同一個 order_sn 給予流水號 1, 2, 3, ...
    logger.info("正在新增 item_no 欄位...")
    merged_df['item_no'] = merged_df.groupby('order_sn').cumcount() + 1
    
    # 新增 order_line_uid 欄位：order_sn + "_" + item_no
    logger.info("正在新增 order_line_uid 欄位...")
    merged_df['order_line_uid'] = merged_df['order_sn'].astype(str) + '_' + merged_df['item_no'].astype(str).str.zfill(2)
    
    # 處理 note 欄位：將 "配送前請先電聯，謝謝！" 設為空值
    logger.info("正在處理 note 欄位...")
    if 'note' in merged_df.columns:
        # 統計處理前的數量
        before_count = len(merged_df[merged_df['note'] == '配送前請先電聯，謝謝！'])
        logger.info(f"找到 {before_count} 筆 note 內容為 '配送前請先電聯，謝謝！' 的記錄")
        
        # 將指定內容設為空值
        merged_df.loc[merged_df['note'] == '配送前請先電聯，謝謝！', 'note'] = ''
        
        # 統計處理後的數量
        after_count = len(merged_df[merged_df['note'] == '配送前請先電聯，謝謝！'])
        logger.info(f"處理完成，剩餘 {after_count} 筆該內容的記錄")
        logger.info(f"已將 {before_count - after_count} 筆記錄的 note 設為空值")
    else:
        logger.warning("未找到 note 欄位，跳過 note 處理")
    
    # 處理 platform 欄位：將 "東森購物" 轉換為 "etmall"
    logger.info("正在處理 platform 欄位...")
    if 'platform' in merged_df.columns:
        # 統計處理前的數量
        before_count = len(merged_df[merged_df['platform'] == '東森購物'])
        logger.info(f"找到 {before_count} 筆 platform 內容為 '東森購物' 的記錄")
        
        # 將 "東森購物" 轉換為 "etmall"
        merged_df.loc[merged_df['platform'] == '東森購物', 'platform'] = 'etmall'
        
        # 統計處理後的數量
        after_count = len(merged_df[merged_df['platform'] == '東森購物'])
        logger.info(f"處理完成，剩餘 {after_count} 筆 '東森購物' 記錄")
        logger.info(f"已將 {before_count - after_count} 筆記錄的 platform 轉換為 'etmall'")
        
        # 統計轉換後的 etmall 數量
        etmall_count = len(merged_df[merged_df['platform'] == 'etmall'])
        logger.info(f"轉換後共有 {etmall_count} 筆 'etmall' 記錄")
    else:
        logger.warning("未找到 platform 欄位，跳過 platform 處理")
    
    # 重新排列欄位順序
    logger.info("正在重新排列欄位順序...")
    
    # 定義目標欄位順序（按照之前指定的長串順序，但只保留指定的欄位）
    target_columns = [
        'platform', 'order_date', 'order_sn', 'item_no', 'order_line_uid',
        'seller_product_sn', 'product_name_platform', 'quantity', 'unit_price',
        'customer_name', 'customer_phone', 'shipping_address', 'note',
        'cost_to_platform', 'delivery_company'
    ]
    
    # 檢查現有欄位
    existing_columns = list(merged_df.columns)
    logger.info(f"現有欄位數量：{len(existing_columns)}")
    logger.info(f"現有欄位：{existing_columns}")
    
    # 只保留指定的欄位，移除其他欄位
    merged_df = merged_df[target_columns]
    
    logger.info(f"重新排列後欄位數量：{len(merged_df.columns)}")
    logger.info(f"重新排列後欄位：{list(merged_df.columns)}")
    
    return merged_df

def save_merged_file(merged_df, output_dir, logger):
    """儲存合併後的檔案"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"etmall_order_report_merged_{timestamp}.csv"
    
    try:
        # 確保所有資料都是字串格式
        for col in merged_df.columns:
            merged_df[col] = merged_df[col].astype(str)
        
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig', na_rep='')
        logger.info(f"合併檔案已儲存至: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"儲存檔案時發生錯誤: {str(e)}")
        return None

def remove_order_report_directory(order_report_dir, logger):
    """移除 Order_Report 目錄及其內容"""
    try:
        order_report_path = Path(order_report_dir)
        if order_report_path.exists():
            # 計算目錄大小
            total_size = 0
            file_count = 0
            for root, dirs, files in os.walk(order_report_path):
                for file in files:
                    file_path = Path(root) / file
                    total_size += file_path.stat().st_size
                    file_count += 1
            
            # 等待一下確保檔案句柄被釋放
            logger.info("等待檔案句柄釋放...")
            time.sleep(2)
            
            # 移除目錄及其內容
            shutil.rmtree(order_report_path)
            logger.info(f"已移除 Order_Report 目錄: {order_report_dir}")
            logger.info(f"移除檔案數量: {file_count} 個")
            logger.info(f"移除總大小: {total_size / 1024 / 1024:.2f} MB")
            return True
        else:
            logger.warning(f"Order_Report 目錄不存在: {order_report_dir}")
            return False
    except Exception as e:
        logger.error(f"移除 Order_Report 目錄時發生錯誤: {str(e)}")
        return False

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("開始執行東森購物訂單報表合併腳本")
    
    # 設定路徑
    base_dir = "temp/etmall/Order_Report"
    output_dir = "temp/etmall"
    
    # 檢查輸入目錄是否存在
    if not os.path.exists(base_dir):
        logger.error(f"輸入目錄不存在: {base_dir}")
        logger.info("請先執行 02_01_etmall_order_report_cleaner.py 腳本")
        return
    
    # 搜尋所有 CSV 檔案
    logger.info(f"正在搜尋目錄: {base_dir}")
    csv_files = find_csv_files(base_dir)
    
    if not csv_files:
        logger.warning(f"在目錄 {base_dir} 中未找到任何 CSV 檔案")
        return
    
    logger.info(f"找到 {len(csv_files)} 個 CSV 檔案")
    
    # 顯示找到的檔案列表
    logger.info("找到的檔案列表:")
    for i, file_path in enumerate(csv_files, 1):
        file_size = Path(file_path).stat().st_size / 1024  # KB
        logger.info(f"  {i:2d}. {file_path} ({file_size:.2f} KB)")
    
    # 處理並合併所有 CSV 檔案
    merged_df = merge_csv_files(csv_files, logger)
    
    if merged_df is None:
        logger.error("合併失敗")
        return
    
    # 顯示合併後的欄位資訊
    logger.info("合併後的欄位順序:")
    for i, col in enumerate(merged_df.columns):
        logger.info(f"  {i+1:2d}. {col}")
    
    # 顯示資料統計
    logger.info("資料統計:")
    logger.info(f"  總資料筆數: {len(merged_df)}")
    logger.info(f"  總欄位數: {len(merged_df.columns)}")
    
    # 檢查是否有重複的 order_sn
    if 'order_sn' in merged_df.columns:
        unique_order_sns = merged_df['order_sn'].nunique()
        total_order_sns = len(merged_df)
        logger.info(f"  唯一 order_sn 數量: {unique_order_sns}")
        logger.info(f"  重複 order_sn 數量: {total_order_sns - unique_order_sns}")
    
    # 儲存合併後的檔案
    output_file = save_merged_file(merged_df, output_dir, logger)
    
    if output_file:
        logger.info("合併檔案儲存成功！")
        logger.info(f"輸出檔案: {output_file}")
        logger.info(f"檔案大小: {Path(output_file).stat().st_size / 1024 / 1024:.2f} MB")
        
        # 移除 Order_Report 目錄及其內容
        logger.info("開始移除 Order_Report 目錄...")
        if remove_order_report_directory(base_dir, logger):
            logger.info("✅ 腳本執行完成！")
            logger.info(f"📁 合併檔案: {output_file}")
            logger.info(f"📊 總資料筆數: {len(merged_df)}")
            logger.info(f"🗑️ 已移除原始 Order_Report 目錄")
        else:
            logger.error("❌ 移除 Order_Report 目錄失敗")
    else:
        logger.error("❌ 腳本執行失敗")

if __name__ == "__main__":
    main()

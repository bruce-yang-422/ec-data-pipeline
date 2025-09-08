#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單日期時間處理腳本
處理日期時間欄位的格式轉換
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
    log_file = log_dir / f"etmall_orders_datetime_processor_{timestamp}.log"
    
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

def load_file(file_path, logger):
    """載入檔案"""
    try:
        logger.info(f"正在載入檔案: {file_path}")
        
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"資料筆數: {len(df)}")
        logger.info(f"欄位數量: {len(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"載入檔案 {file_path} 時發生錯誤: {str(e)}")
        return None

def process_datetime_fields(df, logger):
    """處理日期時間欄位"""
    try:
        logger.info("開始處理日期時間欄位...")
        
        # 複製資料框以避免修改原始資料
        processed_df = df.copy()
        
        # 1. 處理 "訂單日期" 欄位：分割為日期和時間
        if '訂單日期' in processed_df.columns:
            logger.info("處理 '訂單日期' 欄位...")
            
            # 先轉換為 datetime 格式
            temp_datetime = pd.to_datetime(processed_df['訂單日期'], errors='coerce')
            
            # 分割為日期和時間
            processed_df['訂單日期'] = temp_datetime.dt.date
            processed_df['訂單時間'] = temp_datetime.dt.time
            
            # 重新排列欄位順序，將日期和時間放在最前面
            cols = processed_df.columns.tolist()
            # 將日期和時間放在最前面
            cols = ['訂單日期', '訂單時間'] + [col for col in cols if col not in ['訂單日期', '訂單時間']]
            processed_df = processed_df[cols]
            
            logger.info("'訂單日期' 已分割為 '訂單日期' 和 '訂單時間'，並放在最前面")
        else:
            logger.warning("未找到 '訂單日期' 欄位")
        
        # 2. 處理 "配送確認日" 欄位：轉換為標準 datetime
        if '配送確認日' in processed_df.columns:
            logger.info("處理 '配送確認日' 欄位...")
            processed_df['配送確認日'] = pd.to_datetime(processed_df['配送確認日'], errors='coerce')
            logger.info("'配送確認日' 已轉換為標準 datetime 格式")
        else:
            logger.warning("未找到 '配送確認日' 欄位")
        
        # 3. 處理 "出貨指示日" 欄位：轉換為標準 datetime
        if '出貨指示日' in processed_df.columns:
            logger.info("處理 '出貨指示日' 欄位...")
            processed_df['出貨指示日'] = pd.to_datetime(processed_df['出貨指示日'], errors='coerce')
            logger.info("'出貨指示日' 已轉換為標準 datetime 格式")
        else:
            logger.warning("未找到 '出貨指示日' 欄位")
        
        # 4. 處理 "預計入庫日" 欄位：轉換為純 DATE 格式
        if '預計入庫日' in processed_df.columns:
            logger.info("處理 '預計入庫日' 欄位...")
            
            # 使用更靈活的日期解析方式
            def parse_expected_inbound_date(date_str):
                try:
                    if pd.isna(date_str) or date_str == '':
                        return None
                    
                    # 處理 "2025/7/6 上午 12:00:00" 格式
                    if isinstance(date_str, str) and '上午' in date_str:
                        # 提取日期部分，忽略時間
                        date_part = date_str.split('上午')[0].strip()
                        # 解析日期
                        parsed_date = pd.to_datetime(date_part, format='%Y/%m/%d', errors='coerce')
                        if pd.notna(parsed_date):
                            return parsed_date.strftime('%Y-%m-%d')
                    
                    # 嘗試標準解析
                    parsed_date = pd.to_datetime(date_str, errors='coerce')
                    if pd.notna(parsed_date):
                        return parsed_date.strftime('%Y-%m-%d')
                    
                    return None
                except:
                    return None
            
            # 應用自定義解析函數
            processed_df['預計入庫日'] = processed_df['預計入庫日'].apply(parse_expected_inbound_date)
            
            # 記錄處理結果
            non_null_count = processed_df['預計入庫日'].notna().sum()
            total_count = len(processed_df)
            logger.info(f"'預計入庫日' 已轉換為純 DATE 格式 (YYYY-MM-DD)")
            logger.info(f"成功轉換: {non_null_count}/{total_count} 筆資料")
        else:
            logger.warning("未找到 '預計入庫日' 欄位")
        
        # 5. 處理資料型態轉換
        logger.info("開始處理資料型態轉換...")
        
        # 字串型態欄位
        string_columns = [
            '訂單編號', '項次', '訂單ID', '配送狀態', '訂單狀態', '商品屬性', '銷售編號', 
            '子商品銷售編號', '子商品商品編號', '配送方式', '商品名稱', '顏色', '款式', 
            '訂單類別', '配送地址', '貨運公司', '配送單號', '備註', '贈品', '廠商配送訊息', '通路別'
        ]
        
        for col in string_columns:
            if col in processed_df.columns:
                # 將 NaN 轉換為空字串，然後轉換為字串型態
                processed_df[col] = processed_df[col].fillna('').astype(str)
                # 將 'nan' 字串轉換為空字串
                processed_df[col] = processed_df[col].replace('nan', '')
                logger.info(f"欄位 '{col}' 已轉換為字串型態（空值為空白）")
        
        # 特殊處理：純數字字串欄位 - 確保為字串且無小數點
        numeric_string_columns = [
            '訂單類別代號', '送貨單號', '商品編號', '廠商商品號碼', '客戶電話', '室內電話'
        ]
        
        for col in numeric_string_columns:
            if col in processed_df.columns:
                logger.info(f"特殊處理 '{col}' 欄位...")
                # 先轉換為字串，然後移除小數點
                processed_df[col] = processed_df[col].fillna('').astype(str)
                # 將 'nan' 字串轉換為空字串
                processed_df[col] = processed_df[col].replace('nan', '')
                # 移除小數點，只保留整數部分
                processed_df[col] = processed_df[col].apply(
                    lambda x: x.split('.')[0] if x and '.' in x else x
                )
                logger.info(f"'{col}' 已轉換為字串型態（無小數點，空值為空白）")
        
        # 數字型態欄位（價格，小數點後兩位）
        price_columns = ['售價', '成本', '售價_shipping', '成本_shipping']
        for col in price_columns:
            if col in processed_df.columns:
                # 先轉換為數值，然後格式化為小數點後兩位
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
                processed_df[col] = processed_df[col].round(2)
                # 將 NaN 轉換為空字串
                processed_df[col] = processed_df[col].fillna('')
                logger.info(f"欄位 '{col}' 已轉換為數值型態（小數點後兩位，空值為空白）")
        
        # 數量型態欄位
        quantity_columns = ['數量', '數量_shipping']
        for col in quantity_columns:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
                # 將 NaN 轉換為空字串
                processed_df[col] = processed_df[col].fillna('')
                logger.info(f"欄位 '{col}' 已轉換為數值型態（空值為空白）")
        
        # 檢查處理結果
        logger.info("日期時間欄位和資料型態處理完成")
        logger.info(f"處理後欄位數量: {len(processed_df.columns)}")
        
        return processed_df
        
    except Exception as e:
        logger.error(f"處理日期時間欄位時發生錯誤: {str(e)}")
        return None

def save_processed_file(df, output_dir, logger):
    """儲存處理後的檔案"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"etmall_orders_datetime_processed_{timestamp}.csv"
    
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"處理後的檔案已儲存至: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"儲存處理後的檔案時發生錯誤: {str(e)}")
        return None

def analyze_datetime_fields(df, logger):
    """分析日期時間欄位的處理結果"""
    try:
        logger.info("=" * 50)
        logger.info("日期時間欄位處理結果分析")
        logger.info("=" * 50)
        
        # 統計欄位數量
        total_columns = len(df.columns)
        logger.info(f"總欄位數量: {total_columns}")
        
        # 顯示所有欄位清單
        logger.info("欄位清單:")
        for i, col in enumerate(df.columns, 1):
            col_type = str(df[col].dtype)
            logger.info(f"  {i:2d}. {col} ({col_type})")
        
        # 檢查日期時間相關欄位的空值
        datetime_columns = [col for col in df.columns if any(keyword in col.lower() for keyword in ['date', 'time', '日', '時間'])]
        
        if datetime_columns:
            logger.info("日期時間相關欄位空值統計:")
            for col in datetime_columns:
                null_count = df[col].isnull().sum()
                total_count = len(df)
                logger.info(f"  {col}: {null_count}/{total_count} 筆空值 ({null_count/total_count*100:.1f}%)")
        else:
            logger.info("未找到日期時間相關欄位")
        
        # 檢查其他欄位的空值
        other_columns = [col for col in df.columns if col not in datetime_columns]
        if other_columns:
            null_counts = df[other_columns].isnull().sum()
            columns_with_nulls = null_counts[null_counts > 0]
            
            if len(columns_with_nulls) > 0:
                logger.info("其他欄位空值統計:")
                for col, count in columns_with_nulls.items():
                    total_count = len(df)
                    logger.info(f"  {col}: {count}/{total_count} 筆空值 ({count/total_count*100:.1f}%)")
            else:
                logger.info("其他欄位都沒有空值")
                
    except Exception as e:
        logger.error(f"分析日期時間欄位處理結果時發生錯誤: {str(e)}")

def cleanup_temp_files(temp_dir, logger, latest_output_files):
    """清理 temp/etmall 目錄下的所有腳本輸出檔案，只保留最新的處理檔案"""
    try:
        logger.info("開始清理臨時檔案...")
        
        # 要刪除的檔案模式
        patterns_to_delete = [
            "etmall_shipping_orders_deduplicated_*.csv",    # 05 腳本輸出
            "etmall_sales_report_deduplicated_*.csv",       # 05 腳本輸出
            "etmall_orders_datetime_processed_*.csv"        # 07 腳本過去輸出
        ]
        
        deleted_files = []
        for pattern in patterns_to_delete:
            file_pattern = os.path.join(temp_dir, pattern)
            files_to_delete = glob.glob(file_pattern)
            
            for file_path in files_to_delete:
                # 跳過最新生成的處理檔案
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
    logger.info("開始執行東森購物訂單日期時間處理腳本")
    
    # 設定路徑
    temp_dir = "temp/etmall"
    output_dir = "temp/etmall"
    
    # 檢查目錄是否存在
    if not os.path.exists(temp_dir):
        logger.error(f"目錄不存在: {temp_dir}")
        return
    
    # 找尋最新的檔案
    logger.info("正在找尋最新的檔案...")
    
    # 找尋 06 腳本的最新檔案
    merged_pattern = os.path.join(temp_dir, "etmall_orders_merged_*.csv")
    latest_merged_file = find_latest_file(merged_pattern, logger)
    
    if not latest_merged_file:
        logger.error("未找到可處理的合併檔案")
        return
    
    # 載入檔案
    logger.info("=" * 50)
    logger.info("載入檔案")
    logger.info("=" * 50)
    
    df = load_file(latest_merged_file, logger)
    
    if df is None:
        logger.error("檔案載入失敗")
        return
    
    # 處理日期時間欄位
    logger.info("=" * 50)
    logger.info("處理日期時間欄位")
    logger.info("=" * 50)
    
    processed_df = process_datetime_fields(df, logger)
    
    if processed_df is None:
        logger.error("日期時間欄位處理失敗")
        return
    
    # 分析處理結果
    analyze_datetime_fields(processed_df, logger)
    
    # 儲存處理後的檔案
    logger.info("=" * 50)
    logger.info("儲存處理後的檔案")
    logger.info("=" * 50)
    
    processed_output = save_processed_file(processed_df, output_dir, logger)
    
    if processed_output is None:
        logger.error("儲存處理後的檔案失敗")
        return
    
    # 收集最新生成的處理檔案路徑
    latest_output_files = [str(processed_output)]
    
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

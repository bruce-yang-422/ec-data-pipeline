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
import json

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
        
        # 處理日期時間分離
        date_time_columns = ['出貨指示日', '要求配送日']
        for col in date_time_columns:
            if col in df.columns:
                # 檢查是否包含時間資訊（包含空格和冒號）
                has_time = df[col].astype(str).str.contains(r'\s+\d{1,2}:\d{2}', regex=True, na=False)
                if has_time.any():
                    logger.info(f"發現 {col} 欄位包含時間資訊，進行分離處理")
                    
                    # 分離日期和時間
                    date_col = col.replace('日', '日期')
                    time_col = col.replace('日', '時間')
                    df[[date_col, time_col]] = df[col].str.split(' ', expand=True)
                    
                    # 統一日期格式為 YYYY-MM-DD
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
                    
                    # 處理時間格式
                    df[time_col] = df[time_col].fillna('')
                    
                    # 移除原始欄位
                    df = df.drop(columns=[col])
                    
                    logger.info(f"已分離 {col} 為 {date_col} 和 {time_col}")
        
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

def load_field_mapping():
    """載入欄位映射配置"""
    try:
        config_path = Path("config/etmall_fields_mapping.json")
        with open(config_path, 'r', encoding='utf-8') as f:
            field_mapping = json.load(f)
        
        # 建立中英文欄位對應字典
        column_mapping = {}
        for english_field, field_info in field_mapping.items():
            if 'zh_name' in field_info:
                column_mapping[field_info['zh_name']] = english_field
        
        return column_mapping
    except Exception as e:
        print(f"載入欄位映射配置時發生錯誤: {str(e)}")
        return {}

def convert_columns_to_english(df, logger):
    """將中文欄位轉換為英文欄位"""
    try:
        logger.info("正在將中文欄位轉換為英文欄位...")
        
        # 載入欄位映射配置
        column_mapping = load_field_mapping()
        
        if not column_mapping:
            logger.warning("無法載入欄位映射配置，跳過欄位轉換")
            return df
        
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

def cleanup_input_files(csv_files: list, logger):
    """
    清理輸入檔案
    
    Args:
        csv_files: 要刪除的 CSV 檔案路徑列表
        logger: 日誌記錄器
    """
    deleted_count = 0
    for file_path in csv_files:
        try:
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                file_path_obj.unlink()
                logger.info(f"已刪除輸入檔案：{file_path_obj.name}")
                deleted_count += 1
        except Exception as e:
            logger.warning(f"刪除檔案失敗：{file_path} - {str(e)}")
    
    logger.info(f"總共刪除 {deleted_count} 個輸入檔案")

def cleanup_old_merged_files(output_dir: Path, logger, keep_latest: bool = True):
    """
    清理 temp\\etmall 目錄下的舊出貨報表合併檔案，只保留最新的
    
    Args:
        output_dir: temp\\etmall 目錄路徑
        logger: 日誌記錄器
        keep_latest: 是否保留最新的檔案
    """
    if not output_dir.exists():
        logger.warning(f"目錄不存在：{output_dir}")
        return
    
    # 尋找所有出貨報表合併檔案
    merged_files = list(output_dir.glob("etmall_shipping_orders_merged_*.csv"))
    
    if not merged_files:
        logger.info("沒有找到需要清理的出貨報表合併檔案")
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
                logger.info(f"已刪除舊檔案：{file_path.name}")
                deleted_count += 1
        except Exception as e:
            logger.warning(f"刪除檔案失敗：{file_path.name} - {str(e)}")
    
    if files_to_keep:
        logger.info(f"保留最新檔案：{files_to_keep[0].name}")
    
    logger.info(f"總共刪除 {deleted_count} 個舊出貨報表合併檔案")

def cleanup_old_log_files(logs_dir: Path, logger, keep_latest: bool = True):
    """
    清理 logs 目錄下的舊日誌檔案，只保留最新的
    
    Args:
        logs_dir: logs 目錄路徑
        logger: 日誌記錄器
        keep_latest: 是否保留最新的檔案
    """
    if not logs_dir.exists():
        logger.warning(f"日誌目錄不存在：{logs_dir}")
        return
    
    # 尋找所有出貨報表合併腳本的日誌檔案
    log_files = list(logs_dir.glob("etmall_shipping_orders_merger_*.log"))
    
    if not log_files:
        logger.info("沒有找到需要清理的出貨報表合併腳本日誌檔案")
        return
    
    # 按修改時間排序，最新的在最後
    log_files.sort(key=lambda x: x.stat().st_mtime)
    
    if keep_latest:
        # 保留最新的檔案
        files_to_delete = log_files[:-1]  # 除了最後一個（最新的）
        files_to_keep = log_files[-1:]    # 只保留最新的
    else:
        # 刪除所有檔案
        files_to_delete = log_files
        files_to_keep = []
    
    deleted_count = 0
    for file_path in files_to_delete:
        try:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"已刪除舊日誌檔案：{file_path.name}")
                deleted_count += 1
        except Exception as e:
            logger.warning(f"刪除日誌檔案失敗：{file_path.name} - {str(e)}")
    
    if files_to_keep:
        logger.info(f"保留最新日誌檔案：{files_to_keep[0].name}")
    
    logger.info(f"總共刪除 {deleted_count} 個舊日誌檔案")

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
    
    # 將中文欄位轉換為英文欄位
    merged_df = convert_columns_to_english(merged_df, logger)
    
    # 顯示轉換後的欄位資訊
    logger.info("轉換後的欄位順序:")
    for i, col in enumerate(merged_df.columns):
        logger.info(f"  {i+1:2d}. {col}")
    
    # 儲存合併後的檔案
    output_file = save_merged_file(merged_df, output_dir, logger)
    
    if output_file:
        logger.info("腳本執行完成！")
        logger.info(f"輸出檔案: {output_file}")
        logger.info(f"總資料筆數: {len(merged_df)}")
        
        # 注意：data_raw 下的原始檔案不刪除，只清理 temp 目錄下的處理後檔案
        logger.info("注意：data_raw 下的原始檔案已保留，未進行刪除")
        
        # 清理 temp\\etmall 目錄下的舊出貨報表合併檔案，只保留最新的
        logger.info("開始清理 temp\\etmall 目錄下的舊出貨報表合併檔案...")
        cleanup_old_merged_files(Path(output_dir), logger, keep_latest=True)
        
        # 清理 logs 目錄下的舊日誌檔案，只保留最新的
        logger.info("開始清理 logs 目錄下的舊日誌檔案...")
        cleanup_old_log_files(Path("logs"), logger, keep_latest=True)
    else:
        logger.error("腳本執行失敗")

if __name__ == "__main__":
    main()

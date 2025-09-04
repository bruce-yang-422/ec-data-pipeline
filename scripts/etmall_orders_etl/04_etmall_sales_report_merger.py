#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物銷售報表合併腳本
合併 data_raw/etmall/sales_report 目錄下的所有 CSV 檔案
以及 temp/etmall/etmall_order_report_merged_*.csv 檔案
並在 "項次" 和 "配送狀態" 之間新增 "訂單ID" 欄位
訂單ID 格式：訂單編號 + "_" + 項次 (例如：231241430_01)
支援中英文欄位對應
"""

import os
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import glob
import json

# 設定日誌
def setup_logging():
    """設定日誌配置"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"etmall_sales_report_merger_{timestamp}.log"
    
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

def find_order_report_files():
    """搜尋 temp/etmall/etmall_order_report_merged_*.csv 檔案"""
    pattern = "temp/etmall/etmall_order_report_merged_*.csv"
    files = glob.glob(pattern)
    return sorted(files)

def map_columns_to_chinese(df, logger):
    """將英文欄位對應到中文欄位"""
    column_mapping = {
        'platform': '平台',
        'order_date': '訂單日期',
        'order_sn': '訂單編號',
        'item_no': '項次',
        'order_line_uid': '訂單ID',
        'seller_product_sn': '賣家商品編號',
        'product_name_platform': '商品名稱',
        'quantity': '數量',
        'unit_price': '單價',
        'customer_name': '客戶姓名',
        'customer_phone': '客戶電話',
        'shipping_address': '配送地址',
        'note': '備註',
        'cost_to_platform': '平台成本',
        'delivery_company': '配送公司'
    }
    
    # 重新命名欄位
    df_renamed = df.rename(columns=column_mapping)
    
    # 記錄欄位對應
    logger.info("欄位對應完成:")
    for eng, chn in column_mapping.items():
        if eng in df.columns:
            logger.info(f"  {eng} -> {chn}")
    
    return df_renamed

def process_sales_report_file(file_path, logger):
    """處理銷售報表 CSV 檔案（中文欄位）"""
    try:
        logger.info(f"正在處理銷售報表檔案: {file_path}")
        
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # 先將中文欄位轉換為英文欄位
        df = convert_columns_to_english(df, logger)
        
        # 處理訂單日期和時間分離
        if 'order_date' in df.columns:
            # 將訂單日期分離為日期和時間
            df['order_date_original'] = df['order_date']
            
            # 分離日期和時間
            df[['order_date', 'order_time']] = df['order_date'].str.split(' ', expand=True)
            
            # 統一日期格式為 YYYY-MM-DD
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # 處理時間格式
            df['order_time'] = df['order_time'].fillna('')
            
            logger.info(f"已分離訂單日期和時間")
        
        # 處理配送確認日期和時間分離
        if 'shipping_confirm_date' in df.columns:
            # 將配送確認日分離為日期和時間
            df['shipping_confirm_date_original'] = df['shipping_confirm_date']
            
            # 分離日期和時間
            df[['shipping_confirm_date', 'shipping_confirm_time']] = df['shipping_confirm_date'].str.split(' ', expand=True)
            
            # 統一日期格式為 YYYY-MM-DD
            df['shipping_confirm_date'] = pd.to_datetime(df['shipping_confirm_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # 處理時間格式
            df['shipping_confirm_time'] = df['shipping_confirm_time'].fillna('')
            
            logger.info(f"已分離配送確認日期和時間")
        
        # 檢查必要欄位是否存在
        required_columns = ['order_sn', 'item_no', 'shipping_status']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"檔案 {file_path} 缺少必要欄位: {missing_columns}")
            return None
        
        # 新增訂單ID欄位
        # 將項次轉換為兩位數格式 (例如：1 -> 01, 2 -> 02)
        df['order_line_uid'] = df['order_sn'].astype(str) + '_' + df['item_no'].astype(str).str.zfill(2)
        
        # 重新排列欄位順序：在 "item_no" 和 "shipping_status" 之間插入 "order_line_uid"
        columns = list(df.columns)
        order_index = columns.index('item_no')
        
        # 移除訂單ID欄位（如果已存在）
        if 'order_line_uid' in columns:
            columns.remove('order_line_uid')
        
        # 在item_no後面插入order_line_uid
        columns.insert(order_index + 1, 'order_line_uid')
        
        # 重新排列欄位
        df = df[columns]
        
        logger.info(f"銷售報表檔案 {file_path} 處理完成，共 {len(df)} 筆資料")
        return df
        
    except Exception as e:
        logger.error(f"處理銷售報表檔案 {file_path} 時發生錯誤: {str(e)}")
        return None

def process_order_report_file(file_path, logger):
    """處理訂單報表 CSV 檔案（英文欄位）"""
    try:
        logger.info(f"正在處理訂單報表檔案: {file_path}")
        
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # 檢查必要欄位是否存在
        required_columns = ['order_sn', 'item_no', 'order_line_uid']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"檔案 {file_path} 缺少必要欄位: {missing_columns}")
            return None
        
        # 訂單報表已經是英文欄位，不需要轉換
        
        # 處理訂單日期格式統一
        if 'order_date' in df.columns:
            # 統一日期格式為 YYYY-MM-DD
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # 訂單報表沒有時間資訊，設為空
            df['order_time'] = ''
            
            logger.info(f"已統一訂單日期格式")
        
        # 新增配送狀態欄位（預設為空值）
        df['shipping_status'] = ''
        
        # 重新排列欄位順序
        columns = list(df.columns)
        
        # 確保order_line_uid在item_no和shipping_status之間
        if 'item_no' in columns and 'shipping_status' in columns:
            order_index = columns.index('item_no')
            shipping_index = columns.index('shipping_status')
            
            # 移除order_line_uid欄位（如果已存在）
            if 'order_line_uid' in columns:
                columns.remove('order_line_uid')
            
            # 在item_no和shipping_status之間插入order_line_uid
            if shipping_index > order_index:
                columns.insert(order_index + 1, 'order_line_uid')
            else:
                columns.insert(order_index + 1, 'order_line_uid')
        
        # 重新排列欄位
        df = df[columns]
        
        logger.info(f"訂單報表檔案 {file_path} 處理完成，共 {len(df)} 筆資料")
        return df
        
    except Exception as e:
        logger.error(f"處理訂單報表檔案 {file_path} 時發生錯誤: {str(e)}")
        return None

def merge_csv_files(sales_files, order_files, logger):
    """合併所有 CSV 檔案"""
    sales_dataframes = []
    order_dataframes = []
    
    # 處理銷售報表檔案
    for file_path in sales_files:
        df = process_sales_report_file(file_path, logger)
        if df is not None:
            sales_dataframes.append(df)
    
    # 處理訂單報表檔案
    for file_path in order_files:
        df = process_order_report_file(file_path, logger)
        if df is not None:
            order_dataframes.append(df)
    
    if not sales_dataframes and not order_dataframes:
        logger.error("沒有成功處理任何 CSV 檔案")
        return None
    
    # 合併銷售報表
    sales_merged = None
    if sales_dataframes:
        logger.info("正在合併銷售報表...")
        sales_merged = pd.concat(sales_dataframes, ignore_index=True)
        logger.info(f"銷售報表合併完成，共 {len(sales_merged)} 筆資料")
    
    # 合併訂單報表
    order_merged = None
    if order_dataframes:
        logger.info("正在合併訂單報表...")
        order_merged = pd.concat(order_dataframes, ignore_index=True)
        logger.info(f"訂單報表合併完成，共 {len(order_merged)} 筆資料")
    
    # 根據訂單ID進行配對合併
    if sales_merged is not None and order_merged is not None:
        logger.info("正在根據訂單ID進行配對合併...")
        merged_df = merge_by_order_id(sales_merged, order_merged, logger)
    elif sales_merged is not None:
        logger.info("只有銷售報表，直接使用銷售報表")
        merged_df = sales_merged
    elif order_merged is not None:
        logger.info("只有訂單報表，直接使用訂單報表")
        merged_df = order_merged
    else:
        logger.error("沒有可合併的資料")
        return None
    
    total_rows = len(merged_df)
    logger.info(f"合併完成！總共處理 {len(sales_files) + len(order_files)} 個檔案，合計 {total_rows} 筆資料")
    return merged_df

def merge_by_order_id(sales_df, order_df, logger):
    """根據訂單ID進行配對合併"""
    try:
        # 檢查訂單ID欄位
        if 'order_line_uid' not in sales_df.columns:
            logger.error("銷售報表中沒有order_line_uid欄位")
            return sales_df
        
        if 'order_line_uid' not in order_df.columns:
            logger.error("訂單報表中沒有order_line_uid欄位")
            return sales_df
        
        # 獲取訂單ID集合
        sales_order_ids = set(sales_df['order_line_uid'].dropna().unique())
        order_order_ids = set(order_df['order_line_uid'].dropna().unique())
        
        logger.info(f"銷售報表唯一訂單ID數量: {len(sales_order_ids)}")
        logger.info(f"訂單報表唯一訂單ID數量: {len(order_order_ids)}")
        
        # 計算交集和差集
        common_order_ids = sales_order_ids.intersection(order_order_ids)
        sales_only = sales_order_ids - order_order_ids
        order_only = order_order_ids - sales_order_ids
        
        logger.info(f"共同訂單ID數量: {len(common_order_ids)}")
        logger.info(f"僅在銷售報表中的訂單ID數量: {len(sales_only)}")
        logger.info(f"僅在訂單報表中的訂單ID數量: {len(order_only)}")
        
        # 合併共同訂單ID的記錄
        if common_order_ids:
            logger.info("正在合併共同訂單ID的記錄...")
            sales_common = sales_df[sales_df['order_line_uid'].isin(common_order_ids)]
            order_common = order_df[order_df['order_line_uid'].isin(common_order_ids)]
            
            # 使用外連接合併
            merged_common = pd.merge(sales_common, order_common, on='order_line_uid', how='outer', suffixes=('_sales', '_order'))
            
            # 合併重複欄位，按照優先級：sales_report > Order_Report > daily_shipping_orders
            merged_common = merge_duplicate_fields(merged_common, logger)
            
            logger.info(f"共同訂單ID合併完成，共 {len(merged_common)} 筆資料")
        else:
            merged_common = pd.DataFrame()
            logger.warning("沒有共同的訂單ID，無法進行配對合併")
        
        # 合併僅在銷售報表中的記錄
        if sales_only:
            logger.info("正在合併僅在銷售報表中的記錄...")
            sales_only_df = sales_df[sales_df['order_line_uid'].isin(sales_only)]
            logger.info(f"僅在銷售報表中的記錄: {len(sales_only_df)} 筆")
        else:
            sales_only_df = pd.DataFrame()
        
        # 合併僅在訂單報表中的記錄
        if order_only:
            logger.info("正在合併僅在訂單報表中的記錄...")
            order_only_df = order_df[order_df['order_line_uid'].isin(order_only)]
            logger.info(f"僅在訂單報表中的記錄: {len(order_only_df)} 筆")
        else:
            order_only_df = pd.DataFrame()
        
        # 合併所有記錄
        all_dfs = []
        if not merged_common.empty:
            all_dfs.append(merged_common)
        if not sales_only_df.empty:
            all_dfs.append(sales_only_df)
        if not order_only_df.empty:
            all_dfs.append(order_only_df)
        
        if all_dfs:
            final_merged = pd.concat(all_dfs, ignore_index=True)
            logger.info(f"最終合併完成，共 {len(final_merged)} 筆資料")
            return final_merged
        else:
            logger.error("沒有可合併的資料")
            return sales_df
            
    except Exception as e:
        logger.error(f"配對合併時發生錯誤: {str(e)}")
        return sales_df

def save_merged_file(merged_df, output_dir, logger):
    """儲存合併後的檔案"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"etmall_sales_report_merged_{timestamp}.csv"
    
    try:
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"合併檔案已儲存至: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"儲存檔案時發生錯誤: {str(e)}")
        return None

def merge_duplicate_fields(df, logger):
    """合併重複欄位，按照資料來源優先級"""
    try:
        logger.info("正在合併重複欄位...")
        
        # 定義需要合併的欄位映射
        field_mappings = {
            # 訂單相關欄位
            'order_sn': ['order_sn_sales', 'order_sn_order'],
            'order_date': ['order_date_sales', 'order_date_order'],
            'order_time': ['order_time_sales', 'order_time_order'],
            'item_no': ['item_no_sales', 'item_no_order'],
            'shipping_status': ['shipping_status_sales', 'shipping_status_order'],
            
            # 商品相關欄位
            'product_name_platform': ['product_name_platform_sales', 'product_name_platform_order'],
            'quantity': ['quantity_sales', 'quantity_order'],
            'unit_price': ['unit_price_sales', 'unit_price_order'],
            'cost_to_platform': ['cost_to_platform_sales', 'cost_to_platform_order'],
            'seller_product_sn': ['seller_product_sn_sales', 'seller_product_sn_order'],
            
            # 客戶相關欄位
            'customer_name': ['customer_name_sales', 'customer_name_order'],
            'customer_phone': ['customer_phone_sales', 'customer_phone_order'],
            'shipping_address': ['shipping_address_sales', 'shipping_address_order'],
            'note': ['note_sales', 'note_order'],
            
            # 配送相關欄位
            'delivery_company': ['delivery_company_sales', 'delivery_company_order']
        }
        
        # 合併每個欄位
        for target_field, source_fields in field_mappings.items():
            if target_field not in df.columns:
                df[target_field] = ''
            
            # 按照優先級合併：sales > order
            for source_field in source_fields:
                if source_field in df.columns:
                    # 用非空值填補空值
                    mask = (df[target_field].isna() | (df[target_field] == '') | (df[target_field] == 'nan'))
                    df.loc[mask, target_field] = df.loc[mask, source_field]
        
        # 移除重複的來源欄位
        columns_to_drop = []
        for source_fields in field_mappings.values():
            for source_field in source_fields:
                if source_field in df.columns:
                    columns_to_drop.append(source_field)
        
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            logger.info(f"已移除重複欄位: {len(columns_to_drop)} 個")
        
        logger.info("欄位合併完成")
        return df
        
    except Exception as e:
        logger.error(f"合併欄位時發生錯誤: {str(e)}")
        return df

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("開始執行東森購物銷售報表合併腳本")
    
    # 設定路徑
    sales_base_dir = "data_raw/etmall/sales_report"
    output_dir = "temp/etmall"
    
    # 檢查銷售報表輸入目錄是否存在
    if not os.path.exists(sales_base_dir):
        logger.warning(f"銷售報表輸入目錄不存在: {sales_base_dir}")
        sales_files = []
    else:
        # 搜尋銷售報表 CSV 檔案
        logger.info(f"正在搜尋銷售報表目錄: {sales_base_dir}")
        sales_files = find_csv_files(sales_base_dir)
        logger.info(f"找到 {len(sales_files)} 個銷售報表 CSV 檔案")
    
    # 搜尋訂單報表檔案
    logger.info("正在搜尋訂單報表檔案...")
    order_files = find_order_report_files()
    logger.info(f"找到 {len(order_files)} 個訂單報表檔案")
    
    if not sales_files and not order_files:
        logger.warning("未找到任何 CSV 檔案")
        return
    
    # 處理並合併所有 CSV 檔案
    merged_df = merge_csv_files(sales_files, order_files, logger)
    
    if merged_df is None:
        logger.error("合併失敗")
        return
    
    # 顯示合併後的欄位資訊
    logger.info("合併後的欄位順序:")
    for i, col in enumerate(merged_df.columns):
        logger.info(f"  {i+1:2d}. {col}")
    
    # 顯示前幾筆資料的訂單ID範例
    logger.info("訂單ID 範例:")
    if '訂單編號' in merged_df.columns and '項次' in merged_df.columns and '訂單ID' in merged_df.columns:
        sample_data = merged_df[['訂單編號', '項次', '訂單ID']].head(10)
        for _, row in sample_data.iterrows():
            logger.info(f"  訂單編號: {row['訂單編號']}, 項次: {row['項次']}, 訂單ID: {row['訂單ID']}")
    
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

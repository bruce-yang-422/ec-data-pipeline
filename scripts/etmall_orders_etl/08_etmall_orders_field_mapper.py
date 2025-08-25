#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單欄位映射腳本
根據 config/etmall_fields_mapping.json 處理欄位映射和轉換
"""

import os
import pandas as pd
import json
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
    log_file = log_dir / f"etmall_orders_field_mapper_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_field_mapping(config_path, logger):
    """載入欄位映射配置"""
    try:
        logger.info(f"正在載入欄位映射配置: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            field_mapping = json.load(f)
        
        logger.info(f"成功載入欄位映射配置，共 {len(field_mapping)} 個欄位定義")
        return field_mapping
        
    except Exception as e:
        logger.error(f"載入欄位映射配置失敗: {str(e)}")
        return None

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
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        logger.info(f"資料筆數: {len(df)}")
        logger.info(f"欄位數量: {len(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"載入檔案 {file_path} 時發生錯誤: {str(e)}")
        return None

def create_field_mapping_dict(field_mapping, logger):
    """創建欄位映射字典"""
    try:
        logger.info("正在創建欄位映射字典...")
        
        # 創建中文字段名到英文字段名的映射
        zh_to_en = {}
        # 創建英文字段名到順序的映射
        en_to_order = {}
        
        for en_field, config in field_mapping.items():
            zh_name = config.get('zh_name', '')
            order = config.get('order', '999')
            
            if zh_name:
                zh_to_en[zh_name] = en_field
                en_to_order[en_field] = int(order)
        
        logger.info(f"成功創建欄位映射字典，共 {len(zh_to_en)} 個中文字段映射")
        return zh_to_en, en_to_order
        
    except Exception as e:
        logger.error(f"創建欄位映射字典失敗: {str(e)}")
        return None, None

def map_fields(df, zh_to_en, en_to_order, logger):
    """根據配置檔案映射欄位"""
    try:
        logger.info("開始欄位映射...")
        
        # 複製資料框以避免修改原始資料
        mapped_df = df.copy()
        
        # 記錄原始欄位
        original_columns = list(mapped_df.columns)
        logger.info(f"原始欄位數量: {len(original_columns)}")
        
        # 創建新的欄位映射字典
        column_mapping = {}
        unmapped_columns = []
        
        for zh_col in original_columns:
            if zh_col in zh_to_en:
                en_col = zh_to_en[zh_col]
                column_mapping[zh_col] = en_col
                logger.info(f"欄位映射: '{zh_col}' -> '{en_col}'")
            else:
                unmapped_columns.append(zh_col)
                logger.warning(f"未找到映射: '{zh_col}'")
        
        # 重命名欄位
        if column_mapping:
            mapped_df = mapped_df.rename(columns=column_mapping)
            logger.info(f"成功重命名 {len(column_mapping)} 個欄位")
        
        # 記錄未映射的欄位
        if unmapped_columns:
            logger.warning(f"未映射的欄位 ({len(unmapped_columns)} 個): {unmapped_columns}")
        
        # 移除不需要的欄位
        columns_to_remove = ['數量_shipping', '售價_shipping', '成本_shipping']
        for col in columns_to_remove:
            if col in mapped_df.columns:
                mapped_df = mapped_df.drop(columns=[col])
                logger.info(f"已移除欄位: '{col}'")
        
        # 添加缺失的標準欄位（填充空值）
        missing_columns = []
        for en_field, order in en_to_order.items():
            if en_field not in mapped_df.columns:
                missing_columns.append(en_field)
                mapped_df[en_field] = ''
                logger.info(f"添加缺失欄位: '{en_field}' (順序: {order})")
        
        if missing_columns:
            logger.info(f"添加了 {len(missing_columns)} 個缺失的標準欄位")
        
        # 根據順序重新排列欄位
        logger.info("正在重新排列欄位順序...")
        sorted_columns = sorted(en_to_order.items(), key=lambda x: x[1])
        ordered_columns = [col for col, _ in sorted_columns if col in mapped_df.columns]
        
        # 將未在配置中的欄位放在最後
        remaining_columns = [col for col in mapped_df.columns if col not in ordered_columns]
        final_columns = ordered_columns + remaining_columns
        
        mapped_df = mapped_df[final_columns]
        
        logger.info(f"欄位重新排列完成，最終欄位數量: {len(mapped_df.columns)}")
        return mapped_df
        
    except Exception as e:
        logger.error(f"欄位映射失敗: {str(e)}")
        return None

def add_standard_fields(mapped_df, logger):
    """添加標準欄位"""
    try:
        logger.info("正在添加標準欄位...")
        
        # 添加平台欄位
        if 'platform' not in mapped_df.columns:
            mapped_df['platform'] = 'etmall'
            logger.info("已添加 'platform' 欄位，值為 'etmall'")
        else:
            mapped_df['platform'] = 'etmall'
            logger.info("已設定 'platform' 欄位值為 'etmall'")
        
        # 添加商店相關欄位
        if 'shop_id' not in mapped_df.columns:
            mapped_df['shop_id'] = 'ET0001'
            logger.info("已添加 'shop_id' 欄位，值為 'ET0001'")
        else:
            mapped_df['shop_id'] = 'ET0001'
            logger.info("已設定 'shop_id' 欄位值為 'ET0001'")
        
        if 'shop_name' not in mapped_df.columns:
            mapped_df['shop_name'] = '東森購物'
            logger.info("已添加 'shop_name' 欄位，值為 '東森購物'")
        else:
            mapped_df['shop_name'] = '東森購物'
            logger.info("已設定 'shop_name' 欄位值為 '東森購物'")
        
        if 'shop_business_model' not in mapped_df.columns:
            mapped_df['shop_business_model'] = 'B2C'
            logger.info("已添加 'shop_business_model' 欄位，值為 'B2C'")
        
        if 'location' not in mapped_df.columns:
            mapped_df['location'] = '台北'
            logger.info("已添加 'location' 欄位，值為 '台北'")
        
        if 'department' not in mapped_df.columns:
            mapped_df['department'] = '電商部'
            logger.info("已添加 'department' 欄位，值為 '電商部'")
        
        if 'manager' not in mapped_df.columns:
            mapped_df['manager'] = '系統管理員'
            logger.info("已添加 'manager' 欄位，值為 '系統管理員'")
        
        logger.info("標準欄位添加完成")
        return mapped_df
        
    except Exception as e:
        logger.error(f"添加標準欄位失敗: {str(e)}")
        return None

def save_mapped_file(df, output_dir, logger):
    """儲存映射後的檔案"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"etmall_orders_field_mapped_{timestamp}.csv"
    
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"映射後的檔案已儲存至: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"儲存映射後的檔案時發生錯誤: {str(e)}")
        return None

def analyze_mapping_results(df, logger):
    """分析映射結果"""
    try:
        logger.info("=" * 50)
        logger.info("欄位映射結果分析")
        logger.info("=" * 50)
        
        # 統計欄位數量
        total_columns = len(df.columns)
        logger.info(f"總欄位數量: {total_columns}")
        
        # 顯示所有欄位清單
        logger.info("欄位清單:")
        for i, col in enumerate(df.columns, 1):
            logger.info(f"  {i:2d}. {col}")
        
        # 檢查空值統計
        logger.info("空值統計:")
        for col in df.columns:
            null_count = df[col].isnull().sum()
            total_count = len(df)
            if null_count > 0:
                logger.info(f"  {col}: {null_count}/{total_count} 筆空值 ({null_count/total_count*100:.1f}%)")
        
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"分析映射結果時發生錯誤: {str(e)}")

def cleanup_temp_files(temp_dir, logger, latest_output_files):
    """清理 temp/etmall 目錄下的所有腳本輸出檔案，只保留最新的處理檔案"""
    try:
        logger.info("開始清理臨時檔案...")
        
        # 要刪除的檔案模式
        patterns_to_delete = [
            "etmall_shipping_orders_deduplicated_*.csv",    # 05 腳本輸出
            "etmall_sales_report_deduplicated_*.csv",       # 05 腳本輸出
            "etmall_orders_datetime_processed_*.csv",       # 07 腳本輸出
            "etmall_orders_field_mapped_*.csv"              # 08 腳本過去輸出
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
    logger.info("開始執行東森購物訂單欄位映射腳本")
    
    # 設定路徑
    config_path = "config/etmall_fields_mapping.json"
    temp_dir = "temp/etmall"
    output_dir = "temp/etmall"
    
    # 檢查配置檔案是否存在
    if not os.path.exists(config_path):
        logger.error(f"配置檔案不存在: {config_path}")
        return
    
    # 載入欄位映射配置
    logger.info("=" * 50)
    logger.info("載入欄位映射配置")
    logger.info("=" * 50)
    
    field_mapping = load_field_mapping(config_path, logger)
    if field_mapping is None:
        logger.error("欄位映射配置載入失敗")
        return
    
    # 創建欄位映射字典
    zh_to_en, en_to_order = create_field_mapping_dict(field_mapping, logger)
    if zh_to_en is None or en_to_order is None:
        logger.error("欄位映射字典創建失敗")
        return
    
    # 檢查目錄是否存在
    if not os.path.exists(temp_dir):
        logger.error(f"目錄不存在: {temp_dir}")
        return
    
    # 找尋最新的檔案
    logger.info("正在找尋最新的檔案...")
    
    # 找尋 07 腳本的最新檔案
    processed_pattern = os.path.join(temp_dir, "etmall_orders_datetime_processed_*.csv")
    latest_processed_file = find_latest_file(processed_pattern, logger)
    
    if not latest_processed_file:
        logger.error("未找到可處理的檔案")
        return
    
    # 載入檔案
    logger.info("=" * 50)
    logger.info("載入檔案")
    logger.info("=" * 50)
    
    df = load_file(latest_processed_file, logger)
    if df is None:
        logger.error("檔案載入失敗")
        return
    
    # 欄位映射
    logger.info("=" * 50)
    logger.info("欄位映射")
    logger.info("=" * 50)
    
    mapped_df = map_fields(df, zh_to_en, en_to_order, logger)
    if mapped_df is None:
        logger.error("欄位映射失敗")
        return
    
    # 添加標準欄位
    logger.info("=" * 50)
    logger.info("添加標準欄位")
    logger.info("=" * 50)
    
    mapped_df = add_standard_fields(mapped_df, logger)
    if mapped_df is None:
        logger.error("添加標準欄位失敗")
        return
    
    # 分析映射結果
    analyze_mapping_results(mapped_df, logger)
    
    # 儲存映射後的檔案
    logger.info("=" * 50)
    logger.info("儲存映射後的檔案")
    logger.info("=" * 50)
    
    mapped_output = save_mapped_file(mapped_df, output_dir, logger)
    if mapped_output is None:
        logger.error("儲存映射後的檔案失敗")
        return
    
    # 收集最新生成的處理檔案路徑
    latest_output_files = [str(mapped_output)]
    
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

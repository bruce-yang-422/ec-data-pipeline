#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物訂單產品資料豐富腳本
根據 seller_product_sn 從 config/products.yaml 填入相應的產品資料
"""

import os
import pandas as pd
import yaml
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
    log_file = log_dir / f"etmall_orders_product_enricher_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_products_config(config_path, logger):
    """載入產品配置"""
    try:
        logger.info(f"正在載入產品配置: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            products_config = yaml.safe_load(f)
        
        # 創建 seller_product_sn 到產品資料的映射字典
        products_dict = {}
        for product_sn, product_data in products_config.items():
            if product_sn and product_data:
                products_dict[product_sn] = product_data
        
        logger.info(f"成功載入產品配置，共 {len(products_dict)} 個產品定義")
        return products_dict
        
    except Exception as e:
        logger.error(f"載入產品配置失敗: {str(e)}")
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

def enrich_product_data(df, products_dict, logger):
    """根據 seller_product_sn 豐富產品資料"""
    try:
        logger.info("開始豐富產品資料...")
        
        # 複製資料框以避免修改原始資料
        enriched_df = df.copy()
        
        # 檢查是否有 seller_product_sn 欄位
        if 'seller_product_sn' not in enriched_df.columns:
            logger.error("資料中沒有 'seller_product_sn' 欄位")
            return None
        
        # 獲取唯一的 seller_product_sn 值
        unique_product_sns = enriched_df['seller_product_sn'].unique()
        logger.info(f"發現的 seller_product_sn 值數量: {len(unique_product_sns)}")
        
        # 顯示前幾個作為範例
        sample_sns = list(unique_product_sns)[:5]
        logger.info(f"範例 seller_product_sn: {sample_sns}")
        
        # 定義要填入的欄位映射
        field_mapping = {
            'category_level_1': 'category_level_1',
            'category_level_2': 'category_level_2',
            'brand': 'brand',
            'series': 'series',
            'pet_type': 'pet_type',
            'product_name': 'product_name',
            'item_code': 'item_code',
            'sku': 'sku',
            'tags': 'tags',
            'spec': 'spec',
            'unit': 'unit',
            'origin': 'origin',
            'supplier_code': 'supplier_code',
            'supplier': 'supplier'
        }
        
        # 統計匹配和未匹配的數量
        matched_count = 0
        unmatched_count = 0
        
        # 為每個 seller_product_sn 填入相應的產品資料
        for product_sn in unique_product_sns:
            if product_sn in products_dict:
                product_data = products_dict[product_sn]
                matched_count += 1
                logger.info(f"正在處理 seller_product_sn: {product_sn}")
                
                # 填入產品資料
                for target_field, source_field in field_mapping.items():
                    if source_field in product_data:
                        # 使用 loc 來避免 SettingWithCopyWarning
                        mask = enriched_df['seller_product_sn'] == product_sn
                        enriched_df.loc[mask, target_field] = product_data[source_field]
                        logger.info(f"  已填入 {target_field}: {product_data[source_field]}")
                    else:
                        logger.warning(f"  產品資料中沒有 {source_field} 欄位")
            else:
                unmatched_count += 1
                logger.warning(f"未找到 seller_product_sn {product_sn} 的產品資料")
        
        logger.info(f"產品資料豐富完成 - 匹配: {matched_count}, 未匹配: {unmatched_count}")
        return enriched_df
        
    except Exception as e:
        logger.error(f"豐富產品資料失敗: {str(e)}")
        return None

def save_enriched_file(df, output_dir, logger):
    """儲存豐富後的檔案"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"etmall_orders_product_enriched_{timestamp}.csv"
    
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"豐富後的檔案已儲存至: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"儲存豐富後的檔案時發生錯誤: {str(e)}")
        return None

def analyze_enrichment_results(df, logger):
    """分析豐富結果"""
    try:
        logger.info("=" * 50)
        logger.info("產品資料豐富結果分析")
        logger.info("=" * 50)
        
        # 統計欄位數量
        total_columns = len(df.columns)
        logger.info(f"總欄位數量: {total_columns}")
        
        # 顯示所有欄位清單
        logger.info("欄位清單:")
        for i, col in enumerate(df.columns, 1):
            logger.info(f"  {i:2d}. {col}")
        
        # 檢查新增的產品相關欄位
        product_related_fields = [
            'category_level_1', 'category_level_2', 'brand', 'series', 'pet_type',
            'product_name', 'item_code', 'sku', 'tags', 'spec',
            'unit', 'origin', 'supplier_code', 'supplier'
        ]
        
        logger.info("產品相關欄位統計:")
        for field in product_related_fields:
            if field in df.columns:
                non_null_count = df[field].notna().sum()
                total_count = len(df)
                logger.info(f"  {field}: {non_null_count}/{total_count} 筆有值 ({non_null_count/total_count*100:.1f}%)")
            else:
                logger.warning(f"  {field}: 欄位不存在")
        
        # 檢查空值統計
        logger.info("空值統計:")
        for col in df.columns:
            null_count = df[col].isnull().sum()
            total_count = len(df)
            if null_count > 0:
                logger.info(f"  {col}: {null_count}/{total_count} 筆空值 ({null_count/total_count*100:.1f}%)")
        
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"分析豐富結果時發生錯誤: {str(e)}")

def cleanup_temp_files(temp_dir, logger, latest_output_files):
    """清理 temp/etmall 目錄下的所有腳本輸出檔案，只保留最新的處理檔案
    
    注意：腳本 10 的輸出檔案現在保存到 data_processed/merged 目錄
    """
    try:
        logger.info("開始清理臨時檔案...")
        
        # 要刪除的檔案模式
        patterns_to_delete = [
            "etmall_shipping_orders_deduplicated_*.csv",    # 05 腳本輸出
            "etmall_sales_report_deduplicated_*.csv",       # 05 腳本輸出
            "etmall_orders_datetime_processed_*.csv",       # 07 腳本輸出
            "etmall_orders_field_mapped_*.csv",             # 08 腳本輸出
            "etmall_orders_shop_enriched_*.csv",            # 09 腳本輸出
            "etmall_orders_product_enriched_*.csv"          # 10 腳本過去輸出
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
    logger.info("開始執行東森購物訂單產品資料豐富腳本")
    
    # 設定路徑
    products_config_path = "config/products.yaml"
    temp_dir = "temp/etmall"
    output_dir = "data_processed/merged"
    
    # 檢查配置檔案是否存在
    if not os.path.exists(products_config_path):
        logger.error(f"產品配置不存在: {products_config_path}")
        return
    
    # 載入產品配置
    logger.info("=" * 50)
    logger.info("載入產品配置")
    logger.info("=" * 50)
    
    products_dict = load_products_config(products_config_path, logger)
    if products_dict is None:
        logger.error("產品配置載入失敗")
        return
    
    # 檢查目錄是否存在
    if not os.path.exists(temp_dir):
        logger.error(f"目錄不存在: {temp_dir}")
        return
    
    # 找尋最新的檔案
    logger.info("正在找尋最新的檔案...")
    
    # 找尋 09 腳本的最新檔案
    enriched_pattern = os.path.join(temp_dir, "etmall_orders_shop_enriched_*.csv")
    latest_enriched_file = find_latest_file(enriched_pattern, logger)
    
    if not latest_enriched_file:
        logger.error("未找到可處理的檔案")
        return
    
    # 載入檔案
    logger.info("=" * 50)
    logger.info("載入檔案")
    logger.info("=" * 50)
    
    df = load_file(latest_enriched_file, logger)
    if df is None:
        logger.error("檔案載入失敗")
        return
    
    # 豐富產品資料
    logger.info("=" * 50)
    logger.info("豐富產品資料")
    logger.info("=" * 50)
    
    enriched_df = enrich_product_data(df, products_dict, logger)
    if enriched_df is None:
        logger.error("豐富產品資料失敗")
        return
    
    # 分析豐富結果
    analyze_enrichment_results(enriched_df, logger)
    
    # 儲存豐富後的檔案
    logger.info("=" * 50)
    logger.info("儲存豐富後的檔案")
    logger.info("=" * 50)
    
    enriched_output = save_enriched_file(enriched_df, output_dir, logger)
    if enriched_output is None:
        logger.error("儲存豐富後的檔案失敗")
        return
    
    # 收集最新生成的處理檔案路徑
    latest_output_files = [str(enriched_output)]
    
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

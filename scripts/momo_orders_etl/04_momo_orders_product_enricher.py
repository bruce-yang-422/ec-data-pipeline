#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_momo_orders_product_enricher.py

功能：
- 讀取 03_momo_orders_deduplicator.py 輸出的去重檔案
- 以 product_manufacturer_code 為索引比對 config/products.yaml
- 添加商品詳細資訊欄位

使用：python scripts/momo_orders_etl/04_momo_orders_product_enricher.py

輸入：
- temp/momo/momo_accounting_orders_deduplicated.csv
- temp/momo/momo_shipping_orders_deduplicated.csv
- config/products.yaml

輸出：
- temp/momo/momo_accounting_orders_product_enriched.csv
- temp/momo/momo_shipping_orders_product_enriched.csv

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import pandas as pd
import yaml
import logging
import sys
from datetime import datetime
from pathlib import Path

class MomoOrdersProductEnricher:
    def __init__(self):
        # 路徑設定 - 腳本在 scripts/momo_orders_etl/ 目錄下
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parents[1]  # 向上兩層到達專案根目錄
        
        # 檔案路徑
        self.products_yaml_path = self.project_root / "config" / "products.yaml"
        self.input_dir = self.project_root / "temp" / "momo"
        self.output_dir = self.project_root / "temp" / "momo"
        self.logs_dir = self.project_root / "logs"
        
        # 輸入檔案路徑
        self.accounting_file = self.input_dir / "momo_accounting_orders_deduplicated.csv"
        self.shipping_file = self.input_dir / "momo_shipping_orders_deduplicated.csv"
        
        # 輸出檔案路徑
        self.accounting_output_file = self.output_dir / "momo_accounting_orders_product_enriched.csv"
        self.shipping_output_file = self.output_dir / "momo_shipping_orders_product_enriched.csv"
        
        # 確保目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # 設定日誌
        self.setup_logging()
        
        # 商品詳細資訊欄位
        self.product_fields = [
            'category_level_1', 'category_level_2', 'brand', 'series', 'pet_type',
            'product_name', 'item_code', 'sku', 'tags', 'spec', 'unit', 'weight_g',
            'package_size', 'package_type', 'package_qty', 'origin', 'barcode',
            'min_qty', 'price_date', 'msrp', 'price', 'supplier_price', 'list_price',
            'status', 'supplier_code', 'supplier', 'supplier_ref'
        ]
        
    def setup_logging(self):
        """設定日誌系統"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"momo_orders_product_enricher_{timestamp}.log"
        log_path = self.logs_dir / log_filename
        
        # 設定檔案 handler
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # 設定控制台 handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # 設定根 logger
        logging.basicConfig(
            level=logging.INFO,
            handlers=[file_handler, console_handler],
            force=True
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== MOMO 訂單商品詳細資訊豐富化開始 ===")
        
    def load_products_data(self):
        """載入商品資料"""
        self.logger.info("載入商品資料...")
        
        try:
            with open(self.products_yaml_path, 'r', encoding='utf-8') as file:
                products_data = yaml.safe_load(file)
            
            self.logger.info(f"成功載入 {len(products_data)} 個商品資料")
            return products_data
            
        except Exception as e:
            self.logger.error(f"載入商品資料失敗：{e}")
            raise
    
    def check_input_files(self) -> bool:
        """檢查輸入檔案是否存在"""
        self.logger.info("檢查輸入檔案...")
        
        if not self.accounting_file.exists():
            self.logger.error(f"會計訂單檔案不存在: {self.accounting_file}")
            return False
            
        if not self.shipping_file.exists():
            self.logger.error(f"出貨訂單檔案不存在: {self.shipping_file}")
            return False
            
        self.logger.info("✓ 輸入檔案檢查完成")
        return True
    
    def load_order_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """載入訂單資料"""
        self.logger.info("載入訂單資料...")
        
        try:
            # 載入會計訂單（確保特定欄位保持字串格式）
            accounting_df = pd.read_csv(self.accounting_file, encoding='utf-8', dtype=str)
            
            # 欄位重新命名
            field_rename_map = {
                'product_cost': 'platform_product_cost'
            }
            accounting_df = accounting_df.rename(columns=field_rename_map)
            
            # 強制將特定欄位轉換為字串，避免小數點
            string_fields = ['product_manufacturer_code', 'product_sku_main', 'product_barcode', 'product_spec']
            for field in string_fields:
                if field in accounting_df.columns:
                    # 先轉換為整數（如果是數值），再轉換為字串
                    if accounting_df[field].dtype in ['int64', 'float64']:
                        accounting_df[field] = accounting_df[field].astype('Int64').astype(str)
                    else:
                        accounting_df[field] = accounting_df[field].astype(str)
            self.logger.info(f"載入會計訂單: {len(accounting_df)} 筆")
            
            # 載入出貨訂單（確保特定欄位保持字串格式）
            shipping_df = pd.read_csv(self.shipping_file, encoding='utf-8', dtype=str)
            
            # 欄位重新命名
            shipping_df = shipping_df.rename(columns=field_rename_map)
            
            # 強制將特定欄位轉換為字串，避免小數點
            for field in string_fields:
                if field in shipping_df.columns:
                    # 先轉換為整數（如果是數值），再轉換為字串
                    if shipping_df[field].dtype in ['int64', 'float64']:
                        shipping_df[field] = shipping_df[field].astype('Int64').astype(str)
                    else:
                        shipping_df[field] = shipping_df[field].astype(str)
            self.logger.info(f"載入出貨訂單: {len(shipping_df)} 筆")
            
            return accounting_df, shipping_df
            
        except Exception as e:
            self.logger.error(f"載入訂單資料時發生錯誤: {e}")
            raise
    
    def enrich_orders_with_products(self, df: pd.DataFrame, products_data: dict, file_type: str) -> pd.DataFrame:
        """為訂單資料添加商品詳細資訊"""
        self.logger.info(f"開始為 {file_type} 添加商品詳細資訊...")
        
        # 檢查是否有 product_manufacturer_code 欄位
        if 'product_manufacturer_code' not in df.columns:
            self.logger.error(f"{file_type} 缺少 product_manufacturer_code 欄位")
            return df
        
        # 初始化商品詳細資訊欄位
        for field in self.product_fields:
            df[f'product_{field}'] = ''
        
        # 統計匹配情況
        total_records = len(df)
        matched_count = 0
        unmatched_codes = set()
        
        # 遍歷每一筆訂單
        for idx, row in df.iterrows():
            manufacturer_code = str(row['product_manufacturer_code']).strip()
            
            # 清理 manufacturer_code (移除小數點等)
            if manufacturer_code and manufacturer_code != 'nan':
                # 移除 .0 後綴
                if manufacturer_code.endswith('.0'):
                    manufacturer_code = manufacturer_code[:-2]
                
                # 嘗試多種匹配方式
                product_info = self.find_product_info(manufacturer_code, products_data)
                
                if product_info:
                    matched_count += 1
                    
                    # 添加商品詳細資訊
                    for field in self.product_fields:
                        if field in product_info:
                            value = product_info[field]
                            # 處理空值，避免顯示 "nan"
                            if pd.isna(value) or value is None or str(value).strip() == '' or str(value).lower() == 'nan':
                                df.at[idx, f'product_{field}'] = ''
                            else:
                                df.at[idx, f'product_{field}'] = str(value)
                        else:
                            df.at[idx, f'product_{field}'] = ''
                    
                    # 特別處理 cost 欄位，重命名為 product_cost_from_catalog 避免與 platform_product_cost 衝突
                    if 'cost' in product_info:
                        value = product_info['cost']
                        if pd.isna(value) or value is None or str(value).strip() == '' or str(value).lower() == 'nan':
                            df.at[idx, 'product_cost_from_catalog'] = ''
                        else:
                            df.at[idx, 'product_cost_from_catalog'] = str(value)
                    else:
                        df.at[idx, 'product_cost_from_catalog'] = ''
                else:
                    unmatched_codes.add(manufacturer_code)
        
        # 記錄匹配統計
        self.logger.info(f"{file_type} 商品匹配統計:")
        self.logger.info(f"  總筆數: {total_records}")
        self.logger.info(f"  匹配成功: {matched_count} ({matched_count/total_records*100:.1f}%)")
        self.logger.info(f"  未匹配: {total_records - matched_count} ({(total_records - matched_count)/total_records*100:.1f}%)")
        
        if unmatched_codes:
            self.logger.warning(f"未匹配的 product_manufacturer_code 範例: {list(unmatched_codes)[:10]}")
            if len(unmatched_codes) > 10:
                self.logger.warning(f"... 還有 {len(unmatched_codes) - 10} 個未匹配的代碼")
        
        return df
    
    def find_product_info(self, manufacturer_code: str, products_data: dict) -> dict:
        """使用多種匹配方式查找商品資訊"""
        # 1. 直接匹配
        if manufacturer_code in products_data:
            return products_data[manufacturer_code]
        
        # 2. 前面補0匹配（處理前面0消失的情況）
        # 例如: "93766217126" -> "093766217126"
        if len(manufacturer_code) < 15:  # 支援到15位條碼
            padded_code = manufacturer_code.zfill(15)
            if padded_code in products_data:
                self.logger.debug(f"前面補0匹配成功: {manufacturer_code} -> {padded_code}")
                return products_data[padded_code]
        
        # 3. 後面補0匹配（處理後面尾數少一個0的情況）
        # 例如: "9376621712" -> "93766217120"
        if len(manufacturer_code) < 15:
            padded_code = manufacturer_code.ljust(15, '0')
            if padded_code in products_data:
                self.logger.debug(f"後面補0匹配成功: {manufacturer_code} -> {padded_code}")
                return products_data[padded_code]
        
        # 4. 前面補0且後面補0匹配
        if len(manufacturer_code) < 15:
            # 先前面補0到14位，再後面補0到15位
            temp_code = manufacturer_code.zfill(14)
            padded_code = temp_code.ljust(15, '0')
            if padded_code in products_data:
                self.logger.debug(f"前後補0匹配成功: {manufacturer_code} -> {padded_code}")
                return products_data[padded_code]
        
        # 5. 移除前導0後匹配（處理商品資料中可能有前導0的情況）
        stripped_code = manufacturer_code.lstrip('0')
        if stripped_code and stripped_code != manufacturer_code:
            if stripped_code in products_data:
                self.logger.debug(f"移除前導0匹配成功: {manufacturer_code} -> {stripped_code}")
                return products_data[stripped_code]
        
        # 6. 嘗試所有可能的長度組合
        for target_length in range(10, 16):  # 嘗試10-15位長度
            if len(manufacturer_code) < target_length:
                # 前面補0
                padded_code = manufacturer_code.zfill(target_length)
                if padded_code in products_data:
                    self.logger.debug(f"動態長度前面補0匹配成功: {manufacturer_code} -> {padded_code}")
                    return products_data[padded_code]
                
                # 後面補0
                padded_code = manufacturer_code.ljust(target_length, '0')
                if padded_code in products_data:
                    self.logger.debug(f"動態長度後面補0匹配成功: {manufacturer_code} -> {padded_code}")
                    return products_data[padded_code]
        
        # 7. 嘗試部分匹配（如果代碼長度大於等於8位）
        if len(manufacturer_code) >= 8:
            for product_code in products_data.keys():
                # 檢查是否為前綴匹配
                if product_code.startswith(manufacturer_code) or manufacturer_code.startswith(product_code):
                    self.logger.debug(f"部分匹配成功: {manufacturer_code} <-> {product_code}")
                    return products_data[product_code]
        
        return None
    
    def save_enriched_data(self, df: pd.DataFrame, output_file: Path, file_type: str) -> None:
        """儲存豐富化後的資料"""
        self.logger.info(f"儲存 {file_type} 豐富化後的資料...")
        
        try:
            # 在儲存前，將所有NaN值替換為空字串
            df_cleaned = df.copy()
            df_cleaned = df_cleaned.fillna('')
            
            # 處理帳務數字欄位，確保小數點下兩位
            cost_fields = ['product_cost_untaxed', 'platform_product_cost', 'product_original_price']
            for field in cost_fields:
                if field in df_cleaned.columns:
                    # 轉換為數值，保留小數點下兩位
                    df_cleaned[field] = pd.to_numeric(df_cleaned[field], errors='coerce').round(2)
            
            df_cleaned.to_csv(output_file, index=False, encoding='utf-8')
            self.logger.info(f"✓ {file_type} 資料已儲存至: {output_file}")
            
            # 顯示檔案大小
            file_size = output_file.stat().st_size / 1024 / 1024  # MB
            self.logger.info(f"  檔案大小: {file_size:.2f} MB")
            
        except Exception as e:
            self.logger.error(f"儲存 {file_type} 資料時發生錯誤: {e}")
            raise
    
    def generate_summary_report(self, accounting_df: pd.DataFrame, shipping_df: pd.DataFrame,
                              accounting_enriched: pd.DataFrame, shipping_enriched: pd.DataFrame) -> None:
        """生成處理摘要報告"""
        self.logger.info("生成處理摘要報告...")
        
        # 統計各檔案的資料筆數
        accounting_count = len(accounting_df)
        shipping_count = len(shipping_df)
        accounting_enriched_count = len(accounting_enriched)
        shipping_enriched_count = len(shipping_enriched)
        
        # 計算匹配效果
        accounting_matched = len(accounting_enriched[accounting_enriched['product_category_level_1'].str.strip() != ''])
        shipping_matched = len(shipping_enriched[shipping_enriched['product_category_level_1'].str.strip() != ''])
        
        self.logger.info("=" * 60)
        self.logger.info("商品詳細資訊豐富化摘要報告")
        self.logger.info("=" * 60)
        self.logger.info(f"會計訂單檔案:")
        self.logger.info(f"  原始筆數: {accounting_count:,} 筆")
        self.logger.info(f"  豐富化後筆數: {accounting_enriched_count:,} 筆")
        self.logger.info(f"  商品匹配成功: {accounting_matched:,} 筆 ({accounting_matched/accounting_count*100:.1f}%)")
        self.logger.info("")
        self.logger.info(f"出貨訂單檔案:")
        self.logger.info(f"  原始筆數: {shipping_count:,} 筆")
        self.logger.info(f"  豐富化後筆數: {shipping_enriched_count:,} 筆")
        self.logger.info(f"  商品匹配成功: {shipping_matched:,} 筆 ({shipping_matched/shipping_count*100:.1f}%)")
        self.logger.info("")
        self.logger.info(f"總計:")
        self.logger.info(f"  原始總筆數: {accounting_count + shipping_count:,} 筆")
        self.logger.info(f"  豐富化後總筆數: {accounting_enriched_count + shipping_enriched_count:,} 筆")
        self.logger.info(f"  總商品匹配成功: {accounting_matched + shipping_matched:,} 筆")
        self.logger.info("=" * 60)
    
    def run(self) -> None:
        """執行主要的豐富化流程"""
        try:
            self.logger.info("開始執行 MOMO 訂單商品詳細資訊豐富化...")
            
            # 檢查輸入檔案
            if not self.check_input_files():
                return
            
            # 載入商品資料
            products_data = self.load_products_data()
            
            # 載入訂單資料
            accounting_df, shipping_df = self.load_order_data()
            
            # 為會計訂單添加商品詳細資訊
            accounting_enriched = self.enrich_orders_with_products(accounting_df, products_data, "會計訂單")
            
            # 為出貨訂單添加商品詳細資訊
            shipping_enriched = self.enrich_orders_with_products(shipping_df, products_data, "出貨訂單")
            
            # 儲存豐富化後的結果
            self.save_enriched_data(accounting_enriched, self.accounting_output_file, "會計訂單")
            self.save_enriched_data(shipping_enriched, self.shipping_output_file, "出貨訂單")
            
            # 生成摘要報告
            self.generate_summary_report(accounting_df, shipping_df, accounting_enriched, shipping_enriched)
            
            self.logger.info("✓ MOMO 訂單商品詳細資訊豐富化完成！")
            self.logger.info(f"✓ 會計訂單豐富化結果: {self.accounting_output_file}")
            self.logger.info(f"✓ 出貨訂單豐富化結果: {self.shipping_output_file}")
            
        except Exception as e:
            self.logger.error(f"執行過程中發生錯誤: {e}")
            raise

def main():
    """主函數"""
    enricher = MomoOrdersProductEnricher()
    enricher.run()

if __name__ == "__main__":
    main()

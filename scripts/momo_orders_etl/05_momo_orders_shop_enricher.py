#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_momo_orders_shop_enricher.py

功能：
- 讀取 04_momo_orders_product_enricher.py 輸出的豐富化檔案
- 以 platform 為索引比對 config/A02_Shops_Master.json
- 添加商店詳細資訊欄位

使用：python scripts/momo_orders_etl/05_momo_orders_shop_enricher.py

輸入：
- temp/momo/momo_accounting_orders_product_enriched.csv
- temp/momo/momo_shipping_orders_product_enriched.csv
- config/A02_Shops_Master.json

輸出：
- temp/momo/momo_accounting_orders_shop_enriched.csv
- temp/momo/momo_shipping_orders_shop_enriched.csv

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import pandas as pd
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

class MomoOrdersShopEnricher:
    def __init__(self):
        # 路徑設定 - 腳本在 scripts/momo_orders_etl/ 目錄下
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parents[1]  # 向上兩層到達專案根目錄
        
        # 檔案路徑
        self.shops_json_path = self.project_root / "config" / "A02_Shops_Master.json"
        self.input_dir = self.project_root / "temp" / "momo"
        self.output_dir = self.project_root / "temp" / "momo"
        self.logs_dir = self.project_root / "logs"
        
        # 輸入檔案路徑
        self.accounting_file = self.input_dir / "momo_accounting_orders_product_enriched.csv"
        self.shipping_file = self.input_dir / "momo_shipping_orders_product_enriched.csv"
        
        # 輸出檔案路徑
        self.accounting_output_file = self.output_dir / "momo_accounting_orders_shop_enriched.csv"
        self.shipping_output_file = self.output_dir / "momo_shipping_orders_shop_enriched.csv"
        
        # 確保目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # 設定日誌
        self.setup_logging()
        
        # 商店詳細資訊欄位
        self.shop_fields = [
            'shop_id', 'shop_status', 'is_ad_shopee_ads_enabled', 
            'shop_business_model', 'department', 'manager'
        ]
        
    def setup_logging(self):
        """設定日誌系統"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"momo_orders_shop_enricher_{timestamp}.log"
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
        self.logger.info("=== MOMO 訂單商店詳細資訊豐富化開始 ===")
        
    def load_shops_data(self):
        """載入商店資料"""
        self.logger.info("載入商店資料...")
        
        try:
            with open(self.shops_json_path, 'r', encoding='utf-8') as file:
                shops_data = json.load(file)
            
            # 從 JSON 中提取 shops 陣列
            shops_list = shops_data.get('shops', [])
            
            # 建立以 platform 為索引的字典
            shops_dict = {}
            for shop in shops_list:
                platform = shop.get('platform', '').lower()
                if platform:
                    shops_dict[platform] = shop
            
            self.logger.info(f"成功載入 {len(shops_dict)} 個商店資料")
            self.logger.info(f"可用平台: {list(shops_dict.keys())}")
            return shops_dict
            
        except Exception as e:
            self.logger.error(f"載入商店資料失敗：{e}")
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
    
    def enrich_orders_with_shops(self, df: pd.DataFrame, shops_data: dict, file_type: str) -> pd.DataFrame:
        """為訂單資料添加商店詳細資訊"""
        self.logger.info(f"開始為 {file_type} 添加商店詳細資訊...")
        
        # 檢查是否有 platform 欄位
        if 'platform' not in df.columns:
            self.logger.error(f"{file_type} 缺少 platform 欄位")
            return df
        
        # 初始化商店詳細資訊欄位
        for field in self.shop_fields:
            df[f'shop_{field}'] = ''
        
        # 統計匹配情況
        total_records = len(df)
        matched_count = 0
        unmatched_platforms = set()
        
        # 遍歷每一筆訂單
        for idx, row in df.iterrows():
            platform = str(row['platform']).strip().lower()
            
            # 在商店資料中查找
            if platform in shops_data:
                shop_info = shops_data[platform]
                matched_count += 1
                
                # 添加商店詳細資訊
                for field in self.shop_fields:
                    if field in shop_info:
                        df.at[idx, f'shop_{field}'] = str(shop_info[field])
                    else:
                        df.at[idx, f'shop_{field}'] = ''
            else:
                unmatched_platforms.add(platform)
        
        # 記錄匹配統計
        self.logger.info(f"{file_type} 商店匹配統計:")
        self.logger.info(f"  總筆數: {total_records}")
        self.logger.info(f"  匹配成功: {matched_count} ({matched_count/total_records*100:.1f}%)")
        self.logger.info(f"  未匹配: {total_records - matched_count} ({(total_records - matched_count)/total_records*100:.1f}%)")
        
        if unmatched_platforms:
            self.logger.warning(f"未匹配的 platform 範例: {list(unmatched_platforms)}")
        
        return df
    
    def save_enriched_data(self, df: pd.DataFrame, output_file: Path, file_type: str) -> None:
        """儲存豐富化後的資料"""
        self.logger.info(f"儲存 {file_type} 豐富化後的資料...")
        
        try:
            # 處理帳務數字欄位，確保小數點下兩位
            cost_fields = ['product_cost_untaxed', 'platform_product_cost', 'product_original_price', 'product_cost_from_catalog']
            for field in cost_fields:
                if field in df.columns:
                    try:
                        # 轉換為數值，保留小數點下兩位
                        df[field] = pd.to_numeric(df[field], errors='coerce').round(2)
                    except Exception as e:
                        self.logger.warning(f"處理欄位 {field} 時發生錯誤: {e}")
                        # 如果轉換失敗，保持原值
                        continue
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            self.logger.info(f"✓ {file_type} 資料已儲存至: {output_file}")
            
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
        accounting_matched = len(accounting_enriched[accounting_enriched['shop_shop_id'].str.strip() != ''])
        shipping_matched = len(shipping_enriched[shipping_enriched['shop_shop_id'].str.strip() != ''])
        
        self.logger.info("=" * 60)
        self.logger.info("商店詳細資訊豐富化摘要報告")
        self.logger.info("=" * 60)
        self.logger.info(f"會計訂單檔案:")
        self.logger.info(f"  原始筆數: {accounting_count:,} 筆")
        self.logger.info(f"  豐富化後筆數: {accounting_enriched_count:,} 筆")
        self.logger.info(f"  商店匹配成功: {accounting_matched:,} 筆 ({accounting_matched/accounting_count*100:.1f}%)")
        self.logger.info("")
        self.logger.info(f"出貨訂單檔案:")
        self.logger.info(f"  原始筆數: {shipping_count:,} 筆")
        self.logger.info(f"  豐富化後筆數: {shipping_enriched_count:,} 筆")
        self.logger.info(f"  商店匹配成功: {shipping_matched:,} 筆 ({shipping_matched/shipping_count*100:.1f}%)")
        self.logger.info("")
        self.logger.info(f"總計:")
        self.logger.info(f"  原始總筆數: {accounting_count + shipping_count:,} 筆")
        self.logger.info(f"  豐富化後總筆數: {accounting_enriched_count + shipping_enriched_count:,} 筆")
        self.logger.info(f"  總商店匹配成功: {accounting_matched + shipping_matched:,} 筆")
        self.logger.info("=" * 60)
    
    def run(self) -> None:
        """執行主要的豐富化流程"""
        try:
            self.logger.info("開始執行 MOMO 訂單商店詳細資訊豐富化...")
            
            # 檢查輸入檔案
            if not self.check_input_files():
                return
            
            # 載入商店資料
            shops_data = self.load_shops_data()
            
            # 載入訂單資料
            accounting_df, shipping_df = self.load_order_data()
            
            # 為會計訂單添加商店詳細資訊
            accounting_enriched = self.enrich_orders_with_shops(accounting_df, shops_data, "會計訂單")
            
            # 為出貨訂單添加商店詳細資訊
            shipping_enriched = self.enrich_orders_with_shops(shipping_df, shops_data, "出貨訂單")
            
            # 儲存豐富化後的結果
            self.save_enriched_data(accounting_enriched, self.accounting_output_file, "會計訂單")
            self.save_enriched_data(shipping_enriched, self.shipping_output_file, "出貨訂單")
            
            # 生成摘要報告
            self.generate_summary_report(accounting_df, shipping_df, accounting_enriched, shipping_enriched)
            
            self.logger.info("✓ MOMO 訂單商店詳細資訊豐富化完成！")
            self.logger.info(f"✓ 會計訂單豐富化結果: {self.accounting_output_file}")
            self.logger.info(f"✓ 出貨訂單豐富化結果: {self.shipping_output_file}")
            
        except Exception as e:
            self.logger.error(f"執行過程中發生錯誤: {e}")
            raise

def main():
    """主函數"""
    enricher = MomoOrdersShopEnricher()
    enricher.run()

if __name__ == "__main__":
    main()

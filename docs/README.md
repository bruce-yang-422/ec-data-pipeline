# 開發日誌與進度

## 2025-07-04
- excel_password_remover.py 支援自動移除 Shopee Excel 報表密碼，並自動轉換為 CSV，只保留 CSV 檔案。
- 處理過程與結果完整記錄於 log，並自動清理 temp/shopee 及相關 log。
- 修正多帳號密碼對應與批次處理流程。

## 2025-07-03
- 完成 shopee_to_master_cleaner.py 批次合併、唯一鍵、item_seq、sku_key、order_date 自動解析等功能。
- 支援多批次資料合併，唯一鍵為 order_sn 前6碼解析日期 + order_sn + sku_key。
- 新資料自動覆蓋舊資料，確保訂單狀態與明細為最新。
- 欄位順序、英文標頭、商品流水號、店家資訊自動化。
- 支援 shopee 匯出報表自動清洗、合併、去重。
- 初步完成 shopee_to_master_cleaner.py，支援欄位對應、批次讀取、欄位轉換。
- 測試多商品、多訂單合併與 item_seq 產生。

---

# 專案簡介

本專案為多平台電商訂單資料自動清洗、合併、欄位標準化與去重流程，支援 Shopee、Momo、PChome、Yahoo 等平台。

## 主要功能
- 批次讀取 Shopee 匯出訂單 Excel，依欄位對應自動轉換英文標頭與順序
- 自動解析訂單編號前6碼為訂單日期（order_date）
- 產生商品唯一鍵（sku_key），合併時以 order_date+order_sn+sku_key 為唯一鍵
- item_seq 自動依同一訂單多商品流水號
- 新資料自動覆蓋舊資料，確保訂單狀態與明細為最新
- 支援多批次資料合併、去重、欄位自動補齊
- **自動移除 Shopee Excel 報表密碼，並轉存為 CSV，只保留 CSV 檔案**
- 處理過程與結果完整記錄於 log，並自動清理 temp/shopee 及相關 log

## 目錄結構
- scripts/shopee_to_master_cleaner.py：Shopee 訂單自動清洗、合併主腳本
- scripts/excel_password_remover/：自動移除 Excel 密碼並轉存 CSV
- config/shopee_fields_mapping.json：欄位對應設定
- data_processed/merged/shopee_master_orders_cleaned.csv：合併後標準檔

## 使用方式
1. 將 Shopee 匯出 Excel 放入 data_raw/shopee/
2. 執行 `python scripts/excel_password_remover/main.py`，自動移除密碼並轉存為 CSV
3. 執行 `python scripts/shopee_to_master_cleaner.py`，自動合併、清洗、產生主檔
4. 合併結果於 data_processed/merged/shopee_master_orders_cleaned.csv

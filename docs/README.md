# 開發日誌與進度

## 2025-07-10
- 重構 Momo 腳本結構，建立 scripts/momo_orders_etl/ 資料夾。
- 修正三個 Momo 腳本的路徑設定：accounting_cleaner.py、momo_accounting_cleaner.py、momo_batch_processor.py。
- 調整 project_root 路徑從 parent 改為 parents[1]，確保腳本能正確找到專案根目錄。
- 修復腳本無法正確訪問 config/、data_processed/、logs/ 等目錄的問題。
- 更新 momo_batch_processor.py 中的腳本路徑引用，確保批次處理功能正常運作。

## 2025-07-04
- 完成 momo_csv_to_master_cleaner.py 批次處理功能，支援 Momo 平台訂單資料自動清洗與合併。
- 實作多平台統一處理架構，shopee_csv_to_master_cleaner.py 與 momo_csv_to_master_cleaner.py 共用相同處理邏輯。
- 優化資料處理流程，支援批次讀取、欄位對應、自動去重、新資料覆蓋舊資料等完整功能。
- 完善 temp 目錄自動清理機制，確保處理後檔案不殘留。
- 測試多平台資料合併與 item_seq 產生，驗證處理流程穩定性。

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

# 開發日誌（Changelog）

> 本檔案記錄 EC Data Pipeline 專案的主要開發歷程與重要變更。
> 
> 最後更新：2024/7/23

---

## 2024-07-23
- PChome 處理流程重大優化：
  - pchome_cleaner.py、pchome_return_cleaner.py 欄位自動對應邏輯強化，支援括號、全形/半形、特殊符號自動正規化，mapping 能正確對應如「收貨地址(訂單編號)」等欄位。
  - receiver_addr（收貨地址）、receiver_phone（收貨人電話）、receiver_zip（郵遞區號）等欄位自動補齊，並加強自動對應與補空欄機制。
  - 星期幾欄位調整為「星期日=1，星期一=2，...，星期六=7」，與需求一致。
  - 商品名稱自動解析商品ID與選項編號，正則與補零邏輯與 mapping 完全同步。
  - 退貨清洗流程（pchome_return_cleaner.py）與一般訂單清洗流程（pchome_cleaner.py）欄位生成、型態轉換、唯一鍵、補空欄等完全一致，合併分析無落差。
  - pchome_orders_merger.py 合併時同時納入 pchome_*.csv 及 pchome_return_*.csv，欄位順序與格式依 mapping，自動補空欄，唯一鍵去重。
- scripts/pchome_orders_etl/ 新增 README.md，詳細說明三個腳本的功能、步驟、執行方式與建議順序。
- 修正多處 mapping 對不到、欄位全空、欄位自動補空與自動對應問題，提升多平台資料自動化處理穩定性。

## 2025-07-20
- 新增東森（Etmall）自動化批次處理流程：
  - etmall_batch_processor.py：統一執行 xlsx 轉 csv 及資料清洗，支援單步與全流程。
  - etmall_xlsx_to_csv.py：自動將東森 Excel 轉為 CSV。
  - etmall_orders_cleaner.py：自動清洗東森訂單資料，欄位標準化、去重、合併、日誌記錄。
  - etmall_orders_compare_shipping_date.py：比對出貨日期輔助分析。
  - 完整日誌記錄於 logs/，處理結果輸出於 data_processed/merged/。

- 新增 PChome 自動化處理流程：
  - pchome_cleaner.py：自動對應欄位、清洗原始報表，支援智慧欄位比對與批次處理。
  - pchome_orders_merger.py：合併多份清洗後檔案，依唯一鍵去重，產生最終合併檔。
  - pchome_xray.py：資料結構與缺漏檢查工具，支援欄位、型態、缺漏分析與日誌輸出。
  - 處理流程與結果完整記錄於 logs/，合併檔輸出於 data_processed/merged/。

## 2025-07-10
- 重構 Momo 腳本結構，建立 scripts/momo_orders_etl/ 資料夾。
- 修正三個 Momo 腳本的路徑設定：accounting_cleaner.py、momo_accounting_cleaner.py、momo_batch_processor.py。
- 調整 project_root 路徑取得方式，確保腳本能正確找到專案根目錄。
- 修復腳本無法正確訪問 config/、data_processed/、logs/ 等目錄的問題。
- 更新 momo_batch_processor.py 的腳本路徑引用，確保批次處理功能正常運作。

## 2025-07-04
- 完成 momo_csv_to_master_cleaner.py 批次處理功能，支援 Momo 平台訂單資料自動清洗與合併。
- 實作多平台統一處理架構，shopee_csv_to_master_cleaner.py 與 momo_csv_to_master_cleaner.py 共用相同處理邏輯。
- 優化資料處理流程，支援批次讀取、欄位對應、自動去重、新資料覆蓋舊資料等完整功能。
- 完善 temp 目錄自動清理機制，確保處理後檔案不殘留。
- 測試多平台資料合併與 item_seq 產生，驗證處理流程穩定性。

## 2025-07-03
- 完成 shopee_to_master_cleaner.py 批次合併、唯一鍵、item_seq、sku_key、order_date 自動解析等功能。
- 支援多批次資料合併，唯一鍵為 order_sn 前6碼解析日期 + order_sn + sku_key。
- 新資料自動覆蓋舊資料，確保訂單狀態與明細為最新。
- 欄位順序、英文標頭、商品流水號、店家資訊自動化。
- 支援 shopee 匯出報表自動清洗、合併、去重。
- 初步完成 shopee_to_master_cleaner.py，支援欄位對應、批次讀取、欄位轉換。
- 測試多商品、多訂單合併與 item_seq 產生。

## 2025-07-04
- excel_password_remover.py 支援自動移除 Shopee Excel 報表密碼，並自動轉換為 CSV，只保留 CSV 檔案。
- 處理過程與結果完整記錄於 log，並自動清理 temp/shopee 及相關 log。
- 修正多帳號密碼對應與批次處理流程。

## 2025-07-03
- 完成 Shopee 訂單資料轉 master 主表自動轉換腳本。
- 資料夾結構與 README 說明文件上線。

---

> 歷史紀錄可持續追加於本檔案下方。

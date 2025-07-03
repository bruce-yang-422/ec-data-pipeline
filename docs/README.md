# 開發日誌與進度

## 2025-07-03
- 完成 shopee_to_master_cleaner.py 批次合併、唯一鍵、item_seq、sku_key、order_date 自動解析等功能。
- 支援多批次資料合併，唯一鍵為 order_sn 前6碼解析日期 + order_sn + sku_key。
- 新資料自動覆蓋舊資料，確保訂單狀態與明細為最新。
- 欄位順序、英文標頭、商品流水號、店家資訊自動化。
- 支援 shopee 匯出報表自動清洗、合併、去重。
- 初步完成 shopee_to_master_cleaner.py，支援欄位對應、批次讀取、欄位轉換。
- 測試多商品、多訂單合併與 item_seq 產生。

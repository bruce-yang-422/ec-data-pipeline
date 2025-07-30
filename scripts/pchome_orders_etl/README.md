# PChome 訂單資料處理操作說明

本流程適用於自動化清洗、整合 PChome 平台的訂單與退貨資料，產生可直接匯入資料庫的標準化檔案。

---

## 1. 一般訂單清洗：`pchome_cleaner.py`

**功能說明：**
- 處理所有「一般訂單」檔案（第一行為警語「【※此匯出資料...」）。
- 自動欄位對應、欄位清洗、型態轉換、補空欄、唯一鍵生成、重複去除。
- 只處理非退貨資料（同時有 return_apply_date 與 return_approve_date 欄位的檔案會自動跳過）。
- 產出標準化的 `pchome_*.csv` 檔案於 `temp/pchome/`。

**主要步驟：**
1. 讀取原始訂單檔案（支援 csv/xls/xlsx）。
2. 欄位自動對應與清洗（含去除 Excel 匯出異常格式）。
3. 生成明細序號（item_seq）、唯一訂單識別碼（order_id = order_sn + '-' + item_seq）。
4. 從訂單編號自動提取訂單日期，計算星期幾（星期日=1, 星期一=2, ..., 星期六=7）、第幾週。
5. 從商品名稱自動拆出商品ID與選項編號。
6. confirm、is_merge_box 轉布林，receiver_zip 只留前三碼，receiver_addr 自動對應。
7. 數值、日期欄位自動轉型，所有 mapping 欄位補空。
8. 以 order_id 去重。
9. 依訂單日期區間自動命名輸出檔案（如：`pchome_20240101_20240131.csv`）。

**執行方式：**
```bash
python scripts/pchome_orders_etl/pchome_cleaner.py
```
產出：`temp/pchome/pchome_YYYYMMDD_YYYYMMDD.csv` 等多個清洗後檔案

---

## 2. 退貨訂單清洗：`pchome_return_cleaner.py`

**功能說明：**
- 處理所有「退貨訂單」檔案（第一行無警語，且同時有「退貨申請日」與「審核通過日」欄位）。
- 欄位自動對應、清洗、型態轉換、唯一鍵生成、重複去除，邏輯與一般訂單清洗一致。
- 產出標準化的 `pchome_return_*.csv` 檔案於 `temp/pchome/`。

**主要步驟：**
1. 讀取原始退貨檔案（支援 csv）。
2. 欄位自動對應與清洗（同 cleaner）。
3. 生成明細序號、唯一訂單識別碼（order_id = order_sn + '-' + item_seq）。
4. 從訂單編號自動提取訂單日期，計算星期幾（星期日=1, 星期一=2, ..., 星期六=7）、第幾週。
5. 從商品名稱自動拆出商品ID與選項編號。
6. confirm 轉布林，receiver_zip 只留前三碼，receiver_addr 自動對應。
7. 數值、日期欄位自動轉型，所有 mapping 欄位補空。
8. 以 order_id 去重。
9. 依「轉單日期」或「return_apply_date」區間自動命名輸出檔案（如：`pchome_return_20240101-20240131.csv`）。

**執行方式：**
```bash
python scripts/pchome_orders_etl/pchome_return_cleaner.py
```
產出：`temp/pchome/pchome_return_YYYYMMDD-YYYYMMDD.csv` 等多個清洗後檔案

---

## 3. 訂單資料合併：`pchome_orders_merger.py`

**功能說明：**
- 將 `temp/pchome/` 目錄下所有 `pchome_*.csv` 及 `pchome_return_*.csv` 檔案合併成一份總表。
- 欄位順序與格式完全依照 `config/pchome_fields_mapping.json`。
- 以 `platform`、`order_id` 為唯一鍵去重。
- 產出最終可匯入資料庫的 `pchome_orders_merged.csv`。

**主要步驟：**
1. 讀取所有清洗後檔案。
2. 合併所有資料，依唯一鍵去重。
3. 欄位順序依 mapping。
4. 輸出合併檔案至 `data_processed/merged/pchome_orders_merged.csv`。

**執行方式：**
```bash
python scripts/pchome_orders_etl/pchome_orders_merger.py
```
產出：`data_processed/merged/pchome_orders_merged.csv`

---

## 【建議執行順序】

1. **先執行** `pchome_cleaner.py`（一般訂單清洗）
2. **再執行** `pchome_return_cleaner.py`（退貨訂單清洗）
3. **最後執行** `pchome_orders_merger.py`（合併所有清洗後資料）

---

如需自動化批次處理，可將三個腳本依序串接執行。
如有特殊需求（如欄位定義、型態、唯一鍵等），請參考 `config/pchome_fields_mapping.json`。

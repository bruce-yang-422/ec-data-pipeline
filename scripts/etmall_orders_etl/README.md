# 東森購物訂單 ETL 腳本

## 📋 目錄概述

本目錄包含用於處理東森購物訂單資料的完整 ETL（Extract, Transform, Load）腳本套件。這些腳本按照特定的順序執行，將原始訂單資料轉換為標準化、豐富的資料集。

## 🎯 主要功能

- **資料清理**：清理和標準化原始訂單資料
- **資料合併**：整合多個來源的訂單資料
- **去重處理**：識別並移除重複記錄
- **欄位映射**：將中文字段名轉換為英文字段名
- **資料豐富**：添加商店和產品相關資訊
- **自動檔案管理**：智能抓取最新處理檔案
- **完整日誌記錄**：詳細記錄每個處理步驟

## 📁 腳本清單

### 01. 平台訂單清理器
**檔案**：`01_etmall_platform_orders_cleaner.py`
**功能**：清理東森購物平台訂單資料
**輸入**：原始平台訂單檔案
**輸出**：`etmall_platform_orders_cleaned_*.csv`

### 02. 檔案歸檔器
**檔案**：`02_etmall_files_archiver.py`
**功能**：歸檔已處理的原始檔案
**輸入**：已處理的原始檔案
**輸出**：歸檔到 `archive/` 目錄

### 03. 出貨訂單合併器
**檔案**：`03_etmall_shipping_orders_merger.py`
**功能**：合併多個出貨訂單檔案
**輸入**：清理後的出貨訂單檔案
**輸出**：`etmall_shipping_orders_merged_*.csv`

### 04. 銷售報表合併器
**檔案**：`04_etmall_sales_report_merger.py`
**功能**：合併多個銷售報表檔案
**輸入**：清理後的銷售報表檔案
**輸出**：`etmall_sales_report_merged_*.csv`

### 05. 訂單去重器
**檔案**：`05_etmall_orders_deduplicator.py`
**功能**：對訂單資料進行去重處理
**輸入**：合併後的出貨訂單和銷售報表
**輸出**：`etmall_shipping_orders_deduplicated_*.csv`、`etmall_sales_report_deduplicated_*.csv`

### 06. 訂單合併器
**檔案**：`06_etmall_orders_merger.py`
**功能**：將出貨訂單和銷售報表合併
**輸入**：去重後的出貨訂單和銷售報表
**輸出**：`etmall_orders_merged_*.csv`

### 07. 日期時間處理器
**檔案**：`07_etmall_orders_datetime_processor.py`
**功能**：標準化日期時間格式
**輸入**：合併後的訂單資料
**輸出**：`etmall_orders_datetime_processed_*.csv`

### 08. 欄位映射器
**檔案**：`08_etmall_orders_field_mapper.py`
**功能**：根據配置檔案進行欄位映射和轉換
**輸入**：日期時間處理後的訂單資料
**輸出**：`etmall_orders_field_mapped_*.csv`

### 09. 商店資料豐富器
**檔案**：`09_etmall_orders_shop_enricher.py`
**功能**：根據 shop_id 填入商店相關資料
**輸入**：欄位映射後的訂單資料
**輸出**：`etmall_orders_shop_enriched_*.csv`

### 10. 產品資料豐富器
**檔案**：`10_etmall_orders_product_enricher.py`
**功能**：根據 seller_product_sn 填入產品相關資料
**輸入**：商店資料豐富後的訂單資料
**輸出**：`etmall_orders_product_enriched_*.csv`（保存到 `data_processed/merged/` 目錄）
**說明**：此腳本為 ETL 流程的最終輸出，資料將直接傳遞給 BigQuery 上傳器

## 🚀 快速開始

### 前置需求

1. **Python 環境**：Python 3.7+
2. **必要套件**：pandas, pyyaml, pathlib
3. **目錄結構**：確保以下目錄存在
   - `temp/etmall/` - 處理過程中的臨時檔案
   - `data_processed/merged/` - 最終輸出檔案（腳本 10）
   - `logs/` - 日誌檔案
4. **配置檔案**：確認以下檔案完整
   - `config/etmall_fields_mapping.json` - 欄位映射配置
   - `config/A02_Shops_Master.json` - 商店主檔
   - `config/products.yaml` - 產品主檔

### 安裝依賴

```bash
pip install pandas pyyaml
```

### 執行順序

腳本必須按照順序執行，因為每個腳本都依賴前一個腳本的輸出：

```bash
# 1. 清理平台訂單
python 01_etmall_platform_orders_cleaner.py

# 2. 歸檔檔案
python 02_etmall_files_archiver.py

# 3. 合併出貨訂單
python 03_etmall_shipping_orders_merger.py

# 4. 合併銷售報表
python 04_etmall_sales_report_merger.py

# 5. 訂單去重
python 05_etmall_orders_deduplicator.py

# 6. 訂單合併
python 06_etmall_orders_merger.py

# 7. 日期時間處理
python 07_etmall_orders_datetime_processor.py

# 8. 欄位映射
python 08_etmall_orders_field_mapper.py

# 9. 商店資料豐富
python 09_etmall_orders_shop_enricher.py

# 10. 產品資料豐富
python 10_etmall_orders_product_enricher.py
```

## ⚙️ 配置說明

### 欄位映射配置 (`etmall_fields_mapping.json`)
定義中文字段名到英文字段名的映射關係，包含：
- 欄位順序
- 資料類型
- 是否必填
- 欄位描述

### 商店主檔配置 (`A02_Shops_Master.json`)
包含所有商店的詳細資訊，用於豐富訂單資料：
- 商店代號
- 商店名稱
- 商業模式
- 地區、部門、負責人等

### 產品配置 (`products.yaml`)
包含所有產品的詳細資訊，用於豐富訂單資料：
- 產品分類
- 品牌、系列
- 寵物類型
- 規格、供應商等

## 📊 資料流程

```
原始訂單檔案 → 清理 → 合併 → 去重 → 欄位映射 → 資料豐富 → 最終輸出 → BigQuery
     ↓              ↓       ↓       ↓        ↓         ↓         ↓
   01腳本        02-06腳本  05腳本   08腳本   09-10腳本  完成    上傳雲端
```

### 詳細流程說明

1. **資料輸入階段**（腳本 01-06）：
   - 原始檔案 → 清理 → 合併 → 去重 → 最終合併
   - 輸出保存到 `temp/etmall/` 目錄

2. **資料處理階段**（腳本 07-08）：
   - 日期時間標準化 → 欄位映射轉換
   - 輸出保存到 `temp/etmall/` 目錄

3. **資料豐富階段**（腳本 09-10）：
   - 商店資料豐富 → 產品資料豐富
   - 腳本 10 輸出保存到 `data_processed/merged/` 目錄

4. **雲端上傳階段**：
   - BigQuery 上傳器自動讀取 `data_processed/merged/` 目錄
   - 上傳到 Google Cloud BigQuery 進行雲端分析

## 🔍 輸出檔案說明

每個腳本執行後都會產生帶有時間戳的輸出檔案：

- **檔案命名格式**：`腳本名稱_YYYYMMDD_HHMMSS.csv`
- **輸出目錄**：
  - 腳本 01-09：`temp/etmall/`（臨時處理檔案）
  - 腳本 10：`data_processed/merged/`（最終輸出檔案）
- **檔案清理**：腳本會自動清理舊的處理檔案，只保留最新的
- **BigQuery 整合**：腳本 10 的輸出直接傳遞給 BigQuery 上傳器

## 📝 日誌記錄

- **日誌目錄**：`logs/`
- **日誌格式**：`腳本名稱_YYYYMMDD_HHMMSS.log`
- **記錄內容**：執行步驟、資料統計、錯誤資訊等

## ☁️ BigQuery 整合

### 自動上傳流程

腳本 10 執行完成後，資料會自動準備好進行 BigQuery 上傳：

1. **輸出檔案位置**：`data_processed/merged/etmall_orders_product_enriched_*.csv`
2. **上傳器路徑**：`scripts/bigquery_uploader/etmall_to_bigquery_uploader.py`
3. **目標資料表**：`shopee-etl-reporting.yichai_etmall_data.etmall_orders_data`

### 上傳方式

```bash
# 自動模式（推薦）
python scripts/bigquery_uploader/etmall_to_bigquery_uploader.py

# 指定上傳模式
python scripts/bigquery_uploader/etmall_to_bigquery_uploader.py --write_disposition WRITE_TRUNCATE

# 指定特定檔案
python scripts/bigquery_uploader/etmall_to_bigquery_uploader.py --csv data_processed/merged/etmall_orders_product_enriched_20250819_143022.csv
```

### 支援的上傳模式

- **WRITE_TRUNCATE**：覆蓋模式（清空後上傳，預設）
- **WRITE_APPEND**：追加模式（在現有資料後追加）
- **WRITE_EMPTY**：僅空資料表模式

## ⚠️ 注意事項

1. **執行順序**：腳本必須按照編號順序執行
2. **依賴關係**：每個腳本都依賴前一個腳本的輸出
3. **檔案管理**：腳本會自動管理檔案，無需手動清理
4. **錯誤處理**：執行失敗時請檢查日誌檔案
5. **資料備份**：建議在執行前備份重要資料

## 🐛 故障排除

### 常見問題

1. **找不到輸入檔案**
   - 檢查前一個腳本是否成功執行
   - 確認檔案路徑和命名格式

2. **配置檔案載入失敗**
   - 檢查配置檔案路徑
   - 確認 JSON/YAML 格式正確

3. **權限錯誤**
   - 確認對輸出目錄有寫入權限
   - 檢查檔案是否被其他程序佔用

### 日誌分析

每個腳本都會產生詳細的執行日誌，包含：
- 執行步驟
- 資料統計
- 錯誤訊息
- 處理結果

## 📞 支援

如有問題或建議，請：
1. 檢查日誌檔案
2. 確認配置檔案格式
3. 驗證輸入資料格式
4. 聯繫開發團隊

## 📄 授權

本專案遵循專案整體授權條款。

---

**最後更新**：2025年8月28日
**版本**：2.0.0
**更新內容**：
- 腳本 10 輸出路徑更新為 data_processed/merged
- 新增 BigQuery 整合說明
- 完善資料流程和目錄結構說明
- 更新配置檔案和前置需求說明

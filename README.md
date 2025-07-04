# EC Data Pipeline

> 多平台電商訂單 & 帳務自動處理框架（蝦皮、MOMO、PChome、東森、Yahoo⋯）  
> 讓 ETL/報表再也不手忙腳亂！

---

## 🚀 專案介紹

本專案致力於**整合多平台電商訂單資料**，並自動進行欄位對齊、密碼管理、格式轉換、母檔累積、拆分報表、資料上傳雲端（Google BigQuery）及匯入本地 ERP/PostgreSQL。  
一切流程自動化，讓你面對**複雜報表/跨部門問答**時，臉不紅氣不喘！

## 專案簡介
本專案為多平台電商訂單資料自動清洗、合併、欄位標準化與去重流程，支援 Shopee、Momo、PChome、Yahoo 等平台。

## 主要功能
- **多平台支援**：Shopee、Momo 平台訂單資料自動處理
- 批次讀取各平台匯出訂單 Excel/CSV，依欄位對應自動轉換英文標頭與順序
- 自動解析訂單編號前6碼為訂單日期（order_date）
- 產生商品唯一鍵（sku_key），合併時以 order_date+order_sn+sku_key 為唯一鍵
- item_seq 自動依同一訂單多商品流水號
- 新資料自動覆蓋舊資料，確保訂單狀態與明細為最新
- 支援多批次資料合併、去重、欄位自動補齊
- **自動移除 Excel 報表密碼，並轉存為 CSV，只保留 CSV 檔案**
- 處理過程與結果完整記錄於 log，並自動清理 temp 目錄及相關 log
- **統一處理架構**：多平台共用相同處理邏輯，確保資料一致性

## 目錄結構
- scripts/shopee_csv_to_master_cleaner.py：Shopee 訂單自動清洗、合併主腳本
- scripts/momo_csv_to_master_cleaner.py：Momo 訂單自動清洗、合併主腳本
- scripts/excel_password_remover/：自動移除 Excel 密碼並轉存 CSV
- config/shopee_fields_mapping.json：Shopee 欄位對應設定
- config/momo_fields_mapping.json：Momo 欄位對應設定
- data_processed/merged/shopee_master_orders_cleaned.csv：Shopee 合併後標準檔
- data_processed/merged/momo_master_orders_cleaned.csv：Momo 合併後標準檔

## 使用方式
1. 將各平台匯出 Excel/CSV 放入 data_raw/對應平台目錄/
2. 執行 `python scripts/excel_password_remover/main.py`，自動移除密碼並轉存為 CSV
3. 執行對應平台處理腳本：
   - Shopee：`python scripts/shopee_csv_to_master_cleaner.py`
   - Momo：`python scripts/momo_csv_to_master_cleaner.py`
4. 合併結果於 data_processed/merged/對應平台_master_orders_cleaned.csv

## 更新日誌
詳見 docs/README.md

---

## 📁 專案結構（目錄說明）

```
ec-data-pipeline
├── config/       # 所有配置、欄位定義、密碼設定
├── data_raw/     # 原始報表資料（平台/年月分資料夾）
├── data_processed/ # 處理後的資料（報表/合併/分群/彙總）
├── archive/      # 歷史歸檔（原始/報表）
├── scripts/      # 處理腳本（格式轉換、mapping工具、excel密碼移除）
│   └── excel_password_remover/ # Excel 密碼移除與轉檔
├── temp/         # 臨時檔案（解密/快取）
├── docs/         # 說明/數據字典
├── logs/         # 日誌
└── requirements.txt
```

### 重要說明  
- `config/`:  
  - `mapping.xlsx`: 欄位定義/各平台對照分頁  
  - `*_fields_mapping.json`: 各平台/主表欄位定義  
  - `ec_shops_universal_passwords.json`: 所有店家下載報表密碼/稅號統整  
  - `erp_db_connection.yaml`: 連線參數範例

- `data_raw/`:  
  - 各平台子資料夾存放原始下載/解密後報表，避免髒檔污染處理流程。

- `data_processed/`:  
  - 處理後合併檔、拆分維度、SKU/部門/平台等各種彙總。

- `scripts/`:  
  - `excel2mapping.py`: 欄位定義表自動轉 json/yaml 標準化 mapping 工具。  
  - `tree.py`: 專案結構一鍵印出。  
  - `shopee_csv_to_master_cleaner.py`: Shopee 訂單自動清洗、合併主腳本
  - `momo_csv_to_master_cleaner.py`: Momo 訂單自動清洗、合併主腳本
  - `excel_password_remover/`: Excel 密碼移除與自動轉 CSV

- `archive/`:  
  - 歷史原始資料、報表存放區，版本控管不易搞丟。

- `docs/`:  
  - 數據字典、設計說明、對外技術分享都可放這。

---

## 📝 操作說明

1. **原始報表解密、匯入**：  
   放到 `data_raw/平台/`，如 MOMO、Shopee、東森⋯  
   Shopee 請先執行 `python scripts/excel_password_remover/main.py`，自動移除密碼並轉存為 CSV。
2. **欄位自動對齊**：  
   依 `config/mapping.xlsx` 與各平台 json，轉換統一格式。
3. **自動產出母檔**：  
   所有平台合併/彙整至同一主表規格，存放於 `data_processed/merged/`。
4. **自動分群/拆報表**：  
   按 SKU、平台、部門等自動切分報表存於 `data_processed/reports/`。
5. **自動上傳雲端**：  
   上傳 Google BigQuery 並同步本地 PostgreSQL（腳本/自動排程）。
6. **密碼/權限統一管理**：  
   下載報表密碼/店家資訊都由 `config/ec_shops_universal_passwords.json` 控管。

---

## 🛠️ 快速開始

1. **安裝必要套件**  
   ```sh
   pip install -r requirements.txt
   ```
2. **建立資料夾結構**  
   - Windows CMD:
     ```
     mkdir config data_raw data_processed archive scripts temp docs logs
     mkdir data_raw\shopee data_raw\momo data_raw\etmall data_raw\pchome
     mkdir data_processed\merged data_processed\summary
     mkdir data_processed\reports\by_platform data_processed\reports\by_department data_processed\reports\by_sku
     mkdir archive\raw archive\reports
     mkdir temp\shopee temp\momo temp\etmall temp\pchome
     ```
   - Linux/macOS:
     ```
     mkdir -p config data_raw/shopee data_raw/momo data_raw/etmall data_raw/pchome \
       data_processed/merged data_processed/summary \
       data_processed/reports/by_platform data_processed/reports/by_department data_processed/reports/by_sku \
       archive/raw archive/reports \
       temp/shopee temp/momo temp/etmall temp/pchome \
       scripts docs logs
     ```
3. **執行腳本**  
   例如欄位定義轉換（excel2mapping.py）：
   ```
   python scripts/excel2mapping.py
   ```

---

## 🏷️ 專案精神

> 欄位不統一？密碼亂糟糟？一鍵就搞定！
>  
> 歡迎你加入未來電商營運「資料驅動」的行列——讓數據自己開會、自己對齊、自己上雲端！

---

## 🙋‍♂️ 聯絡窗口

專案維護人：楊翔志  
聯絡方式：bruce.yichai20250505@gmail.com

---

## 🔖 補充備註

- 本專案可自行擴充新平台欄位、串接 ERP/BQ/本地 DB
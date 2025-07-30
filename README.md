# EC Data Pipeline

> 多平台電商訂單與帳務自動化處理框架  
> Shopee、MOMO、PChome、東森、Yahoo 等平台報表一鍵整合、清洗、上雲端！

---

## 🌟 專案簡介

EC Data Pipeline 致力於解決多平台電商資料整合的痛點，提供自動化的訂單/帳務清洗、欄位標準化、密碼管理、格式轉換、合併去重、雲端上傳（Google BigQuery）、本地 ERP/PostgreSQL 匯入等一站式解決方案。  
讓你面對複雜報表、跨部門需求時，數據處理不再手忙腳亂！

---

## 🚀 主要功能

- **多平台支援**：Shopee、Momo、PChome、東森、Yahoo 等主流平台訂單自動處理
- **自動欄位對齊**：依 mapping 設定自動轉換欄位名稱與順序
- **密碼自動移除**：Excel 報表密碼自動解除並轉存為 CSV
- **唯一鍵生成**：order_date + order_sn + sku_key 組合唯一鍵，確保資料不重複
- **批次合併/去重**：多批次資料自動合併、去重、欄位補齊
- **完整日誌**：處理過程與結果自動記錄，temp/logs 自動清理
- **雲端/本地同步**：支援 Google BigQuery 上傳與本地 ERP/PostgreSQL 匯入
- **彈性擴充**：可自訂新平台欄位、串接更多資料庫

---

## 📁 目錄結構

```
ec-data-pipeline/
├── config/                # 欄位定義、密碼、連線設定
├── data_raw/              # 原始報表（依平台/年月分資料夾）
├── data_processed/        # 處理後資料（合併/分群/彙總）
├── archive/               # 歷史歸檔
├── scripts/               # 處理腳本（ETL、密碼移除、mapping 工具）
│   ├── excel_password_remover/   # Excel 密碼移除與轉檔
│   ├── momo_orders_etl/          # Momo 訂單 ETL 處理
│   └── shopee_csv_to_master_cleaner.py # Shopee 訂單處理
├── temp/                  # 臨時檔案
├── docs/                  # 說明/數據字典
├── logs/                  # 日誌
└── requirements.txt       # 依賴套件
```

---

## ⚡ 快速開始

1. **安裝依賴套件**
   ```sh
   pip install -r requirements.txt
   ```

2. **建立資料夾結構**（如未自動建立）
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

3. **資料處理流程**
   1. 將各平台匯出 Excel/CSV 放入 `data_raw/平台/`
   2. 執行密碼移除與轉檔：
      ```
      python scripts/excel_password_remover/main.py
      ```
   3. 執行對應平台清洗/合併腳本：
      - Shopee：
        ```
        python scripts/shopee_csv_to_master_cleaner.py
        ```
      - Momo：
        ```
        python scripts/momo_csv_to_master_cleaner.py
        ```
      - Momo ETL：
        ```
        python scripts/momo_orders_etl/momo_batch_processor.py
        ```
      - **PChome（建議順序）：**
        ```
        python scripts/pchome_orders_etl/pchome_cleaner.py
        python scripts/pchome_orders_etl/pchome_return_cleaner.py
        python scripts/pchome_orders_etl/pchome_orders_merger.py
        ```
   4. 合併結果於 `data_processed/merged/平台_master_orders_cleaned.csv` 或 `data_processed/merged/pchome_orders_merged.csv`

---

## 📝 操作說明

- 欄位定義、平台對應請見 `config/` 內 mapping.xlsx、各平台 *_fields_mapping.json
- 密碼/權限統一管理於 `config/ec_shops_universal_passwords.json`
- 處理過程自動產生日誌於 `logs/`
- 歷史資料、報表歸檔於 `archive/`
- 欄位定義自動轉換工具：`python scripts/excel2mapping.py`
- **PChome 處理流程完整說明請見** `scripts/pchome_orders_etl/README.md`

---

## 🆕 近期重大更新

- **2024-07-23 PChome 處理流程重大優化：**
  - pchome_cleaner.py、pchome_return_cleaner.py 欄位自動對應邏輯強化，支援括號、全形/半形、特殊符號自動正規化，mapping 能正確對應如「收貨地址(訂單編號)」等欄位。
  - receiver_addr（收貨地址）、receiver_phone（收貨人電話）、receiver_zip（郵遞區號）等欄位自動補齊，並加強自動對應與補空欄機制。
  - 星期幾欄位調整為「星期日=1，星期一=2，...，星期六=7」，與需求一致。
  - 商品名稱自動解析商品ID與選項編號，正則與補零邏輯與 mapping 完全同步。
  - 退貨清洗流程（pchome_return_cleaner.py）與一般訂單清洗流程（pchome_cleaner.py）欄位生成、型態轉換、唯一鍵、補空欄等完全一致，合併分析無落差。
  - pchome_orders_merger.py 合併時同時納入 pchome_*.csv 及 pchome_return_*.csv，欄位順序與格式依 mapping，自動補空欄，唯一鍵去重。
  - scripts/pchome_orders_etl/ 新增 README.md，詳細說明三個腳本的功能、步驟、執行方式與建議順序。
  - 修正多處 mapping 對不到、欄位全空、欄位自動補空與自動對應問題，提升多平台資料自動化處理穩定性。

---

## 🏷️ 專案精神

> 欄位不統一？密碼亂糟糟？一鍵就搞定！  
> 讓數據自己開會、自己對齊、自己上雲端！

---

## 🙋‍♂️ 聯絡窗口

- 專案維護人：楊翔志  
- Email：bruce.yichai20250505@gmail.com

---

## 🔖 備註

- 本專案可彈性擴充新平台、串接 ERP/BQ/本地 DB
- 詳細更新日誌請見 `docs/README.md`

---

如需進一步協助，歡迎來信聯絡！
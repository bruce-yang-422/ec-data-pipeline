# EC Data Pipeline

> **多平台電商訂單與帳務自動化處理框架**  
> Shopee、MOMO、PChome、東森、Yahoo 等平台報表一鍵整合、清洗、上雲端！

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-green.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## 🌟 專案簡介

EC Data Pipeline 致力於解決多平台電商資料整合的痛點，提供自動化的訂單/帳務清洗、欄位標準化、密碼管理、格式轉換、合併去重、雲端上傳（Google BigQuery）、本地 ERP/PostgreSQL 匯入等一站式解決方案。

讓你面對複雜報表、跨部門需求時，數據處理不再手忙腳亂！

### ✨ 核心價值

- 🔄 **自動化處理**：從原始報表到清洗完成的資料，全程自動化
- 🎯 **標準化輸出**：統一的欄位格式和資料結構，便於分析
- 🚀 **多平台支援**：支援主流電商平台，可輕鬆擴展新平台
- 📊 **雲端整合**：直接上傳至 Google BigQuery，支援大數據分析
- 🛡️ **安全可靠**：自動密碼處理、完整日誌記錄、錯誤處理機制

---

## 🚀 主要功能

### 📦 平台支援
- **Shopee**：蝦皮購物訂單處理
- **MOMO**：富邦媒體訂單與帳務處理 (A1102, C1105)
- **PChome**：網路家庭訂單與退貨處理
- **東森購物 (ETMall)**：完整的 5 階段 ETL 流程
- **Yahoo**：雅虎購物中心訂單處理

### 🔧 核心功能
- **自動欄位對齊**：依 mapping 設定自動轉換欄位名稱與順序
- **密碼自動移除**：Excel 報表密碼自動解除並轉存為 CSV
- **唯一鍵生成**：order_date + order_sn + sku_key 組合唯一鍵，確保資料不重複
- **批次合併/去重**：多批次資料自動合併、去重、欄位補齊
- **產品資訊匹配**：自動匹配產品主檔，豐富訂單資訊
- **店家資訊增強**：根據店家主檔自動補充店家相關資訊
- **完整日誌**：處理過程與結果自動記錄，logs/ 自動清理
- **雲端/本地同步**：支援 Google BigQuery 上傳與本地 ERP/PostgreSQL 匯入

---

## 📁 專案結構

```
ec-data-pipeline/
├── 📂 config/                    # 配置文件
│   ├── 📁 env/                   # 環境配置
│   ├── 📋 A02_Shops_Master.json  # 店家主檔
│   ├── 📋 products.yaml          # 產品主檔
│   ├── 📋 *_fields_mapping.json  # 各平台欄位對應
│   ├── 📈 mapping.xlsx           # 主要欄位定義
│   └── 📋 ec_shops_universal_passwords.json # 密碼管理
│
├── 📂 data_raw/                  # 原始資料
│   ├── 📂 shopee/               # 蝦皮原始報表
│   ├── 📂 momo/                 # MOMO 原始報表
│   ├── 📂 pchome/               # PChome 原始報表
│   ├── 📂 etmall/               # 東森購物原始報表
│   │   └── 📂 backup/           # 原始檔案備份
│   └── 📂 Yahoo/                # Yahoo 原始報表
│
├── 📂 data_processed/            # 處理後資料
│   ├── 📂 merged/               # 最終合併資料
│   ├── 📂 reports/              # 分析報表
│   │   ├── 📂 by_platform/      # 依平台分類
│   │   ├── 📂 by_department/    # 依部門分類
│   │   └── 📂 by_sku/          # 依商品分類
│   ├── 📂 check/                # 資料檢查
│   └── 📂 summary/              # 摘要資料
│
├── 📂 scripts/                   # 處理腳本
│   ├── 📂 etmall_orders_etl/    # 東森購物 ETL 流程
│   │   ├── 🐍 01_etmall_xlsx_to_csv.py      # Excel 轉 CSV
│   │   ├── 🐍 02_etmall_orders_cleaner.py   # 資料清理
│   │   ├── 🐍 03_etmall_merger.py           # 資料合併
│   │   ├── 🐍 04_etmall_orders_enricher.py  # 店家資訊增強
│   │   └── 🐍 05_etmall_orders_product_matcher.py # 產品匹配
│   ├── 📂 momo_orders_etl/       # MOMO ETL 處理
│   ├── 📂 pchome_orders_etl/     # PChome ETL 處理
│   ├── 📂 yahoo_orders_etl/      # Yahoo ETL 處理
│   ├── 📂 excel_password_remover/ # Excel 密碼移除工具
│   ├── 📂 bigquery_uploader/     # BigQuery 上傳工具
│   └── 📂 sku_name_mapping_tool/ # SKU 對應工具
│
├── 📂 temp/                      # 臨時文件
│   ├── 📂 etmall/               # 東森購物處理中檔案
│   ├── 📂 momo/                 # MOMO 處理中檔案
│   ├── 📂 pchome/               # PChome 處理中檔案
│   └── 📂 shopee/               # 蝦皮處理中檔案
│
├── 📂 archive/                   # 歷史歸檔
│   ├── 📂 raw/                  # 原始檔案歷史
│   └── 📂 reports/              # 報表歷史
│
├── 📂 logs/                      # 系統日誌
├── 📂 docs/                      # 文檔說明
├── 📖 README.md                  # 專案說明
└── 📋 requirements.txt           # 依賴套件
```

---

## ⚡ 快速開始

### 1. 環境準備

```bash
# 克隆專案
git clone <repository-url>
cd ec-data-pipeline

# 安裝依賴套件
pip install -r requirements.txt

# 或使用虛擬環境（推薦）
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. 目錄初始化

如果某些目錄不存在，請手動建立：

**Windows CMD:**
```cmd
mkdir config data_raw data_processed archive scripts temp docs logs
mkdir data_raw\shopee data_raw\momo data_raw\etmall data_raw\pchome data_raw\Yahoo
mkdir data_processed\merged data_processed\summary data_processed\check
mkdir data_processed\reports\by_platform data_processed\reports\by_department data_processed\reports\by_sku
mkdir archive\raw archive\reports
mkdir temp\shopee temp\momo temp\etmall temp\pchome temp\Yahoo
```

**Linux/macOS:**
```bash
mkdir -p {config,data_raw/{shopee,momo,etmall,pchome,Yahoo},data_processed/{merged,summary,check,reports/{by_platform,by_department,by_sku}},archive/{raw,reports},temp/{shopee,momo,etmall,pchome,Yahoo},scripts,docs,logs}
```

### 3. 配置設定

1. 複製並編輯配置文件：
   - `config/mapping.xlsx` - 主要欄位定義
   - `config/*_fields_mapping.json` - 各平台欄位對應
   - `config/products.yaml` - 產品主檔
   - `config/A02_Shops_Master.json` - 店家主檔

2. 設定 BigQuery 金鑰（如需使用）：
   - 將 Google Cloud 服務帳號金鑰放置於 `config/bigquery_uploader_key.json`

---

## 🛠️ 使用指南

### 東森購物 (ETMall) - 完整 ETL 流程

東森購物提供了最完整的 5 階段 ETL 處理流程：

1. **將原始報表放入目錄**
   ```
   data_raw/etmall/東森購物_YYYYMMDD_001.xls
   ```

2. **執行 ETL 流程**（按順序執行）：

   ```bash
   # 階段 1: Excel 轉 CSV + 備份
   python scripts/etmall_orders_etl/01_etmall_xlsx_to_csv.py
   
   # 階段 2: 資料清理與標準化
   python scripts/etmall_orders_etl/02_etmall_orders_cleaner.py
   
   # 階段 3: 資料合併與去重
   python scripts/etmall_orders_etl/03_etmall_merger.py
   
   # 階段 4: 店家資訊增強
   python scripts/etmall_orders_etl/04_etmall_orders_enricher.py
   
   # 階段 5: 產品資訊匹配
   python scripts/etmall_orders_etl/05_etmall_orders_product_matcher.py
   ```

3. **結果輸出**
   - 最終處理結果：`temp/etmall/05_etmall_orders_product_matched_YYYYMMDD_HHMMSS.csv`

### 其他平台處理

#### MOMO 購物
```bash
# 基本清理
python scripts/momo_csv_to_master_cleaner.py

# 或使用 ETL 批次處理
python scripts/momo_orders_etl/momo_batch_processor.py
```

#### PChome 購物
```bash
# 建議執行順序
python scripts/pchome_orders_etl/pchome_cleaner.py
python scripts/pchome_orders_etl/pchome_return_cleaner.py
python scripts/pchome_orders_etl/pchome_orders_merger.py
```

#### 蝦皮購物 (Shopee)
```bash
python scripts/shopee_csv_to_master_cleaner.py
```

#### Yahoo 購物
```bash
python scripts/yahoo_orders_etl/yahoo_cleaner.py
```

### 工具腳本

#### Excel 密碼移除
```bash
python scripts/excel_password_remover/main.py
```

#### BigQuery 上傳
```bash
python scripts/bigquery_uploader/bigquery_uploader.py
```

#### 日誌清理
```bash
python scripts/clear_logs.py
```

---

## 📊 資料流程圖

```
原始報表 (Excel/CSV)
    ↓
密碼移除 & 格式轉換
    ↓
欄位對應 & 資料清理
    ↓
資料合併 & 去重
    ↓
資訊增強 (店家/產品)
    ↓
最終輸出 (CSV)
    ↓
雲端上傳 (BigQuery) / 本地匯入
```

---

## 🔧 配置說明

### 欄位對應配置
- `config/mapping.xlsx` - 主要欄位定義和對應規則
- `config/*_fields_mapping.json` - 各平台特定的欄位對應

### 主檔配置
- `config/products.yaml` - 產品主檔，用於產品資訊匹配
- `config/A02_Shops_Master.json` - 店家主檔，用於店家資訊增強

### 密碼管理
- `config/ec_shops_universal_passwords.json` - 統一密碼管理

---

## 📋 依賴套件

主要依賴包括：
- `pandas` - 資料處理
- `openpyxl` - Excel 檔案處理
- `PyYAML` - YAML 檔案處理
- `google-cloud-bigquery` - BigQuery 上傳
- `msoffcrypto-tool` - Excel 密碼移除

完整依賴請參考 `requirements.txt`。

---

## 🆕 版本更新

### v2.0.0 (2025-08-05) - 東森購物 ETL 重構
- ✨ **重大更新**：將東森購物 ETL 流程重構為 5 個獨立模組化腳本
- 🔧 優化 Excel 檔案轉換、日期格式化、檔案命名邏輯
- 🛡️ 大幅提升穩定性和重複資料處理能力
- 📊 統一資料清理與增強流程，確保符合 BigQuery 欄位型態要求
- 🎯 新增根據店家主檔和產品主檔自動增強訂單資料功能

### v1.5.0 (2024-07-23) - PChome 處理流程優化
- 🔧 欄位自動對應邏輯強化，支援括號、全形/半形、特殊符號正規化
- 📍 收貨地址、電話、郵遞區號欄位自動補齊機制
- 📅 星期欄位調整為標準格式（星期日=1，...，星期六=7）
- 🏷️ 商品名稱自動解析商品ID與選項編號
- 🔄 退貨清洗流程與一般訂單流程完全同步
- 📝 新增詳細的 README 文檔

---

## 🏷️ 專案精神

> **欄位不統一？密碼亂糟糟？一鍵就搞定！**  
> **讓數據自己開會、自己對齊、自己上雲端！**

---

## 🤝 貢獻指南

1. Fork 本專案
2. 建立功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交變更 (`git commit -m 'Add some AmazingFeature'`)
4. 推送分支 (`git push origin feature/AmazingFeature`)
5. 開啟 Pull Request

---

## 📞 聯絡資訊

- **專案維護人**：楊翔志
- **Email**：bruce.yichai20250505@gmail.com
- **專案類型**：企業內部資料處理工具

---

## 📜 授權條款

本專案採用 MIT 授權條款。詳細資訊請參考 `LICENSE` 文件。

---

## 🔖 附註

- 本專案可彈性擴充新平台、串接 ERP/BigQuery/本地資料庫
- 詳細更新日誌和技術文檔請參考 `docs/` 目錄
- 如需技術支援或功能定制，歡迎聯絡專案維護人

**讓數據處理變得簡單，讓分析決策更有效率！** 🚀
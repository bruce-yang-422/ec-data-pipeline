# Excel Password Remover 工具

## 功能概述

這個工具提供完整的檔案處理流程，包括：

1. **自動解壓縮**：支援 ZIP 和 RAR 檔案解壓縮（含密碼保護）
2. **Excel 密碼移除**：自動移除受密碼保護的 Excel 檔案
3. **格式轉換**：將 Excel 檔案轉換為 CSV 格式
4. **資料清理**：清理 CSV 資料中的空格和換行符號
5. **批次處理**：支援多平台、多帳號、多密碼的批次處理

## 支援的檔案格式

- **壓縮檔案**：`.zip`、`.rar`
- **Excel 檔案**：`.xlsx`、`.xls`
- **資料檔案**：`.csv`

## 支援的平台

工具會根據目錄名稱自動識別平台並使用對應的密碼：

- `etmall`：東森購物（密碼：17279274）
- `momo`：MOMO購物中心（密碼：A17279274）
- `shopee`：蝦皮購物
- `pchome`：PC購物中心
- `yahoo`：Yahoo購物中心

## 使用方法

### 主要腳本

```bash
# 執行完整的處理流程
python scripts/excel_password_remover/main.py
```

### 測試腳本

```bash
# 測試解壓縮功能
python scripts/excel_password_remover/test_extract.py
```

## 檔案結構

```
scripts/excel_password_remover/
├── main.py              # 主要處理腳本
├── utils.py             # 工具函數（解壓縮、密碼處理）
├── remover.py           # Excel 密碼移除功能
├── test_extract.py      # 測試腳本
└── README.md           # 說明文件
```

## 處理流程

1. **解壓縮階段**：
   - 掃描 `data_raw/` 下各平台目錄
   - 自動解壓縮 ZIP/RAR 檔案到 `temp/平台名稱/`
   - 使用對應平台的密碼進行解壓縮

2. **密碼移除階段**：
   - 掃描所有 Excel 檔案
   - 根據平台名稱或檔案名稱匹配密碼
   - 移除 Excel 檔案密碼保護

3. **格式轉換階段**：
   - 將所有 Excel 檔案轉換為 CSV 格式
   - 保持原始檔案結構

4. **資料清理階段**：
   - 清理 CSV 資料中的空格和換行符號
   - 移除多餘的空格
   - 標準化資料格式

5. **檔案整理階段**：
   - 刪除原始 Excel 檔案
   - 只保留清理後的 CSV 檔案

## 密碼配置

密碼配置檔案：`config/ec_shops_universal_passwords.json`

```json
[
    {
        "shop_id": "ET0001",
        "shop_name": "東森購物",
        "shop_account": "541767",
        "report_download_password": "17279274",
        "keywords": ["東森", "東森購物", "森森"]
    }
]
```

## 輸出結果

- **處理後的檔案**：`temp/平台名稱/`
- **執行日誌**：`logs/execution_log_YYYYMMDD_HHMMSS.txt`
- **清理後的 CSV**：保留在 `temp/` 目錄下

## 注意事項

1. **RAR 檔案處理**：
   - 需要安裝 `unrar` 命令列工具或 `pyunpack` Python 套件
   - Windows 用戶可能需要安裝 WinRAR 或 7-Zip

2. **密碼處理**：
   - 工具會自動根據平台名稱匹配密碼
   - 如果找不到對應密碼，會嘗試使用檔案名稱匹配

3. **檔案編碼**：
   - 支援多種編碼格式（UTF-8、Big5、GBK 等）
   - 自動檢測並處理編碼問題

## 錯誤處理

- 所有處理過程都會記錄在日誌檔案中
- 失敗的檔案不會影響其他檔案的處理
- 提供詳細的錯誤訊息和處理統計

## 依賴套件

確保已安裝以下 Python 套件：

```bash
pip install pandas openpyxl msoffcrypto-tool pyunpack patool
```

## 範例使用

```bash
# 1. 將壓縮檔案放入對應平台目錄
# data_raw/etmall/orders.zip
# data_raw/momo/reports.rar

# 2. 執行處理腳本
python scripts/excel_password_remover/main.py

# 3. 檢查結果
# temp/etmall/orders.csv
# temp/momo/reports.csv
``` 
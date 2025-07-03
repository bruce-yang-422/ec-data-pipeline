# excel2mapping.py
"""
腳本用途：
--------
將 config/mapping.xlsx 內所有分頁(sheet)，
依每一分頁的「欄位定義表」自動轉出對應的 json mapping 檔，
每個分頁對應一份 {sheet}_fields_mapping.json

使用場景：
- 各平台/主表欄位定義全部集中在一份 mapping.xlsx，不用分散管理
- 每分頁維護「order, 欄位英文, 欄位中文, 型態, 說明, 是否必填, 備註」
- 新增平台或 schema，只要多加一個分頁即可

適用流程：
- ETL 資料校驗、型態轉換、mapping 自動對照
- 資料庫建表/查帳/欄位比對
- 資料治理欄位定義單一來源（single source of truth）

表頭需求：
-----------
每一分頁需有以下欄位（順序不限，名稱需完全相同）：
    order, 欄位英文, 欄位中文, 型態, 說明, 是否必填, 備註

輸出範例（momo_fields_mapping.json）：
----------------------------------------
{
    "order_id": {
        "order": "1",
        "zh_name": "訂單編號",
        "type": "STRING",
        "description": "MOMO原始訂單編號",
        "required": "是",
        "note": ""
    },
    ...
}
"""

import os
import pandas as pd
import json

# ===== 參數設定 =====
CONFIG_DIR = os.path.join(os.path.dirname(__file__), '../config')
MAPPING_XLSX = os.path.join(CONFIG_DIR, 'mapping.xlsx')
OUTPUT_TEMPLATE = os.path.join(CONFIG_DIR, '{}_fields_mapping.json')

# ===== 主程式開始 =====
def main():
    """
    讀取 mapping.xlsx 內所有分頁，逐一轉換成 json 欄位定義檔
    """
    # 讀取所有分頁到 dict
    df_dict = pd.read_excel(MAPPING_XLSX, dtype=str, sheet_name=None)

    for sheet, df in df_dict.items():
        df = df.fillna('')
        mapping = {}
        for _, row in df.iterrows():
            key = row['欄位英文'].strip()
            mapping[key] = {
                'order': row['order'].strip(),
                'zh_name': row['欄位中文'].strip(),
                'type': row['型態'].strip(),
                'description': row['說明'].strip(),
                'required': row['是否必填'].strip(),
                'note': row['備註'].strip()
            }
        out_file = OUTPUT_TEMPLATE.format(sheet.lower())
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
        print(f'✅ 分頁 {sheet} → {os.path.basename(out_file)}')

    print('全部分頁轉換完成！')

if __name__ == "__main__":
    main()

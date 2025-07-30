import pandas as pd
from pathlib import Path

# 設定檔案路徑
project_root = Path(__file__).parents[2]
cleaned_path = project_root / 'data_processed' / 'merged' / 'etmall_orders_cleaned.csv'
raw_path = project_root / 'data_raw' / 'etmall' / '東森_訂單銷售報表20250718122504.csv'
output_path = project_root / 'data_processed' / 'merged' / 'etmall_shipping_date_diff.csv'

# 讀取檔案
cleaned_df = pd.read_csv(cleaned_path, dtype=str)
raw_df = pd.read_csv(raw_path, dtype=str)

# 標準化欄位名稱
cleaned_df = cleaned_df.rename(columns=lambda x: x.strip())
raw_df = raw_df.rename(columns=lambda x: x.strip())

# 產生唯一 key
cleaned_df['key'] = cleaned_df['order_date'].astype(str).str.strip() + '_' + cleaned_df['order_sn'].astype(str).str.strip() + '_' + cleaned_df['line_number'].astype(str).str.strip()

# 嘗試找出原始檔案的訂單日期、訂單編號、項次欄位名稱
raw_order_date_col = None
raw_order_sn_col = None
raw_line_number_col = None
for col in raw_df.columns:
    if '訂單日期' in col:
        raw_order_date_col = col
    if '訂單編號' in col:
        raw_order_sn_col = col
    if '項次' in col:
        raw_line_number_col = col

if not all([raw_order_date_col, raw_order_sn_col, raw_line_number_col]):
    raise Exception(f'原始檔案缺少必要欄位: 訂單日期({raw_order_date_col}), 訂單編號({raw_order_sn_col}), 項次({raw_line_number_col})')

raw_df['key'] = raw_df[raw_order_date_col].astype(str).str.strip() + '_' + raw_df[raw_order_sn_col].astype(str).str.strip() + '_' + raw_df[raw_line_number_col].astype(str).str.strip()

# shipping_confirmation_date 欄位名稱
cleaned_ship_col = 'shipping_confirmation_date'
raw_ship_col = None
for col in raw_df.columns:
    if '配送確認日' in col:
        raw_ship_col = col
        break
if not raw_ship_col:
    for col in raw_df.columns:
        if '出貨確認日' in col or 'shipping_confirmation_date' in col:
            raw_ship_col = col
            break
if not raw_ship_col:
    raise Exception('原始檔案缺少出貨/配送確認日欄位')

# 合併比對
merged = pd.merge(
    cleaned_df[['key', cleaned_ship_col]],
    raw_df[['key', raw_ship_col]],
    on='key',
    how='inner',
    suffixes=('_cleaned', '_raw')
)

# 找出 shipping_confirmation_date 不一致的紀錄
mask = merged[f'{cleaned_ship_col}'] != merged[f'{raw_ship_col}']
diff_df = merged[mask].copy()

diff_df = diff_df.rename(columns={
    f'{cleaned_ship_col}': 'shipping_confirmation_date_cleaned',
    f'{raw_ship_col}': 'shipping_confirmation_date_raw'
})

diff_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'比對完成，不一致紀錄已存為: {output_path}') 
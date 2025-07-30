import pandas as pd
import os
from datetime import datetime

# 指定分析目標檔案
csv_path = 'data_raw/pchome/PCHOME_5月退貨2025072209_DBFF044DF62E4E744EC5.csv'

log_dir = 'logs'
log_file = os.path.join(log_dir, 'pchome_return_xray.log')
os.makedirs(log_dir, exist_ok=True)

def log(msg):
    print(msg)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')

log(f"====== X光檢查開始：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")

if not os.path.exists(csv_path):
    log(f'[ERROR] 找不到指定檔案: {csv_path}')
    exit(1)

encodings_to_try = ['utf-8-sig', 'utf-8', 'utf-16', 'big5', 'cp950']
read_success = False
df = None
for enc in encodings_to_try:
    try:
        df = pd.read_csv(csv_path, encoding=enc, header=0, dtype=str, keep_default_na=False)
        log(f"[INFO] 以編碼 {enc} 讀取成功")
        read_success = True
        break
    except Exception as e:
        log(f"[WARN] 以編碼 {enc} 讀取失敗: {e}")
if not read_success:
    log(f"[ERROR] 所有常見編碼皆無法讀取 {csv_path}")
    exit(1)

log(f"[INFO] 檢查檔案: {csv_path}")
log("\n--- 前五行內容 ---")
log(df.head(5).to_string())

log("\n--- 欄位名稱 ---")
log(str(list(df.columns)))

log("\n--- 資料型態 ---")
log(str(df.dtypes))

from io import StringIO
buf = StringIO()
df.info(buf=buf)
log("\n--- 資料缺漏狀況 ---")
log(buf.getvalue())

# 額外：每個欄位的前3筆資料
log("\n--- 各欄位前3筆資料 ---")
for col in df.columns:
    log(f"{col}: {df[col].head(3).tolist()}")

log(f"====== X光檢查結束：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")

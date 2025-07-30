import os
import json
import pandas as pd
from google.cloud import bigquery
from pathlib import Path
from datetime import datetime

# 設定參數
project_root = Path(__file__).parents[2]
csv_path = project_root / 'data_processed' / 'merged' / 'etmall_orders_cleaned.csv'
mapping_path = project_root / 'config' / 'etmall_fields_mapping.json'
key_path = project_root / 'config' / 'bigquery_uploader_key.json'
target_table = 'shopee-etl-reporting.yichai_etmall_data.etmall_orders_data'

# 設定 GOOGLE_APPLICATION_CREDENTIALS 環境變數
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(key_path.resolve())

# 讀取 mapping 設定
with open(mapping_path, 'r', encoding='utf-8') as f:
    mapping = json.load(f)

# 產生 BigQuery schema
schema = []
col_types: dict[str, str] = {}
for en, v in mapping.items():
    field_type = v.get('bq_type', v.get('type', 'STRING'))
    schema.append(bigquery.SchemaField(en, field_type))
    col_types[en] = field_type

# 讀取資料
print(f'讀取資料: {csv_path}')
df: pd.DataFrame = pd.read_csv(csv_path, dtype=str)

# 型態轉換
for col, bq_type in col_types.items():
    if col not in df.columns:
        continue
    if bq_type == 'STRING':
        df[col] = df[col].astype(str).where(df[col].notnull(), None)
    elif bq_type == 'INT64':
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        df[col] = df[col].where(df[col].notnull(), None)
    elif bq_type == 'NUMERIC':
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].where(df[col].notnull(), None)
    elif bq_type == 'TIMESTAMP':
        def to_bq_ts(val: str | float | None) -> str | None:
            if pd.isnull(val) or str(val).strip() == '':
                return None
            s = str(val).strip()
            # 若已經有時區，直接回傳
            if any(tz in s for tz in ['+', '-', 'Z']):
                return s
            # 若只有日期，補 00:00:00
            if len(s) == 10:
                s += ' 00:00:00'
            # 若沒有時區，補 +08:00
            try:
                dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                return dt.strftime('%Y-%m-%d %H:%M:%S+08:00')
            except Exception:
                return s + '+08:00'
        df[col] = df[col].apply(to_bq_ts)
    else:
        df[col] = df[col].where(df[col].notnull(), None)

# 空值處理（保險起見）
for col in df.columns:
    df[col] = df[col].where(df[col].notnull(), None)

# 上傳到 BigQuery
client = bigquery.Client()
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=False,
    encoding='UTF-8',
)

# 重新輸出暫存檔，確保型態正確
_temp_path = csv_path.parent / (csv_path.stem + '_bq_upload.csv')
df.to_csv(_temp_path, index=False, encoding='utf-8-sig')

with open(_temp_path, 'rb') as f:
    job = client.load_table_from_file(f, target_table, job_config=job_config)

print('上傳中...')
job.result()
print(f'✅ 上傳完成: {target_table}') 
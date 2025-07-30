#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery 工具函數模組

主要功能：
- BigQuery 客戶端建立與認證
- CSV 檔案上傳至 BigQuery
- 重複資料檢查與處理
- 資料表存在性檢查
- 資料表資訊查詢

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import List, Optional, Dict, Any


def get_bq_client(credential_path: str) -> bigquery.Client:
    """建立 BigQuery 客戶端"""
    try:
        # 設定環境變數
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
        
        # 建立客戶端
        client = bigquery.Client()
        
        print(f"✅ BigQuery 客戶端建立成功")
        return client
        
    except Exception as e:
        print(f"❌ BigQuery 客戶端建立失敗: {e}")
        raise


def upload_csv_to_bq(
    client: bigquery.Client,
    csv_path: str,
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
    write_disposition: str = "WRITE_APPEND",
    logger=None
) -> bool:
    """上傳 CSV 檔案至 BigQuery"""
    try:
        # 檢查檔案是否存在
        if not os.path.exists(csv_path):
            error_msg = f"❌ CSV 檔案不存在: {csv_path}"
            if logger:
                logger.error(error_msg)
            else:
                print(error_msg)
            return False
            
        # 檢查檔案大小
        file_size = os.path.getsize(csv_path)
        if file_size == 0:
            error_msg = f"❌ CSV 檔案為空: {csv_path}"
            if logger:
                logger.error(error_msg)
            else:
                print(error_msg)
            return False
            
        size_msg = f"📊 CSV 檔案大小: {file_size / 1024 / 1024:.2f} MB"
        if logger:
            logger.info(size_msg)
        else:
            print(size_msg)
        
        # 建立 table 參考
        table_ref = client.dataset(dataset_id).table(table_id)
        
        # 設定 job config
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # 跳過標題行
            autodetect=False,  # 使用自定義 schema
        )
        
        # 執行上傳
        upload_msg = f"📤 開始上傳至 {dataset_id}.{table_id}..."
        if logger:
            logger.info(upload_msg)
        else:
            print(upload_msg)
        
        with open(csv_path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            )
            
        # 等待完成
        job.result()
        
        # 檢查結果
        table = client.get_table(table_ref)
        success_msg = f"✅ 上傳成功！資料表 {table_id} 共有 {table.num_rows} 筆資料"
        if logger:
            logger.info(success_msg)
        else:
            print(success_msg)
        
        return True
        
    except Exception as e:
        error_msg = f"❌ 上傳失敗: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(error_msg)
        return False


def check_duplicate_order_sn(df: pd.DataFrame) -> Optional[List[str]]:
    """檢查 order_sn 重複"""
    try:
        if 'order_sn' not in df.columns:
            print("⚠️ 資料框中沒有 order_sn 欄位")
            return None
            
        # 找出重複的 order_sn
        duplicates = df[df['order_sn'].duplicated(keep=False)]
        
        if len(duplicates) > 0:
            duplicate_sns = duplicates['order_sn'].unique().tolist()
            print(f"⚠️ 發現 {len(duplicate_sns)} 個重複的 order_sn")
            return duplicate_sns
        else:
            print("✅ 無重複的 order_sn")
            return None
            
    except Exception as e:
        print(f"❌ 檢查重複時發生錯誤: {e}")
        return None


def check_table_exists(client: bigquery.Client, dataset_id: str, table_id: str) -> bool:
    """檢查 BigQuery 資料表是否存在"""
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def get_table_info(client: bigquery.Client, dataset_id: str, table_id: str) -> Optional[Dict[str, Any]]:
    """取得資料表資訊"""
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        
        return {
            'table_id': table.table_id,
            'dataset_id': table.dataset_id,
            'project': table.project,
            'num_rows': table.num_rows,
            'num_bytes': table.num_bytes,
            'created': table.created,
            'modified': table.modified,
            'schema': [field.name for field in table.schema]
        }
        
    except Exception as e:
        print(f"❌ 取得資料表資訊失敗: {e}")
        return None

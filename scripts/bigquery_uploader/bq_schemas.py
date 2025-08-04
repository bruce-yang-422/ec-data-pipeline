#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery 資料表結構定義模組

主要功能：
- 定義 BigQuery 資料表的結構 Schema
- 包含欄位名稱、資料類型、必填欄位等詳細資訊
- 支援資料品質檢查與驗證

已定義的資料表結構：
- c1105_momo_accounting_orders: Momo 帳務對帳資料表
- a1102_momo_shipping_orders: Momo 物流對帳資料表

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

from google.cloud import bigquery

# Momo 帳務對帳資料表結構
# 用於儲存 Momo 平台訂單的帳務對帳資料，包含訂單基本資訊、商品詳情、
# 價格資訊、物流狀態等完整資料。此資料表主要用於財務對帳和營收分析。
c1105_momo_accounting_orders_schema = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("order_sn_main", "INTEGER"),
    bigquery.SchemaField("order_line_number", "STRING"),
    bigquery.SchemaField("order_sub_sequence", "STRING"),
    bigquery.SchemaField("order_detail_sequence", "STRING"),
    bigquery.SchemaField("item_sequence", "INTEGER"),
    bigquery.SchemaField("order_sn", "STRING"),
    bigquery.SchemaField("order_date", "DATE"),
    bigquery.SchemaField("shipping_provider", "STRING"),
    bigquery.SchemaField("tracking_number", "STRING"),
    bigquery.SchemaField("delivery_type", "STRING"),
    bigquery.SchemaField("order_type", "STRING"),
    bigquery.SchemaField("actual_shipping_date", "DATE"),
    bigquery.SchemaField("order_transfer_date", "DATE"),
    bigquery.SchemaField("ship_by_date", "DATE"),
    bigquery.SchemaField("recipient_name", "STRING"),
    bigquery.SchemaField("product_manufacturer_code", "STRING"),
    bigquery.SchemaField("product_sku_main", "INTEGER"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("single_product_id", "INTEGER"),
    bigquery.SchemaField("product_variation", "STRING"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("product_cost_untaxed", "FLOAT"),
    bigquery.SchemaField("product_cost", "INTEGER"),
    bigquery.SchemaField("product_original_price", "INTEGER"),
    bigquery.SchemaField("gift", "STRING"),
    bigquery.SchemaField("key_for_merge", "STRING"),
    bigquery.SchemaField("is_abnormal_order", "BOOLEAN"),
    bigquery.SchemaField("data_source", "STRING"),
    bigquery.SchemaField("processing_date", "TIMESTAMP"),
]

# Momo 物流對帳資料表結構
# 用於儲存 Momo 平台訂單的物流對帳資料，包含出貨資訊、配送狀態、
# 發票資料、客戶資訊等物流相關資料。此資料表主要用於物流追蹤和配送分析。
a1102_momo_shipping_orders_schema = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("order_date", "DATE"),
    bigquery.SchemaField("order_sn", "STRING"),
    bigquery.SchemaField("order_sn_main", "INTEGER"),
    bigquery.SchemaField("order_line_number", "STRING"),
    bigquery.SchemaField("order_sub_sequence", "STRING"),
    bigquery.SchemaField("order_detail_sequence", "STRING"),
    bigquery.SchemaField("shipping_provider", "STRING"),
    bigquery.SchemaField("tracking_number", "STRING"),
    bigquery.SchemaField("order_type", "STRING"),
    bigquery.SchemaField("customer_delivery_request", "STRING"),
    bigquery.SchemaField("order_transfer_date", "TIMESTAMP"),
    bigquery.SchemaField("ship_by_date", "STRING"),
    bigquery.SchemaField("recipient_name", "STRING"),
    bigquery.SchemaField("product_manufacturer_code", "STRING"),
    bigquery.SchemaField("product_sku_main", "INTEGER"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("single_product_id", "STRING"),
    bigquery.SchemaField("product_variation", "STRING"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("product_cost", "INTEGER"),
    bigquery.SchemaField("product_original_price", "INTEGER"),
    bigquery.SchemaField("gift", "STRING"),
    bigquery.SchemaField("buyer_name", "STRING"),
    bigquery.SchemaField("invoice_number", "STRING"),
    bigquery.SchemaField("invoice_date", "DATE"),
    bigquery.SchemaField("customer_personal_id", "INTEGER"),
    bigquery.SchemaField("group_variable_price_product", "STRING"),
    bigquery.SchemaField("order_completion_date", "STRING"),
    bigquery.SchemaField("return_refund_status", "STRING"),
    bigquery.SchemaField("processing_date", "TIMESTAMP"),
    bigquery.SchemaField("is_abnormal_order", "BOOLEAN"),
    bigquery.SchemaField("key_for_merge", "STRING"),
]

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

# ETMall 訂單資料表結構
# 用於儲存 ETMall 平台訂單的完整資料，包含訂單基本資訊、商品詳情、
# 客戶資訊、物流狀態、商品分類等完整資料。此資料表主要用於訂單分析和營收統計。
etmall_orders_schema = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("order_date", "DATE"),
    bigquery.SchemaField("order_sn", "STRING"),
    bigquery.SchemaField("line_number", "INTEGER"),
    bigquery.SchemaField("order_line_uid", "STRING"),
    bigquery.SchemaField("merge_no", "STRING"),
    bigquery.SchemaField("shipping_sn", "STRING"),
    bigquery.SchemaField("product_sale_id", "INTEGER"),
    bigquery.SchemaField("product_id", "INTEGER"),
    bigquery.SchemaField("product_name_platform", "STRING"),
    bigquery.SchemaField("color", "STRING"),
    bigquery.SchemaField("style", "STRING"),
    bigquery.SchemaField("seller_product_sn", "STRING"),
    bigquery.SchemaField("order_type", "STRING"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("unit_price", "FLOAT"),
    bigquery.SchemaField("cost_to_platform", "FLOAT"),
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("customer_phone", "STRING"),
    bigquery.SchemaField("customer_tel", "STRING"),
    bigquery.SchemaField("shipping_address", "STRING"),
    bigquery.SchemaField("shipping_carrier", "STRING"),
    bigquery.SchemaField("shipping_code", "STRING"),
    bigquery.SchemaField("shipping_request_date", "DATE"),
    bigquery.SchemaField("shipping_expected_date", "DATE"),
    bigquery.SchemaField("shipping_expected_time", "STRING"),
    bigquery.SchemaField("note", "STRING"),
    bigquery.SchemaField("gift_info", "STRING"),
    bigquery.SchemaField("vendor_shipping_note", "STRING"),
    bigquery.SchemaField("expected_stockin_date", "DATE"),
    bigquery.SchemaField("expected_delivery_date", "DATE"),
    bigquery.SchemaField("sales_channel", "STRING"),
    bigquery.SchemaField("order_type_code", "STRING"),
    bigquery.SchemaField("company_name", "STRING"),
    bigquery.SchemaField("shop_id", "STRING"),
    bigquery.SchemaField("shop_name", "STRING"),
    bigquery.SchemaField("shop_business_model", "STRING"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("department", "STRING"),
    bigquery.SchemaField("manager", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("subcategory", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("series", "STRING"),
    bigquery.SchemaField("pet_type", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("item_code", "STRING"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("tags", "STRING"),
    bigquery.SchemaField("spec", "STRING"),
    bigquery.SchemaField("unit", "STRING"),
    bigquery.SchemaField("origin", "STRING"),
    bigquery.SchemaField("purchase_cost", "FLOAT"),
    bigquery.SchemaField("supplier_code", "STRING"),
    bigquery.SchemaField("supplier", "STRING"),
    bigquery.SchemaField("processing_date", "TIMESTAMP"),
]

# PChome 訂單資料表結構
# 用於儲存 PChome 平台訂單的完整資料，包含訂單基本資訊、商品詳情、
# 客戶資訊、物流狀態、商品規格等完整資料。此資料表主要用於訂單分析和營收統計。
pchome_orders_data_schema = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("order_sn", "STRING"),
    bigquery.SchemaField("item_seq", "STRING"),
    bigquery.SchemaField("order_date", "DATE"),
    bigquery.SchemaField("order_weekday", "INTEGER"),
    bigquery.SchemaField("order_week", "INTEGER"),
    bigquery.SchemaField("temp_layer", "INTEGER"),
    bigquery.SchemaField("is_merge_box", "BOOLEAN"),
    bigquery.SchemaField("ship_order_no", "STRING"),
    bigquery.SchemaField("confirm", "BOOLEAN"),
    bigquery.SchemaField("weight_total_kg", "FLOAT"),
    bigquery.SchemaField("weight_max_kg", "FLOAT"),
    bigquery.SchemaField("ship_date", "DATE"),
    bigquery.SchemaField("transfer_date", "DATE"),
    bigquery.SchemaField("preorder_date", "DATE"),
    bigquery.SchemaField("return_apply_date", "DATE"),
    bigquery.SchemaField("return_approve_date", "DATE"),
    bigquery.SchemaField("receiver", "STRING"),
    bigquery.SchemaField("receiver_zip", "STRING"),
    bigquery.SchemaField("receiver_addr", "STRING"),
    bigquery.SchemaField("receiver_phone", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("sku_option", "STRING"),
    bigquery.SchemaField("order_qty", "INTEGER"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("cancel_qty", "INTEGER"),
    bigquery.SchemaField("price_unit", "FLOAT"),
    bigquery.SchemaField("price_total", "FLOAT"),
    bigquery.SchemaField("product_spec", "STRING"),
    bigquery.SchemaField("vendor_no", "STRING"),
    bigquery.SchemaField("product_weight_kg", "FLOAT"),
    bigquery.SchemaField("package_len", "INTEGER"),
    bigquery.SchemaField("package_wid", "INTEGER"),
    bigquery.SchemaField("package_hei", "INTEGER"),
    bigquery.SchemaField("package_type", "STRING"),
    bigquery.SchemaField("remark", "STRING"),
    bigquery.SchemaField("processing_date", "TIMESTAMP"),
]

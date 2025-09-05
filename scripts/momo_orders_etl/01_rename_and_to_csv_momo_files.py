#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
01_rename_and_to_csv_momo_files.py

功能：
- 遞迴掃描 data_raw/momo 下 .xls/.xlsx/.csv（跳過 backup/）
- 同時支援舊/新檔名格式解析
- 轉出為新命名規則的 .csv（所有欄位以字串處理）
- 若目標 .csv 已存在：僅保留較新的版本（以修改時間判斷）
- 轉檔成功後將來源 .xls/.xlsx 依新命名規則重新命名搬到 backup/；
  若 backup 內同名已存在：僅保留較新的版本（以修改時間判斷）
"""

import re
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional

import pandas as pd
import shutil
import os

# ===== 參數設定 =====
INPUT_CSV_ENCODING = "utf-8-sig"
INPUT_CSV_SEP = ","
INPUT_CSV_KEEP_DEFAULT_NA = False
INPUT_CSV_NA_FILTER = False

OUTPUT_CSV_ENCODING = "utf-8-sig"
OUTPUT_CSV_QUOTING = csv.QUOTE_ALL
OUTPUT_CSV_LINETERMINATOR = "\n"

EXCEL_SHEET_STRATEGY = "first"  # 'first' 或 'concat'


# ===== 日誌 =====
def setup_logging(project_root: Path) -> None:
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)
    for log_file in log_dir.glob('rename_momo_files_*.log'):
        try:
            log_file.unlink()
        except OSError as e:
            print(f"無法刪除舊日誌 {log_file}: {e}")

    log_filename = f'rename_momo_files_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_path = log_dir / log_filename

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, 'w', 'utf-8'),
            logging.StreamHandler()
        ]
    )


# ===== 模組資訊 =====
def get_module_info() -> Dict[str, Dict[str, Dict[str, str] or str]]:
    return {
        'A1102': {
            'name': '未出貨訂單管理',
            'delivery_methods': {
                '1': '廠商配送',
                '2': '超商取貨',
                '3': '第三方物流',
            }
        },
        'C1105': {
            'name': '對帳訂單明細',
            'delivery_methods': {}
        }
    }


# ===== 新命名判斷 =====
def is_already_renamed(stem: str, module_info: Dict[str, Dict[str, str]]) -> bool:
    a1102_delivery_names = '|'.join(map(re.escape, module_info['A1102']['delivery_methods'].values()))
    pat_a1102 = rf'^A1102_[123]_({a1102_delivery_names})_[0-9]{{6}}_\d{{8}}_\d{{6}}$'

    c1105_name = re.escape(module_info['C1105']['name'])
    pat_c1105 = rf'^C1105_{c1105_name}_[0-9]{{6}}_\d{{8}}_\d{{6}}$'

    return bool(re.match(pat_a1102, stem) or re.match(pat_c1105, stem))


# ===== 檔名解析（舊/新格式）=====
def parse_filename(stem: str) -> Optional[Tuple[str, str, str, str, str]]:
    name = stem.strip()

    # 舊格式 A1102：A1102_<delivery>_<ignored>_<cust6>_<dt14 | date8_time6>(尾可含 (n)/(全形) 與空白)
    m = re.match(
        r'^(A1102)_(\d)_(\d+)_([0-9]{6})_(\d{14}|\d{8}_\d{6})(?:\s*[\(\（]\d+[\)\）])?$',
        name
    )
    if m:
        module_code, delivery_code, customer_code, dt = m.group(1), m.group(2), m.group(4), m.group(5)
        if '_' in dt:
            date_str, time_str = dt.split('_', 1)
        else:
            date_str, time_str = dt[:8], dt[8:]
        return module_code, delivery_code, customer_code, date_str, time_str

    # 舊格式 C1105：C1105_<cust6>_<dt14 | date8_time6>(尾可含 (n)/(全形) 與空白)
    m = re.match(
        r'^(C1105)_([0-9]{6})_(\d{14}|\d{8}_\d{6})(?:\s*[\(\（]\d+[\)\）])?$',
        name
    )
    if m:
        module_code, customer_code, dt = m.group(1), m.group(2), m.group(3)
        delivery_code = ''
        if '_' in dt:
            date_str, time_str = dt.split('_', 1)
        else:
            date_str, time_str = dt[:8], dt[8:]
        return module_code, delivery_code, customer_code, date_str, time_str

    # 新格式 A1102：A1102_<delivery>_<配送中文名>_<cust6>_<YYYYMMDD>_<HHMMSS>
    m = re.match(
        r'^(A1102)_(\d)_[^_]+_([0-9]{6})_(\d{8})_(\d{6})$',
        name
    )
    if m:
        return m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)

    # 新格式 C1105：C1105_對帳訂單明細_<cust6>_<YYYYMMDD>_<HHMMSS>
    m = re.match(
        r'^(C1105)_對帳訂單明細_([0-9]{6})_(\d{8})_(\d{6})$',
        name
    )
    if m:
        return m.group(1), '', m.group(2), m.group(3), m.group(4)

    return None


# ===== 產生新檔名（不含副檔名）=====
def generate_new_stem(module_code: str, delivery_code: str, customer_code: str,
                      date_str: str, time_str: str,
                      module_info: Dict[str, Dict[str, str]]) -> str:
    if module_code not in module_info:
        return f"{module_code}_{delivery_code}_{customer_code}_{date_str}_{time_str}"

    module_name = module_info[module_code]['name']
    if delivery_code and delivery_code in module_info[module_code].get('delivery_methods', {}):
        delivery_name = module_info[module_code]['delivery_methods'][delivery_code]
        return f"{module_code}_{delivery_code}_{delivery_name}_{customer_code}_{date_str}_{time_str}"
    else:
        return f"{module_code}_{module_name}_{customer_code}_{date_str}_{time_str}"


# ===== 讀寫（全字串）=====
def read_file_as_str_df(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in ('.xls', '.xlsx'):
        if EXCEL_SHEET_STRATEGY == 'concat':
            sheets = pd.read_excel(path, sheet_name=None, dtype=str)
            dfs = []
            for sheet_name, df in sheets.items():
                df = df.astype(str).fillna("")
                df.insert(0, "_sheet", str(sheet_name))
                dfs.append(df)
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        else:
            return pd.read_excel(path, sheet_name=0, dtype=str).astype(str).fillna("")
    elif suffix == '.csv':
        return pd.read_csv(
            path, dtype=str, sep=INPUT_CSV_SEP,
            encoding=INPUT_CSV_ENCODING,
            keep_default_na=INPUT_CSV_KEEP_DEFAULT_NA,
            na_filter=INPUT_CSV_NA_FILTER
        ).astype(str).fillna("")
    else:
        raise ValueError(f"不支援的副檔名: {suffix}")


def write_df_to_csv_all_str(df: pd.DataFrame, out_path: Path) -> None:
    df = df.copy()
    
    # 處理帳務數字欄位，確保小數點下兩位
    cost_fields = ['product_cost_untaxed', 'platform_product_cost', 'product_original_price']
    for field in cost_fields:
        if field in df.columns:
            # 轉換為數值，保留小數點下兩位
            df[field] = pd.to_numeric(df[field], errors='coerce').round(2)
    
    # 其他欄位轉為字串
    df = df.astype(str).fillna("")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        out_path,
        index=False,
        quoting=OUTPUT_CSV_QUOTING,
        encoding=OUTPUT_CSV_ENCODING,
        lineterminator=OUTPUT_CSV_LINETERMINATOR
    )


# ===== 比較並保留較新版本 =====
def keep_newer_when_conflict(src: Path, dst: Path) -> str:
    """
    目標 dst 已存在時：
    - 若 src 較新：覆寫 dst，回傳 'replaced'
    - 若 dst 較新：保留 dst，刪除/丟棄 src，回傳 'kept_dst'
    - 若 dst 不存在：回傳 'moved'（呼叫端執行 move/寫出）
    備註：依檔案 mtime 判斷新舊。
    """
    if not dst.exists():
        return 'moved'
    try:
        src_mtime = src.stat().st_mtime
    except FileNotFoundError:
        # src 可能是暫存尚未寫出（例如 DataFrame 要寫出的 CSV 還沒存在檔案系統）
        # 呼叫端應自行處理此情境
        return 'dst_exists'
    dst_mtime = dst.stat().st_mtime
    if src_mtime > dst_mtime:
        # src 較新 → 覆寫
        try:
            dst.unlink()
        except Exception:
            pass
        return 'replaced'
    else:
        return 'kept_dst'


# ===== 主流程 =====
def process_files(momo_dir: Path, module_info: Dict[str, Dict[str, str]]) -> None:
    backup_dir = momo_dir / "backup"
    backup_dir.mkdir(exist_ok=True)

    # 遞迴掃描，但跳過 backup 目錄
    targets = [
        p for p in momo_dir.rglob("*")
        if p.is_file()
        and p.suffix.lower() in ('.xls', '.xlsx', '.csv')
        and backup_dir not in p.parents
    ]

    if not targets:
        logging.warning(f"在 {momo_dir} 目錄下沒有找到任何檔案")
        return

    logging.info(f"找到 {len(targets)} 個待處理檔案")
    converted_count = skipped_count = moved_count = replaced_csv = kept_csv = replaced_backup = kept_backup = 0

    for src_path in targets:
        stem, ext = src_path.stem, src_path.suffix.lower()
        logging.info(f"處理檔案: {src_path.name}")

        # 已為新命名且為 CSV → 直接跳過
        if ext == '.csv' and is_already_renamed(stem, module_info):
            logging.info(f"已為新命名且為 CSV，略過: {src_path.name}")
            skipped_count += 1
            continue

        parsed = parse_filename(stem)
        if not parsed:
            logging.warning(f"無法解析檔名，略過: {stem}")
            skipped_count += 1
            continue

        module_code, delivery_code, customer_code, date_str, time_str = parsed
        new_stem = generate_new_stem(module_code, delivery_code, customer_code, date_str, time_str, module_info)

        # 1) 產出 CSV（保留較新版本）
        out_csv = src_path.parent / f"{new_stem}.csv"
        if out_csv.exists():
            # 比較來源與現有 CSV 的 mtime
            decision = keep_newer_when_conflict(src_path, out_csv)
            if decision == 'kept_dst':
                logging.info(f"目標 CSV 較新，保留現有：{out_csv.name}；來源略過：{src_path.name}")
                kept_csv += 1
            else:
                # 'replaced' or 'moved'（視為需要重寫）
                try:
                    df = read_file_as_str_df(src_path)
                    write_df_to_csv_all_str(df, out_csv)  # 覆寫或寫入
                    if decision == 'replaced':
                        logging.info(f"🔁 覆寫較舊 CSV：{out_csv.name}")
                        replaced_csv += 1
                    else:
                        logging.info(f"✅ 轉檔成功：{src_path.name} -> {out_csv.name}")
                        converted_count += 1
                except Exception as e:
                    logging.error(f"❌ 轉檔失敗: {src_path.name} - {e}")
                    skipped_count += 1
                    continue
        else:
            # 不存在 → 直接寫出
            try:
                df = read_file_as_str_df(src_path)
                write_df_to_csv_all_str(df, out_csv)
                logging.info(f"✅ 轉檔成功：{src_path.name} -> {out_csv.name}")
                converted_count += 1
            except Exception as e:
                logging.error(f"❌ 轉檔失敗: {src_path.name} - {e}")
                skipped_count += 1
                continue

        # 2) Excel 搬到 backup（保留較新版本）
        if ext in ('.xls', '.xlsx'):
            dst_backup = backup_dir / f"{new_stem}{ext}"
            if dst_backup.exists():
                decision = keep_newer_when_conflict(src_path, dst_backup)
                if decision == 'kept_dst':
                    # 目的較新 → 保留目的；刪除來源避免重複
                    try:
                        src_path.unlink()
                        logging.info(f"🗑️ 來源較舊，刪除來源：{src_path.name}；保留 backup：{dst_backup.name}")
                        kept_backup += 1
                    except Exception as e:
                        logging.error(f"刪除舊來源失敗：{src_path.name} - {e}")
                        skipped_count += 1
                elif decision in ('replaced', 'moved'):
                    # 目的較舊或不存在 → 用新來源覆寫/搬移
                    try:
                        dst_backup.unlink(missing_ok=True)
                    except TypeError:
                        # Python <3.8 無 missing_ok
                        if dst_backup.exists():
                            try:
                                dst_backup.unlink()
                            except Exception:
                                pass
                    try:
                        shutil.move(str(src_path), str(dst_backup))
                        if decision == 'replaced':
                            logging.info(f"🔁 覆寫較舊 backup：{dst_backup.name}")
                            replaced_backup += 1
                        else:
                            logging.info(f"📦 搬移至 backup：{dst_backup.name}")
                            moved_count += 1
                    except Exception as e:
                        logging.error(f"❌ 搬移/覆寫 backup 失敗：{src_path.name} - {e}")
                        skipped_count += 1
            else:
                # 目的檔不存在 → 直接搬移
                try:
                    shutil.move(str(src_path), str(dst_backup))
                    logging.info(f"📦 搬移至 backup：{dst_backup.name}")
                    moved_count += 1
                except Exception as e:
                    logging.error(f"❌ 搬移至 backup 失敗：{src_path.name} - {e}")
                    skipped_count += 1

    logging.info("\n=== 作業完成 ===")
    logging.info(f"成功轉出 CSV：{converted_count} 個")
    logging.info(f"覆寫較舊 CSV：{replaced_csv} 個；保留較新 CSV：{kept_csv} 個")
    logging.info(f"搬移至 backup：{moved_count} 個；覆寫 backup：{replaced_backup} 個；保留較新 backup：{kept_backup} 個")
    logging.info(f"跳過：{skipped_count} 個")


# ===== 入口 =====
def main():
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]
    setup_logging(project_root)
    logging.info("=== Momo 檔案重新命名與轉檔開始 ===")

    momo_dir = project_root / 'data_raw' / 'momo'
    if not momo_dir.exists():
        logging.error(f"找不到 momo 目錄: {momo_dir}")
        return

    module_info = get_module_info()
    process_files(momo_dir, module_info)
    logging.info("=== Momo 檔案重新命名與轉檔完成 ===")


if __name__ == '__main__':
    main()

# scripts/excel_password_remover/utils.py
# -*- coding: utf-8 -*-
"""
工具集：
- 密碼設定載入 (json/yaml)
- 路徑確保
- zip 壓縮檔自動解壓（可擴充）

輸入：
- JSON/YAML 密碼設定檔案
- ZIP 壓縮檔案

輸出：
- 解壓縮後的檔案
- 確保存在的目錄

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import os
import zipfile
import json
import yaml
from pathlib import Path
from typing import Union, Any, Dict, List

def load_passwords_json(json_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """載入帳號密碼 json 檔（建議格式為 UTF-8 編碼）"""
    path = Path(json_path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def load_passwords_yaml(yaml_path: Union[str, Path]) -> Dict[str, Any]:
    """載入帳號密碼 yaml 檔"""
    path = Path(yaml_path)
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dir(path: Union[str, Path]) -> None:
    """確保目錄存在（支援 Path 或 str）"""
    if isinstance(path, (str, Path)):
        Path(path).mkdir(parents=True, exist_ok=True)
    else:
        raise ValueError(f"無法處理型態: {type(path)}")

def extract_zip_files(zip_path: Union[str, Path], output_dir: Union[str, Path]) -> None:
    """
    解壓 zip 壓縮檔到指定目錄
    目前僅支援 zip，可依需求擴充 rar/7z
    """
    zip_path = str(zip_path)
    output_dir = str(output_dir)
    if zip_path.lower().endswith(".zip"):
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)
    else:
        raise NotImplementedError("目前僅支援 zip。如需支援 rar/7z 可引入 pyunpack、patool。")

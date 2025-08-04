# scripts/excel_password_remover/utils.py
# -*- coding: utf-8 -*-
"""
工具集：
- 密碼設定載入 (json/yaml)
- 路徑確保
- zip/rar 壓縮檔自動解壓（支援密碼）

輸入：
- JSON/YAML 密碼設定檔案
- ZIP/RAR 壓縮檔案

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
from typing import Union, Any, Dict, List, Optional
import subprocess
import shutil

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

def get_password_for_platform(platform_name: str, passwords_config: List[Dict[str, Any]]) -> Optional[str]:
    """
    根據平台名稱從密碼配置中取得對應的密碼
    
    Args:
        platform_name: 平台名稱 (如 'etmall', 'momo', 'shopee' 等)
        passwords_config: 密碼配置列表
    
    Returns:
        對應的密碼字串，如果找不到則返回 None
    """
    platform_keywords = {
        'etmall': ['東森', '東森購物', '森森'],
        'momo': ['MOMO購物中心', 'MOMO', '富邦', '富邦MOMO'],
        'shopee': ['蝦皮', 'Shopee'],
        'pchome': ['PC購物中心', 'PC', '網家'],
        'yahoo': ['Yahoo', 'Yahoo購物中心', '雅虎購物中心', '雅虎']
    }
    
    # 取得平台對應的關鍵字
    keywords = platform_keywords.get(platform_name.lower(), [])
    
    # 在密碼配置中尋找匹配的商店
    for shop in passwords_config:
        if shop.get('keywords'):
            for keyword in keywords:
                if any(kw in keyword for kw in shop['keywords']):
                    return shop.get('report_download_password')
    
    return None

def extract_zip_files(zip_path: Union[str, Path], output_dir: Union[str, Path], password: Optional[str] = None) -> bool:
    """
    解壓 zip 壓縮檔到指定目錄
    
    Args:
        zip_path: ZIP 檔案路徑
        output_dir: 輸出目錄
        password: 密碼（可選）
    
    Returns:
        解壓縮是否成功
    """
    zip_path = Path(zip_path)
    output_dir = Path(output_dir)
    
    if not zip_path.lower().endswith(".zip"):
        return False
    
    try:
        if password:
            # 使用密碼解壓縮
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(output_dir, pwd=password.encode('utf-8'))
        else:
            # 無密碼解壓縮
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(output_dir)
        return True
    except Exception as e:
        print(f"解壓縮 ZIP 檔案失敗：{zip_path} - {e}")
        return False

def extract_rar_files(rar_path: Union[str, Path], output_dir: Union[str, Path], password: Optional[str] = None) -> bool:
    """
    解壓 RAR 壓縮檔到指定目錄
    
    Args:
        rar_path: RAR 檔案路徑
        output_dir: 輸出目錄
        password: 密碼（可選）
    
    Returns:
        解壓縮是否成功
    """
    rar_path = Path(rar_path)
    output_dir = Path(output_dir)
    
    if not rar_path.lower().endswith(".rar"):
        return False
    
    try:
        # 檢查是否有 unrar 命令
        if shutil.which("unrar"):
            # 使用 unrar 命令
            cmd = ["unrar", "x", str(rar_path), str(output_dir)]
            if password:
                cmd.extend(["-p" + password])
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        else:
            # 嘗試使用 pyunpack
            try:
                from pyunpack import Archive
                archive = Archive(str(rar_path))
                if password:
                    archive.extractall(str(output_dir), password=password)
                else:
                    archive.extractall(str(output_dir))
                return True
            except ImportError:
                print("未安裝 pyunpack 套件，無法解壓 RAR 檔案")
                return False
    except Exception as e:
        print(f"解壓縮 RAR 檔案失敗：{rar_path} - {e}")
        return False

def extract_archive_files(archive_path: Union[str, Path], output_dir: Union[str, Path], 
                         platform_name: str = None, passwords_config: List[Dict[str, Any]] = None) -> bool:
    """
    解壓縮檔案（支援 ZIP 和 RAR）
    
    Args:
        archive_path: 壓縮檔案路徑
        output_dir: 輸出目錄
        platform_name: 平台名稱（用於取得密碼）
        passwords_config: 密碼配置列表
    
    Returns:
        解壓縮是否成功
    """
    archive_path = Path(archive_path)
    output_dir = Path(output_dir)
    
    # 確保輸出目錄存在
    ensure_dir(output_dir)
    
    # 取得密碼
    password = None
    if platform_name and passwords_config:
        password = get_password_for_platform(platform_name, passwords_config)
    
    # 根據檔案類型選擇解壓縮方法
    if archive_path.lower().endswith(".zip"):
        return extract_zip_files(archive_path, output_dir, password)
    elif archive_path.lower().endswith(".rar"):
        return extract_rar_files(archive_path, output_dir, password)
    else:
        print(f"不支援的檔案格式：{archive_path}")
        return False

def batch_extract_archives(source_dir: Union[str, Path], output_base_dir: Union[str, Path],
                          platform_name: str = None, passwords_config: List[Dict[str, Any]] = None) -> List[Path]:
    """
    批次解壓縮目錄中的所有壓縮檔案
    
    Args:
        source_dir: 來源目錄
        output_base_dir: 輸出基礎目錄
        platform_name: 平台名稱
        passwords_config: 密碼配置列表
    
    Returns:
        成功解壓縮的檔案路徑列表
    """
    source_dir = Path(source_dir)
    output_base_dir = Path(output_base_dir)
    
    extracted_files = []
    
    # 搜尋所有壓縮檔案
    archive_extensions = ['.zip', '.rar']
    for ext in archive_extensions:
        for archive_file in source_dir.glob(f"*{ext}"):
            # 建立對應的輸出目錄
            output_dir = output_base_dir / archive_file.stem
            ensure_dir(output_dir)
            
            # 解壓縮
            if extract_archive_files(archive_file, output_dir, platform_name, passwords_config):
                extracted_files.append(output_dir)
                print(f"成功解壓縮：{archive_file.name} -> {output_dir}")
            else:
                print(f"解壓縮失敗：{archive_file.name}")
    
    return extracted_files

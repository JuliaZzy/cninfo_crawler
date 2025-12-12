#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告信息收集器 - 从CSV文件读取PDF链接并提取数据资源信息

功能：
1. 从CSV文件中读取PDF链接
2. 逐个解析PDF，提取"存货", "无形资产", "开发支出"的数据
3. 生成长格式和宽格式的Excel报告
4. 添加"是否包含数据资产"标记列
"""

import os
import re
import requests
import pandas as pd
import pdfplumber
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime
import warnings
import logging
from pathlib import Path
import glob
import argparse
from decimal import Decimal, InvalidOperation

# 抑制pdfplumber的警告信息
warnings.filterwarnings("ignore", category=UserWarning, module="pdfplumber")
logging.getLogger("pdfplumber").setLevel(logging.ERROR)


TARGET_KEYWORD = "其中：数据资源"
PARENT_CATEGORIES = ["存货", "无形资产", "开发支出"]
SPECIAL_UNIT_MULTIPLIERS = {
    "600941.SH": (Decimal("1000000"), "百万"),
    "601727.SH": (Decimal("1000"), "千"),
}


def _normalize_text(text: str) -> str:
    """统一文本：去掉换行、空格，并将英文冒号替换为中文冒号。"""
    if text is None:
        return ""
    cleaned = str(text).replace('\n', '')
    cleaned = cleaned.replace(':', '：')
    cleaned = re.sub(r'\s+', '', cleaned)
    return cleaned


def adjust_amount_for_special_unit(sec_code: str, amount_str: str) -> str:
    """针对特殊证券代码按单位换算金额"""
    if not amount_str:
        return amount_str

    normalized_code = (sec_code or "").upper()
    config = SPECIAL_UNIT_MULTIPLIERS.get(normalized_code)
    if not config:
        return amount_str

    multiplier, unit_label = config
    cleaned_amount = (
        str(amount_str).replace(",", "").replace(" ", "").strip()
    )
    if cleaned_amount in {"", "N/A", "空值", "-", "nan", "None"}:
        return amount_str

    try:
        numeric_value = Decimal(cleaned_amount)
    except (InvalidOperation, ValueError):
        return amount_str

    adjusted_value = numeric_value * multiplier
    formatted = f"{adjusted_value:,.2f}".rstrip("0").rstrip(".")
    print(
        f"  🔄 {normalized_code} 单位为{unit_label}，已换算金额: {amount_str} -> {formatted}"
    )
    return formatted if formatted else "0"


def extract_data_by_table(pdf_content, pdf_url):
    """
    仅通过表格方式查找"其中：数据资源"。
    规则：
        1. 必须出现在同一个表格的行内；
        2. 允许目标文字及父类别文字中包含空格、全角空格、不同冒号；
        3. 取该行中“其中：数据资源”之后的第一个 >0 的数字；
        4. 父类别只可能是["存货","无形资产","开发支出"]，取上一行最近的非空值。
    
    Returns:
        tuple: (提取结果列表, 是否在表格中找到"其中：数据资源")
    """
    found_items = []
    has_data_resource_keyword = False

    def extract_number_from_text(text):
        """从文本中提取第一个有效数字"""
        if not text:
            return None, False, False
        cleaned_text = str(text).strip()
        number_patterns = [
            r'((?:\d{1,3},)*\d{1,3}\.\d{2})',
            r'((?:\d{1,3},)*\d{1,3}\.\d+)',
            r'((?:\d{1,3},)+\d+)',
            r'((?:\d{1,3},)*\d+)',
            r'(\d+\.\d{2})',
            r'(\d+\.\d+)',
            r'(\d+)',
        ]
        for pattern in number_patterns:
            match = re.search(pattern, cleaned_text)
            if match:
                value_str = match.group(1)
                try:
                    numeric_value = float(value_str.replace(',', ''))
                    return value_str, True, numeric_value > 0
                except Exception:
                    return value_str, True, True
        return None, False, False

    def find_parent_category(table, current_index):
        """向上查找父类别（上一行，允许跳过空行）"""
        parent_row_idx = current_index - 1
        while parent_row_idx >= 0:
            parent_row = table[parent_row_idx]
            if not parent_row:
                parent_row_idx -= 1
                continue
            normalized_cells = ''.join(_normalize_text(cell) for cell in parent_row if cell)
            if not normalized_cells:
                parent_row_idx -= 1
                continue
            for cat in PARENT_CATEGORIES:
                if cat in normalized_cells:
                    return cat
            # 如果上一行有文本但不是目标父类，则停止查找，避免跨越其他段落
            break
        return None

    try:
        import sys
        from io import StringIO

        old_stderr = sys.stderr
        sys.stderr = StringIO()

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                with pdfplumber.open(BytesIO(pdf_content)) as pdf:
                    for page_num, page in enumerate(pdf.pages, 1):
                        # 只要页面文本中包含"数据资源"，就标记为True，用于"是否包含数据资产"
                        page_text = page.extract_text() or ""
                        if "数据资源" in page_text:
                            has_data_resource_keyword = True

                        tables = page.extract_tables()
                        if not tables:
                            continue
                        for table in tables:
                            if not table:
                                continue
                            for row_idx, row in enumerate(table):
                                if not row:
                                    continue
                                normalized_cells = [_normalize_text(cell) for cell in row]
                                target_col_idx = None
                                for col_idx, cell_text in enumerate(normalized_cells):
                                    if cell_text and TARGET_KEYWORD in cell_text:
                                        target_col_idx = col_idx
                                        has_data_resource_keyword = True
                                        break
                                if target_col_idx is None:
                                    continue

                                # 在同一行中，寻找目标文字后的第一个有效数字
                                found_value = None
                                for col_idx in range(target_col_idx, len(row)):
                                    cell_value = row[col_idx]
                                    value, has_num, is_positive = extract_number_from_text(cell_value)
                                    if has_num and is_positive:
                                        found_value = value
                                        break
                                if not found_value:
                                    continue

                                parent_category = find_parent_category(table, row_idx)
                                if not parent_category:
                                    continue

                                found_items.append({
                                    "category": parent_category,
                                    "value": found_value,
                                    "method": "table",
                                    "page": page_num
                                })
                                print(f"    ✅ [表格] 第{page_num}页 {parent_category}其中：数据资源: {found_value}")
        finally:
            sys.stderr = old_stderr

    except Exception as e:
        print(f"    ⚠️ 表格提取方法出错: {e}")
        return [], has_data_resource_keyword

    if not found_items:
        print("    ⚠️ 表格中未找到符合条件的'其中：数据资源'。")

    return found_items, has_data_resource_keyword


def process_pdf_link(row_data, session, headers, folder_path, download_pdf=True):
    """
    处理单个PDF链接，下载并解析数据
    
    Args:
        row_data (dict): CSV行数据，包含PDF链接等信息
        session (requests.Session): 请求会话
        headers (dict): 请求头
        folder_path (str): 保存文件夹路径
        download_pdf (bool): 是否下载PDF文件到本地
    
    Returns:
        list: 解析结果列表
    """
    pdf_url = row_data.get('PDF链接', '')
    if not pdf_url:
        print(f"  ❌ 跳过：无PDF链接")
        return []
    
    sec_code = row_data.get('股票代码', row_data.get('证券代码', '未知代码'))
    sec_name = row_data.get('公司名称', '未知公司')
    report_title = row_data.get('财报名称', '未知报告')
    report_date = row_data.get('报告日期', '未知日期')
    
    # 清理并构造文件名
    report_name_base = f"{sec_name}：{report_title}_[{report_date}]"
    file_name = re.sub(r'[\\/:*?"<>|]', '_', report_name_base) + ".pdf"
    file_path = os.path.join(folder_path, file_name)

    # 检查文件是否已存在（仅在下载PDF模式下检查）
    if download_pdf and os.path.exists(file_path):
        print(f"  📄 文件已存在，直接读取: {file_name}")
        try:
            with open(file_path, 'rb') as f:
                pdf_content = f.read()
        except Exception as e:
            print(f"  ❌ 读取已存在文件失败: {e}")
            return []
    else:
        try:
            print(f"  📥 正在下载: {sec_name} - {report_title}")
            response = session.get(pdf_url, headers=headers, timeout=(15, 45))
            response.raise_for_status()
            
            # 验证是否为PDF
            if 'application/pdf' not in response.headers.get('Content-Type', ''):
                print(f"  ⚠️ 警告: {file_name} 不是PDF文件。")
                return []

            pdf_content = response.content

            # 根据用户选择决定是否保存PDF文件到本地
            if download_pdf:
                os.makedirs(folder_path, exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(pdf_content)
                print(f"  ✅ PDF已保存: {file_name}")
            else:
                print(f"  📊 仅解析数据，未保存PDF: {file_name}")
        except requests.exceptions.RequestException as e:
            print(f"  ❌ 下载失败: {e}")
            return []

    # 在内存中解析PDF内容 - 仅使用表格提取逻辑
    print(f"  🔍 使用表格提取方法...")
    extracted_data_table, has_data_resource_keyword = extract_data_by_table(pdf_content, pdf_url)
    
    all_extracted_data = extracted_data_table
    print(f"  📊 表格方法找到: {len(extracted_data_table)} 条")
    
    # 将报告自身信息添加到提取结果中
    results_for_excel = []
    if all_extracted_data:
        for item in all_extracted_data:
            adjusted_amount = adjust_amount_for_special_unit(sec_code, item['value'])
            results_for_excel.append({
                "证券代码": sec_code,
                "公司名称": sec_name,
                "报告名称": report_title,
                "报告日期": report_date,
                "项目名称": item['category'],
                "金额": adjusted_amount,
                "PDF链接": pdf_url,
                "_has_data_resource": 1 if has_data_resource_keyword else 0  # 临时字段，用于后续判断
            })
    else:
        # 即使没找到数据，也记录三条（对应三个项目），方便追溯，金额设为0
        for category in ["存货", "无形资产", "开发支出"]:
            adjusted_amount = adjust_amount_for_special_unit(sec_code, "0")
            results_for_excel.append({
                "证券代码": sec_code,
                "公司名称": sec_name,
                "报告名称": report_title,
                "报告日期": report_date,
                "项目名称": category,
                "金额": adjusted_amount,
                "PDF链接": pdf_url,
                "_has_data_resource": 1 if has_data_resource_keyword else 0  # 临时字段，用于后续判断
            })
            
    return results_for_excel


def parse_args():
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 解析后的参数
    """
    parser = argparse.ArgumentParser(
        description="报告信息收集器 - 从CSV文件读取PDF链接并提取数据资源信息"
    )
    parser.add_argument(
        "--csv-file",
        type=str,
        default=None,
        help="指定CSV文件路径（可选）。如果不指定，将自动查找最新的listed_companies_*.csv文件"
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="不下载PDF文件，仅解析数据生成Excel（快速模式）。如果未指定此参数，程序会询问是否下载"
    )
    parser.add_argument(
        "--download-pdf",
        action="store_true",
        help="下载PDF文件到本地（完整模式）。如果未指定此参数，程序会询问是否下载"
    )
    return parser.parse_args()


def find_csv_file(csv_file_path=None):
    """
    查找符合命名模式的CSV文件并解析文件名信息
    
    Args:
        csv_file_path (str, optional): 指定的CSV文件路径。如果提供，直接使用该文件；否则自动查找最新的文件
    
    Returns:
        tuple: (CSV文件路径, 解析信息字典) 或 (None, None)
    """
    # 如果指定了文件路径，直接使用
    if csv_file_path:
        if not os.path.exists(csv_file_path):
            print(f"❌ 指定的CSV文件不存在: {csv_file_path}")
            return None, None
        print(f"📄 使用指定的CSV文件: {csv_file_path}")
        latest_file = csv_file_path
    else:
        # 查找所有符合模式的CSV文件
        pattern = "listed_companies_*_*.csv"
        csv_files = glob.glob(pattern)
        
        if not csv_files:
            print("❌ 未找到符合命名模式的CSV文件（listed_companies_*_*.csv）")
            return None, None
        
        # 按文件名中的时间戳排序，返回最新的（更准确）
        def extract_timestamp_from_filename(filename):
            """从文件名中提取时间戳用于排序"""
            try:
                name_without_ext = os.path.basename(filename).replace('.csv', '')
                parts = name_without_ext.split('_')
                # 文件名格式：listed_companies_{start_date}_{end_date}_{report_type}_{timestamp}
                # timestamp 格式通常是 YYYYMMDD_HHMMSS
                if len(parts) >= 6:
                    timestamp_str = '_'.join(parts[5:])  # 获取时间戳部分
                    # 尝试解析时间戳
                    if '_' in timestamp_str:
                        date_part, time_part = timestamp_str.split('_', 1)
                        # 转换为可比较的格式
                        return f"{date_part}_{time_part}"
                # 如果无法解析，使用文件修改时间作为备选
                return str(os.path.getmtime(filename))
            except:
                return str(os.path.getmtime(filename))
        
        csv_files.sort(key=extract_timestamp_from_filename, reverse=True)
        latest_file = csv_files[0]
        
        print(f"📄 找到CSV文件: {latest_file}")
        if len(csv_files) > 1:
            print(f"   提示: 找到 {len(csv_files)} 个匹配文件，使用最新的（按文件名时间戳）: {latest_file}")
            print(f"   其他文件: {', '.join(csv_files[1:3])}..." if len(csv_files) > 3 else f"   其他文件: {', '.join(csv_files[1:])}")
    
    # 解析文件名：listed_companies_{start_date}_{end_date}_{report_type}_{timestamp}.csv
    file_name = os.path.basename(latest_file)
    # 去掉扩展名
    name_without_ext = file_name.replace('.csv', '')
    # 分割文件名
    parts = name_without_ext.split('_')
    
    if len(parts) >= 5:
        # listed_companies_{start_date}_{end_date}_{report_type}_{timestamp}
        start_date_str = parts[2]  # 例如: 20250801
        end_date_str = parts[3]    # 例如: 20250831
        report_type = parts[4]     # 例如: bndbg
        # timestamp 可能包含下划线，所以取剩余部分
        timestamp = '_'.join(parts[5:]) if len(parts) > 5 else ''
        
        file_info = {
            'start_date_str': start_date_str,
            'end_date_str': end_date_str,
            'report_type': report_type,
            'original_timestamp': timestamp
        }
        
        print(f"📋 解析文件名信息:")
        print(f"   开始日期: {start_date_str}")
        print(f"   结束日期: {end_date_str}")
        print(f"   报告类型: {report_type}")
        
        return latest_file, file_info
    else:
        print(f"⚠️ 无法解析文件名格式，使用默认命名")
        return latest_file, None


def pivot_to_wide_format(df_long):
    """
    将长格式数据转换为宽格式，并添加"是否包含数据资产"列
    去重逻辑：
    1. 同一个证券代码下的存货/无形资产/开发支出，优先取大于0的值
    2. 如果有多个值且相等，取第一个
    3. 如果有多个值且不相等，取第一个大于0的（如果都没有大于0的，取第一个）
    
    Args:
        df_long (pd.DataFrame): 长格式数据
    
    Returns:
        pd.DataFrame: 宽格式数据
    """
    print("\n正在进行数据透视操作...")
    
    # 去重逻辑：按证券代码和项目名称分组，优先选择大于0的值
    print("正在按规则去重...")
    
    def get_numeric_value(val):
        """将值转换为数值，用于比较"""
        if pd.isna(val):
            return 0
        try:
            val_str = str(val).strip().replace(',', '').replace(' ', '')
            if val_str in ['N/A', '空值', '-', 'nan', 'None', '0', '']:
                return 0
            return float(val_str)
        except:
            return 0
    
    # 按证券代码、公司名称、报告名称、报告日期、PDF链接、项目名称分组
    deduplicated_rows = []
    
    grouped = df_long.groupby(['证券代码', '公司名称', '报告名称', '报告日期', 'PDF链接', '项目名称'])
    
    for (sec_code, company, report, date, pdf_link, category), group in grouped:
        if len(group) == 1:
            # 只有一条记录，直接添加
            deduplicated_rows.append(group.iloc[0].to_dict())
        else:
            # 多条记录，按规则选择
            group = group.copy()
            group['_numeric_value'] = group['金额'].apply(get_numeric_value)
            
            # 优先选择大于0的值
            positive_rows = group[group['_numeric_value'] > 0]
            
            if len(positive_rows) > 0:
                # 如果有大于0的值，选择第一个
                selected_row = positive_rows.iloc[0]
            else:
                # 如果没有大于0的值，选择第一个
                selected_row = group.iloc[0]
            
            deduplicated_rows.append(selected_row.drop('_numeric_value').to_dict())
    
    df_long_dedup = pd.DataFrame(deduplicated_rows)
    print(f"去重前: {len(df_long)} 行，去重后: {len(df_long_dedup)} 行")
    
    # 创建金额透视表
    df_pivot = df_long_dedup.pivot_table(
        index=['证券代码', '公司名称', '报告名称', '报告日期', 'PDF链接'], 
        columns='项目名称',                           
        values='金额',                                
        aggfunc='first'                               
    ).reset_index()
    
    print("数据透视完成！")
    
    # 创建"是否包含数据资产"列
    # 新逻辑：只要PDF中有"数据资源"这个词，就设为1
    # 从原始长格式数据中获取每个PDF的标记
    has_data_col = []
    item_cols = ['存货', '无形资产', '开发支出']  # 定义项目列，用于后续数据清理
    
    # 为每个PDF链接创建一个标记字典
    pdf_has_data_resource = {}
    if '_has_data_resource' in df_long_dedup.columns:
        for pdf_link in df_long_dedup['PDF链接'].unique():
            pdf_rows = df_long_dedup[df_long_dedup['PDF链接'] == pdf_link]
            if len(pdf_rows) > 0:
                # 取第一条记录的标记（所有记录的标记应该相同）
                pdf_has_data_resource[pdf_link] = int(pdf_rows.iloc[0]['_has_data_resource'])
            else:
                pdf_has_data_resource[pdf_link] = 0
    else:
        # 如果临时字段不存在，默认都是0（不应该发生）
        for pdf_link in df_pivot['PDF链接'].unique():
            pdf_has_data_resource[pdf_link] = 0
    
    # 根据PDF链接设置"是否包含数据资产"
    for idx, row in df_pivot.iterrows():
        pdf_link = row['PDF链接']
        has_data = pdf_has_data_resource.get(pdf_link, 0)
        has_data_col.append(has_data)
    
    df_pivot['是否包含数据资产'] = has_data_col
    
    # 将所有空值、N/A等替换为0
    print("正在清理数据：将空值、N/A等替换为0...")
    for col in item_cols:
        if col in df_pivot.columns:
            df_pivot[col] = df_pivot[col].replace(['N/A', '空值', '-', 'nan', 'None', ''], '0')
            df_pivot[col] = df_pivot[col].fillna('0')
    
    # 调整列顺序：基本信息 -> 金额列 -> 是否包含数据资产 -> PDF链接
    base_cols = ['证券代码', '公司名称', '报告名称', '报告日期']
    amount_cols = [col for col in item_cols if col in df_pivot.columns]
    other_cols = ['是否包含数据资产', 'PDF链接']
    
    final_columns = base_cols + amount_cols + other_cols
    # 只保留存在的列
    final_columns = [col for col in final_columns if col in df_pivot.columns]
    
    df_final = df_pivot[final_columns]
    
    return df_final


def main():
    """
    主函数 - 从CSV读取PDF链接并提取数据
    """
    # 解析命令行参数
    args = parse_args()
    
    # 根据命令行参数决定是否下载PDF
    if args.no_download:
        download_pdf = False
        print("\n✅ 已通过命令行参数设置：仅生成Excel数据（快速模式，不下载PDF）")
    elif args.download_pdf:
        download_pdf = True
        print("\n✅ 已通过命令行参数设置：下载PDF并生成Excel（完整模式）")
    else:
        # 询问是否下载PDF
        print("\n" + "="*60)
        print("是否下载PDF文件到本地？")
        print("y - 下载PDF并生成Excel（完整模式）")
        print("n - 仅生成Excel数据（快速模式，不下载PDF）")
        print("="*60)
        
        while True:
            choice = input("请输入选择 (y/n): ").strip().lower()
            if choice == 'y':
                download_pdf = True
                print("✅ 已选择：下载PDF并生成Excel（完整模式）")
                break
            elif choice == 'n':
                download_pdf = False
                print("✅ 已选择：仅生成Excel数据（快速模式）")
                break
            else:
                print("❌ 无效选择，请输入 y 或 n")
    
    print(f"\n📁 PDF下载模式: {'开启' if download_pdf else '关闭'}")
    if not download_pdf:
        print("⚡ 快速模式：仅解析PDF内容，不保存到本地")
    else:
        print("💾 完整模式：下载并保存PDF文件到本地")
    
    # 查找CSV文件
    csv_file, file_info = find_csv_file(args.csv_file)
    if not csv_file:
        return
    
    # 读取CSV文件
    try:
        print(f"\n📖 正在读取CSV文件: {csv_file}")
        df_csv = pd.read_csv(csv_file, dtype=str)
        print(f"✅ 成功读取 {len(df_csv)} 条记录")
        
        # 检查必要的列
        required_cols = ['PDF链接']
        missing_cols = [col for col in required_cols if col not in df_csv.columns]
        if missing_cols:
            print(f"❌ CSV文件缺少必要的列: {missing_cols}")
            return
        
        # 显示列名
        print(f"📋 CSV文件包含的列: {', '.join(df_csv.columns.tolist())}")
        
    except Exception as e:
        print(f"❌ 读取CSV文件失败: {e}")
        return
    
    # 初始化
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/pdf, application/json, text/plain, */*",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice",
    }
    
    folder_path = os.path.join(os.getcwd(), "FinancialReports_Collection")
    all_results_for_excel = []
    start_time = time.time()
    
    print(f"\n🚀 开始处理 {len(df_csv)} 个PDF链接...")
    print("="*60)
    
    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_row = {
            executor.submit(process_pdf_link, row.to_dict(), session, headers, folder_path, download_pdf): idx 
            for idx, row in df_csv.iterrows()
        }
        
        completed = 0
        for future in as_completed(future_to_row):
            completed += 1
            try:
                extracted_data = future.result()
                if extracted_data:
                    all_results_for_excel.extend(extracted_data)
                print(f"📊 进度: {completed}/{len(df_csv)} ({completed/len(df_csv)*100:.1f}%)")
            except Exception as exc:
                print(f'❌ 一个任务在执行过程中出错: {exc}')
    
    # 显示统计信息
    print(f"\n🎯 处理完成统计:")
    print(f"  📊 总记录数: {len(df_csv)}")
    # 统计成功提取的数据（金额大于0的记录数）
    success_count = 0
    for r in all_results_for_excel:
        amount = r.get('金额', '0')
        if amount and str(amount) not in ['0', 'N/A', '空值', '-', 'nan', 'None', '']:
            try:
                if float(str(amount).replace(',', '')) > 0:
                    success_count += 1
            except:
                pass
    print(f"  ✅ 成功提取数据（金额>0）: {success_count}")
    
    # 生成最终的Excel报告
    print("\n===== 正在生成Excel报告... =====")
    if all_results_for_excel:
        # 生成长格式报告
        df_long = pd.DataFrame(all_results_for_excel)
        # 确保包含所有必要的列（不去重，保留所有数据）
        # 注意：必须保留_has_data_resource字段，供pivot_to_wide_format使用
        required_cols = ['证券代码', '公司名称', '报告名称', '报告日期', '项目名称', '金额', 'PDF链接']
        if '_has_data_resource' in df_long.columns:
            required_cols.append('_has_data_resource')
        available_cols = [col for col in required_cols if col in df_long.columns]
        df_long = df_long[available_cols]
        
        # 将所有空值、N/A等替换为0（长格式保留所有数据，不去重）
        print("正在清理数据：将空值、N/A等替换为0...")
        df_long['金额'] = df_long['金额'].replace(['N/A', '空值', '-', 'nan', 'None', ''], '0')
        df_long['金额'] = df_long['金额'].fillna('0')
        
        # 生成输出文件名
        # 使用当前时间作为timestamp
        output_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if file_info:
            # 从CSV文件名中提取的信息
            start_date_str = file_info['start_date_str']
            end_date_str = file_info['end_date_str']
            report_type = file_info['report_type']
            
            # 构建文件名：long_output_{start_date}_{end_date}_{report_type}_{timestamp}.xlsx
            long_output_filename = f'long_output_{start_date_str}_{end_date_str}_{report_type}_{output_timestamp}.xlsx'
            wide_output_filename = f'wide_output_{start_date_str}_{end_date_str}_{report_type}_{output_timestamp}.xlsx'
        else:
            # 如果无法解析文件名，使用默认命名
            long_output_filename = f'long_output_{output_timestamp}.xlsx'
            wide_output_filename = f'wide_output_{output_timestamp}.xlsx'
        
        # 生成长格式Excel
        if os.path.exists(long_output_filename):
            os.remove(long_output_filename)
        df_long.to_excel(long_output_filename, index=False)
        print(f"🎉 长格式报告生成完毕！已保存为 ./{long_output_filename}")
        
        # 生成宽格式报告
        print("\n正在生成宽格式报告...")
        df_wide = pivot_to_wide_format(df_long)
        
        if os.path.exists(wide_output_filename):
            os.remove(wide_output_filename)
        
        df_wide.to_excel(wide_output_filename, index=False, freeze_panes=(1, 0))
        print(f"🎉 宽格式报告生成完毕！已保存为 ./{wide_output_filename}")
        
        print("\n📊 宽格式报告预览:")
        print(df_wide.head(10))
    else:
        print("❌ 未提取到任何数据，不生成Excel文件。")
        
    end_time = time.time()
    print(f"\n⏱️ 总耗时: {(end_time - start_time):.2f} 秒")
    print("✅ 程序执行完毕")


if __name__ == "__main__":
    print("=" * 60)
    print("报告信息收集器 - 数据资源提取工具")
    print("=" * 60)
    print("功能：从CSV文件读取PDF链接并提取数据资源信息")
    print("输出：生成长格式和宽格式的Excel报告")
    print("=" * 60)
    print("使用方法：")
    print("  python report_info_collection.py                              # 自动查找最新的CSV文件，会询问是否下载PDF")
    print("  python report_info_collection.py --csv-file file.csv          # 指定CSV文件，会询问是否下载PDF")
    print("  python report_info_collection.py --no-download                 # 不下载PDF，仅解析数据（快速模式）")
    print("  python report_info_collection.py --download-pdf                # 下载PDF到本地（完整模式）")
    print("  python report_info_collection.py --csv-file file.csv --no-download  # 指定CSV文件且不下载PDF")
    print("=" * 60)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n程序执行完毕")


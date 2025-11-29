#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告信息收集器 - 从CSV文件读取PDF链接并提取数据资源信息

功能：
1. 从CSV文件中读取PDF链接
2. 逐个解析PDF，提取"存货", "无形资产", "开发支出"的数据
3. 生成长格式和宽格式的Excel报告
4. 添加"是否包含数据资产"标记列

作者：基于financial_data_crawler.py转换
日期：2025年
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

# 抑制pdfplumber的警告信息
warnings.filterwarnings("ignore", category=UserWarning, module="pdfplumber")
logging.getLogger("pdfplumber").setLevel(logging.ERROR)


def extract_data_by_category(pdf_content, pdf_url):
    """
    通过解析PDF中的表格结构来提取数据，能够精确区分列，避免误抓。
    
    Args:
        pdf_content (bytes): PDF文件的二进制内容
        pdf_url (str): PDF文件的URL（用于调试）
    
    Returns:
        list: 包含提取数据的字典列表
    """
    found_items = []
    parent_categories = ["存货", "无形资产", "开发支出"]
    # 用于去重的集合，记录已经找到的类别
    found_categories = set()
    
    def find_first_number_in_row(row, start_col=1):
        """
        在行的指定列开始位置查找第一个有效数字
        
        Args:
            row (list): 表格行数据
            start_col (int): 开始查找的列索引
            
        Returns:
            tuple: (找到的数字, 是否检测到数字)
        """
        has_number = False
        found_value = "空值"
        
        for i in range(start_col, len(row)):
            cell_value = row[i]
            if cell_value and isinstance(cell_value, str):
                # 清理单元格内容，去除空格和特殊字符
                cleaned_value = cell_value.strip().replace(' ', '')
                
                # 更宽松的数字匹配模式，包括各种格式
                number_patterns = [
                    r'((?:\d{1,3},)*\d{1,3}\.\d{2})',  # 标准格式：1,234.56
                    r'((?:\d{1,3},)*\d+)',              # 整数格式：1,234
                    r'(\d+\.\d{2})',                    # 简单小数：123.45
                    r'(\d+)',                           # 纯数字：123
                    r'(-)',                             # 负号或空值标记
                ]
                
                for pattern in number_patterns:
                    match = re.search(pattern, cleaned_value)
                    if match:
                        found_value = match.group(1)
                        has_number = True
                        break
                
                if has_number:
                    break
        
        return found_value, has_number
    
    try:
        # 临时抑制pdfplumber的警告
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with pdfplumber.open(BytesIO(pdf_content)) as pdf:
                for page in pdf.pages:
                    # 尝试提取页面上的所有表格
                    tables = page.extract_tables()
                    if not tables:
                        continue

                    for table in tables:
                        last_parent_item = None
                        # 遍历表格的每一行
                        for row in table:
                            if not row or not row[0]:  # 跳过空行或第一列为空的行
                                continue
                            
                            # 清理第一列的文本，去除换行符
                            first_col_text = row[0].replace('\n', '')

                            # 步骤1: 检查是否为父项
                            is_parent = False
                            for cat in parent_categories:
                                if cat in first_col_text:
                                    last_parent_item = cat
                                    is_parent = True
                                    break
                            if is_parent:
                                continue # 如果是父项行，继续检查下一行

                            # 步骤2: 检查是否为子项，并且我们已经找到了它的父项
                            if last_parent_item and "数据资源" in first_col_text:
                                # 去重检查：如果这个类别已经找到过，跳过
                                if last_parent_item in found_categories:
                                    continue
                                    
                                # 步骤3: 智能查找数字位置
                                found_value, has_number = find_first_number_in_row(row, start_col=1)
                                
                                if has_number:
                                    print(f"    ✅ {last_parent_item}数据资源: {found_value}")
                                else:
                                    print(f"    ⚠️ {last_parent_item}数据资源: 未检测到数字")

                                found_items.append({
                                    "category": last_parent_item,
                                    "value": found_value,
                                    "has_data": 1 if has_number and found_value != "空值" and found_value != "-" else 0
                                })
                                # 记录已找到的类别，避免重复
                                found_categories.add(last_parent_item)
                                # 重置父项，避免下一行的其他"其中"项被错误归类
                                last_parent_item = None

    except Exception as e:
        print(f"    ❌ 解析PDF表格时出错: {e}")
        return []
        
    if not found_items:
        print(f"    ⚠️ 在此PDF的任何表格中未找到'数据资源'相关条目。")
        
    return found_items


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

    # 在内存中解析PDF内容
    extracted_data = extract_data_by_category(pdf_content, pdf_url)
    
    # 将报告自身信息添加到提取结果中
    results_for_excel = []
    if extracted_data:
        for item in extracted_data:
            results_for_excel.append({
                "证券代码": sec_code,
                "公司名称": sec_name,
                "报告名称": report_title,
                "报告日期": report_date,
                "项目名称": item['category'],
                "金额": item['value'],
                "是否包含数据资产": item['has_data'],
                "PDF链接": pdf_url
            })
    else:
        # 即使没找到数据，也记录三条（对应三个项目），方便追溯
        for category in ["存货", "无形资产", "开发支出"]:
            results_for_excel.append({
                "证券代码": sec_code,
                "公司名称": sec_name,
                "报告名称": report_title,
                "报告日期": report_date,
                "项目名称": category,
                "金额": "N/A",
                "是否包含数据资产": 0,
                "PDF链接": pdf_url
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
    
    Args:
        df_long (pd.DataFrame): 长格式数据
    
    Returns:
        pd.DataFrame: 宽格式数据
    """
    print("\n正在进行数据透视操作...")
    
    # 先对数据进行去重
    print("正在去除重复数据...")
    df_long_dedup = df_long.drop_duplicates(
        subset=['公司名称', '报告名称', '报告日期', '项目名称'], 
        keep='first'
    )
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
    # 检查三个项目（存货、无形资产、开发支出）是否有数据
    item_cols = ['存货', '无形资产', '开发支出']
    has_data_col = []
    
    for idx, row in df_pivot.iterrows():
        has_data = 0
        for col in item_cols:
            if col in df_pivot.columns:
                value = row[col]
                # 检查值是否有效（不是N/A、空值、-等）
                if pd.notna(value) and str(value) not in ['N/A', '空值', '-', 'nan', 'None']:
                    # 尝试提取数字
                    value_str = str(value).replace(',', '').replace(' ', '')
                    if re.search(r'\d', value_str):
                        has_data = 1
                        break
        has_data_col.append(has_data)
    
    df_pivot['是否包含数据资产'] = has_data_col
    
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
    print(f"  ✅ 成功提取数据: {len([r for r in all_results_for_excel if r.get('金额') != 'N/A'])}")
    
    # 生成最终的Excel报告
    print("\n===== 正在生成Excel报告... =====")
    if all_results_for_excel:
        # 生成长格式报告
        df_long = pd.DataFrame(all_results_for_excel)
        df_long = df_long[['证券代码', '公司名称', '报告名称', '报告日期', '项目名称', '金额', '是否包含数据资产', 'PDF链接']]
        
        # 最终去重处理
        print("正在进行最终数据去重...")
        original_count = len(df_long)
        df_long = df_long.drop_duplicates(subset=['公司名称', '报告名称', '项目名称'], keep='first')
        final_count = len(df_long)
        print(f"去重前: {original_count} 行，去重后: {final_count} 行，去除了 {original_count - final_count} 行重复数据")
        
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


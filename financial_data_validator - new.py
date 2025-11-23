#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
财务数据验证器 - 快速验证财报PDF是否可访问
功能：
1. 快速检查财报PDF是否可以成功下载
2. 不保存PDF文件，不解析PDF内容
3. 输出包含股票代码、公司名称、财报名称的Excel表格

用途：快速验证爬取的数据是否准确，无需完整下载和解析
"""

import os
import re
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys
import argparse
from datetime import datetime, timedelta
import warnings

# 抑制警告信息
warnings.filterwarnings("ignore")


def validate_pdf_access(announcement_info, session, headers):
    """
    快速验证PDF是否可以访问（只检查响应头，不下载完整文件）
    
    Args:
        announcement_info (dict): 公告信息
        session (requests.Session): 请求会话
        headers (dict): 请求头
    
    Returns:
        dict or None: 如果PDF可访问，返回包含股票代码、公司名称、财报名称、报告日期、PDF链接的字典；否则返回None
    """
    file_url = 'https://static.cninfo.com.cn/' + announcement_info['adjunctUrl']
    sec_name = announcement_info.get('secName', '未知公司')
    sec_code = announcement_info.get('secCode', '未知代码')
    announcement_title = announcement_info.get('announcementTitle', '未知报告')
    
    # 处理证券代码：确保6位格式并添加交易所后缀
    if sec_code and sec_code != '未知代码':
        sec_code_str = str(sec_code)
        
        # 如果是数字，补齐前导零到6位
        if sec_code_str.isdigit():
            sec_code_str = sec_code_str.zfill(6)  # 补齐到6位，如 1 -> 000001
        
        # 添加交易所后缀
        if sec_code_str.startswith('60') or sec_code_str.startswith('68'):
            sec_code = sec_code_str + '.SH'  # 上交所
        elif sec_code_str.startswith('00') or sec_code_str.startswith('30'):
            sec_code = sec_code_str + '.SZ'  # 深交所
        elif sec_code_str.startswith('83') or sec_code_str.startswith('87') or sec_code_str.startswith('92'):
            sec_code = sec_code_str + '.BJ'  # 北交所
        else:
            sec_code = sec_code_str  # 保持原样
    
    # 处理时间戳，获取报告日期
    raw_time = announcement_info.get('announcementTime')
    if isinstance(raw_time, int):
        report_date = datetime.fromtimestamp(raw_time / 1000).strftime('%Y-%m-%d')
    elif isinstance(raw_time, str):
        report_date = raw_time.split(' ')[0]
    else:
        report_date = datetime.now().strftime('%Y-%m-%d')
    
    # 严格过滤：只处理2025年的报告
    if not report_date.startswith('2025'):
        return None
    
    try:
        report_year = int(report_date.split('-')[0])
        if report_year != 2025:
            return None
    except (ValueError, IndexError):
        return None
    
    # 过滤：排除包含"摘要"的报告
    if '摘要' in announcement_title:
        return None
    
    # 过滤：财报名称必须包含"2025"字样（排除旧财报的修改版）
    if '2025' not in announcement_title:
        return None
    
    # 过滤：排除英文版
    if '（英文版）' in announcement_title or '(英文版)' in announcement_title:
        return None
    
    # 过滤：排除标题中包含其他年份的报告（排除旧财报的修改版）
    # 例如：2022年、2023年、2024年的报告修改版
    other_years = ['2022', '2023', '2024', '2021', '2020']
    for year in other_years:
        if year in announcement_title:
            return None
    
    # 获取PDF链接
    pdf_url = 'https://static.cninfo.com.cn/' + announcement_info.get('adjunctUrl', '')
    
    try:
        # 使用HEAD请求快速检查文件是否存在（更快）
        # 如果HEAD不支持，则使用GET但只读取少量字节
        try:
            response = session.head(file_url, headers=headers, timeout=10, allow_redirects=True)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'application/pdf' in content_type:
                    return {
                        "股票代码": sec_code,
                        "公司名称": sec_name,
                        "财报名称": announcement_title,
                        "报告日期": report_date,
                        "PDF链接": pdf_url
                    }
        except:
            # 如果HEAD失败，尝试GET但只读取前1024字节验证
            response = session.get(file_url, headers=headers, timeout=10, stream=True)
            response.raise_for_status()
            
            # 只读取前1024字节来验证是否为PDF
            chunk = next(response.iter_content(1024), b'')
            if chunk.startswith(b'%PDF'):
                return {
                    "股票代码": sec_code,
                    "公司名称": sec_name,
                    "财报名称": announcement_title,
                    "报告日期": report_date,
                    "PDF链接": pdf_url
                }
        
        return None
        
    except requests.exceptions.RequestException:
        return None
    except Exception as e:
        return None


def get_announcements_multi_api(session, headers, exchange, date_str, report_categories, api_urls):
    """
    使用多个API接口获取公告数据，提高数据完整性
    
    Args:
        session: 请求会话
        headers: 请求头
        exchange: 交易所信息
        date_str: 日期字符串
        report_categories: 报告类别列表
        api_urls: API接口列表
    
    Returns:
        list: 公告列表
    """
    all_announcements = []
    seen_announcements = set()  # 用于去重
    
    for api_url in api_urls:
        for report_category in report_categories:
            page_num = 1
            total_pages = None  # 总页数，第一次请求后获取
            empty_pages_count = 0  # 连续空页计数
            max_empty_pages = 3  # 连续3页无有效数据就停止
            
            while True:
                try:
                    # 请求参数
                    # 处理日期：如果date_str已经是日期范围格式（包含~），直接使用；否则按天查询
                    if '~' in date_str:
                        se_date = date_str  # 已经是日期范围格式
                    else:
                        se_date = f"{date_str}~{date_str}"  # 单天查询
                    
                    post_data = {
                        "pageNum": str(page_num), 
                        "pageSize": "30", 
                        "column": exchange["column"],
                        "tabName": "fulltext", 
                        "plate": "", 
                        "stock": "", 
                        "searchkey": "",
                        "secid": "", 
                        "category": report_category, 
                        "trade": "",
                        "seDate": se_date, 
                        "sortName": "", 
                        "sortType": "",
                        "isHLtitle": "true"
                    }
                    
                    # 显示进度（每页都显示，使用flush确保实时输出）
                    if total_pages:
                        print(f"  正在查询第 {page_num}/{total_pages} 页...", end='', flush=True)
                    else:
                        print(f"  正在查询第 {page_num} 页...", end='', flush=True)
                    
                    response = session.post(api_url, headers=headers, data=post_data, timeout=20)
                    response.raise_for_status()
                    
                    # 检查响应状态
                    if response.status_code != 200:
                        print(f"\n  ⚠️ API返回状态码: {response.status_code}")
                        break
                    
                    try:
                        data = response.json()
                    except ValueError:
                        print(f"\n  ⚠️ API返回的不是JSON格式数据")
                        break
                    
                    # 获取总页数（第一次请求时）
                    if total_pages is None and isinstance(data, dict):
                        total_pages = data.get('totalpages', 0)
                        if total_pages > 0:
                            print(f"\n  共 {total_pages} 页数据")
                        else:
                            print(f"\n  无数据")
                            break
                    
                    # 检查是否超过总页数（更严格的停止条件）
                    if total_pages:
                        if page_num > total_pages + 3:
                            # 超过总页数3页后强制停止
                            print(f"\n  已超过总页数 {total_pages} 页，强制停止（当前第 {page_num} 页）")
                            break
                        elif page_num > total_pages and empty_pages_count >= 2:
                            # 超过总页数且连续2页无有效数据，立即停止
                            print(f"\n  已超过总页数 {total_pages} 且连续 {empty_pages_count} 页无有效数据，停止查询")
                            break
                    
                    # 添加最大页数限制，防止无限循环（安全措施）
                    max_pages_limit = 500
                    if page_num > max_pages_limit:
                        print(f"\n  达到最大页数限制 {max_pages_limit}，强制停止")
                        break
                    
                    # 处理不同的响应格式
                    if isinstance(data, list):
                        announcements = data
                    elif isinstance(data, dict):
                        announcements = data.get('announcements', [])
                    else:
                        announcements = []
                    
                    # 如果API返回空数据，说明已经到最后一页
                    if not announcements:
                        if page_num == 1:
                            print(f"\n  无数据")
                        else:
                            print(f"\n  查询完成（共 {page_num-1} 页）")
                        break
                    
                    # 显示当前页获取到的原始数据量
                    raw_count = len(announcements)
                    
                    # 去重处理 + 2025年过滤
                    filtered_announcements = []
                    for ann in announcements:
                        # 先检查是否为2025年的报告
                        announcement_time = ann.get('announcementTime', '')
                        if announcement_time:
                            try:
                                if isinstance(announcement_time, int):
                                    ann_date = datetime.fromtimestamp(announcement_time / 1000)
                                elif isinstance(announcement_time, str):
                                    ann_date = datetime.strptime(announcement_time.split(' ')[0], '%Y-%m-%d')
                                else:
                                    continue
                                
                                # 只保留2025年的报告
                                if ann_date.year != 2025:
                                    continue
                                    
                            except (ValueError, TypeError):
                                continue
                        
                        # 使用多个字段组合作为唯一标识
                        unique_key = (
                            ann.get('secCode', ''),
                            ann.get('announcementTitle', ''),
                            ann.get('announcementTime', ''),
                            ann.get('adjunctUrl', '')
                        )
                        
                        if unique_key not in seen_announcements:
                            seen_announcements.add(unique_key)
                            filtered_announcements.append(ann)
                    
                    # 将过滤后的数据添加到总列表
                    all_announcements.extend(filtered_announcements)
                    
                    # 显示当前页获取到的数据量（过滤后）
                    if len(filtered_announcements) < raw_count:
                        print(f" - 本页 {raw_count} 条（过滤后 {len(filtered_announcements)} 条）", end='\n')
                    else:
                        print(f" - 本页 {len(filtered_announcements)} 条", end='\n')
                    
                    # 检查过滤后是否有有效数据
                    if len(filtered_announcements) == 0:
                        empty_pages_count += 1
                        # 如果连续多页都没有有效数据，就停止（不管是否超过总页数）
                        if empty_pages_count >= max_empty_pages:
                            if total_pages and page_num > total_pages:
                                print(f"\n  连续 {empty_pages_count} 页无有效数据，且已超过总页数 {total_pages}，停止查询")
                            else:
                                print(f"\n  连续 {empty_pages_count} 页无有效数据，停止查询")
                            break
                    else:
                        empty_pages_count = 0  # 重置计数
                    
                    page_num += 1
                    time.sleep(0.3)  # 避免请求过于频繁
                    
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        print(f"  ❌ API接口返回404，可能接口已失效: {api_url}")
                    else:
                        print(f"  ⚠️ API请求HTTP错误: {e.response.status_code}")
                    break
                except requests.exceptions.RequestException as e:
                    print(f"  ⚠️ API请求失败: {e}")
                    break
                except Exception as e:
                    print(f"  ⚠️ API请求异常: {e}")
                    break
    
    return all_announcements


def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(
        description='财务数据验证器 - 快速验证财报PDF可访问性',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 爬取半年报，按天查询，所有交易所
  python financial_data_validator.py --report_type 半年报 --start_date 20250630 --end_date 20251001 --query_mode day
  
  # 爬取年报，按周查询，只查询上交所和深交所
  python financial_data_validator.py --report_type 年报 --start_date 20250101 --end_date 20250430 --query_mode week --exchanges 上交所 深交所
  
  # 爬取一季度报，全部查询（不按日期分段）
  python financial_data_validator.py --report_type 一季度 --start_date 20250401 --end_date 20250531 --query_mode all
        """
    )
    
    # 财报类型（必须）
    parser.add_argument(
        '--report_type',
        type=str,
        required=True,
        choices=['半年报', '年报', '一季度', '三季度'],
        help='财报类型：半年报、年报、一季度、三季度'
    )
    
    # 开始日期（必须）
    parser.add_argument(
        '--start_date',
        type=str,
        required=True,
        help='开始日期，格式：YYYYMMDD，如 20250630'
    )
    
    # 结束日期（必须）
    parser.add_argument(
        '--end_date',
        type=str,
        required=True,
        help='结束日期，格式：YYYYMMDD，如 20251001'
    )
    
    # 查询模式（必须）
    parser.add_argument(
        '--query_mode',
        type=str,
        required=True,
        choices=['day', 'week', 'all'],
        help='查询模式：day(按天)、week(按周)、all(全部，不分段)'
    )
    
    # 交易所（可选，默认全部）
    parser.add_argument(
        '--exchanges',
        type=str,
        nargs='+',
        default=['all'],
        choices=['all', '上交所', '深交所', '北交所', '新三板', '科创板'],
        help='交易所列表，默认all（全部）。可选：上交所、深交所、北交所、新三板、科创板'
    )
    
    return parser.parse_args()


def get_report_category(report_type):
    """
    根据财报类型返回API的category参数
    
    Args:
        report_type: 财报类型（半年报、年报、一季度、三季度）
    
    Returns:
        str: category参数值
    """
    category_map = {
        '半年报': 'category_bndbg_szsh',
        '年报': 'category_ndbg_szsh',
        '一季度': 'category_yjdbg_szsh',
        '三季度': 'category_sjdbg_szsh'
    }
    return category_map.get(report_type, 'category_bndbg_szsh')


def get_exchanges_list(exchanges_arg):
    """
    根据参数返回交易所列表
    
    Args:
        exchanges_arg: 用户指定的交易所列表
    
    Returns:
        list: 交易所列表
    """
    all_exchanges = [
        {"name": "上交所", "column": "sse"},
        {"name": "深交所", "column": "szse"},
        {"name": "北交所", "column": "bj"},
        {"name": "新三板", "column": "neeq"},
        {"name": "科创板", "column": "star"}
    ]
    
    if 'all' in exchanges_arg or len(exchanges_arg) == 0:
        return all_exchanges
    
    # 根据用户指定的交易所名称过滤
    name_to_column = {ex['name']: ex for ex in all_exchanges}
    selected = [name_to_column[name] for name in exchanges_arg if name in name_to_column]
    
    return selected if selected else all_exchanges


def generate_date_list(start_date, end_date, query_mode):
    """
    根据查询模式生成日期列表
    
    Args:
        start_date: 开始日期（datetime对象）
        end_date: 结束日期（datetime对象）
        query_mode: 查询模式（day/week/all）
    
    Returns:
        list: 日期字符串列表或日期范围列表
    """
    if query_mode == 'day':
        # 按天：每天一个日期字符串
        return [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
                for i in range((end_date - start_date).days + 1)]
    
    elif query_mode == 'week':
        # 按周：每周一个日期范围
        date_ranges = []
        current_date = start_date
        while current_date <= end_date:
            week_end = min(current_date + timedelta(days=6), end_date)
            date_ranges.append(
                f"{current_date.strftime('%Y-%m-%d')}~{week_end.strftime('%Y-%m-%d')}"
            )
            current_date = week_end + timedelta(days=1)
        return date_ranges
    
    else:  # query_mode == 'all'
        # 全部：一个日期范围
        return [f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}"]


def main():
    """
    主函数 - 快速验证财报PDF可访问性
    """
    # 解析命令行参数
    args = parse_arguments()
    
    # 解析日期
    try:
        start_date = datetime.strptime(args.start_date, '%Y%m%d')
        end_date = datetime.strptime(args.end_date, '%Y%m%d')
    except ValueError:
        print("错误：日期格式不正确，请使用 YYYYMMDD 格式，如 20250630")
        sys.exit(1)
    
    if start_date > end_date:
        print("错误：开始日期不能晚于结束日期")
        sys.exit(1)
    
    # 获取财报类型对应的category
    report_category = get_report_category(args.report_type)
    
    # 获取交易所列表
    exchanges = get_exchanges_list(args.exchanges)
    
    # 生成日期列表
    date_list = generate_date_list(start_date, end_date, args.query_mode)
    
    print("=" * 60)
    print("财务数据验证器 - 快速验证财报PDF可访问性")
    print("=" * 60)
    print(f"财报类型: {args.report_type}")
    print(f"时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    print(f"查询模式: {args.query_mode} ({'按天' if args.query_mode == 'day' else '按周' if args.query_mode == 'week' else '全部'})")
    print(f"交易所: {', '.join([ex['name'] for ex in exchanges])}")
    print(f"过滤规则: 财报名称必须含'2025'，排除'摘要'和'英文版'")
    print("=" * 60)
    
    session = requests.Session()
    api_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'

    # 请求头配置
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # 报告类别
    report_categories = [report_category]
    
    # 使用有效的API接口
    api_urls = [
        'http://www.cninfo.com.cn/new/hisAnnouncement/query'  # 主要API接口
    ]

    all_valid_reports = []
    start_time = time.time()
    
    # 统计信息
    total_announcements = 0
    total_valid = 0
    
    # 遍历每个交易所
    for exchange in exchanges:
        print(f"\n{'='*50}")
        print(f"开始验证 {exchange['name']} ({exchange['column']})")
        print(f"{'='*50}")
        
        exchange_announcements = 0
        exchange_valid = 0
        
        # 遍历每个日期/日期范围
        for idx, date_item in enumerate(date_list, 1):
            # 根据查询模式处理日期
            if args.query_mode == 'day':
                # 按天：date_item是日期字符串
                date_str = date_item
                date_display = date_str
            else:
                # 按周或全部：date_item是日期范围字符串
                date_str = date_item
                date_display = date_item
            
            # 显示进度
            if len(date_list) > 1:
                print(f"  处理 {idx}/{len(date_list)}: {date_display}")
            else:
                print(f"  查询日期范围: {date_display}")
            
            # 使用多API接口获取公告数据
            announcements = get_announcements_multi_api(
                session, headers, exchange, date_str, report_categories, api_urls
            )
            
            if not announcements:
                print(f"  未获取到公告数据\n")
                continue
            
            exchange_announcements += len(announcements)
            print(f"  获取到 {len(announcements)} 个公告，正在验证PDF可访问性...")
            
            # 使用线程池并发验证（提高速度）
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_info = {
                    executor.submit(validate_pdf_access, ann, session, headers): ann 
                    for ann in announcements
                }
                for future in as_completed(future_to_info):
                    try:
                        result = future.result()
                        if result:
                            all_valid_reports.append(result)
                            exchange_valid += 1
                    except Exception as exc:
                        pass
            
            if len(announcements) > 0:
                print(f"  验证完成，{exchange_valid} 个PDF可访问")
        
        # 交易所统计
        print(f"\n{exchange['name']} 统计:")
        print(f"  总公告数: {exchange_announcements}")
        print(f"  可访问PDF: {exchange_valid}")
        
        total_announcements += exchange_announcements
        total_valid += exchange_valid
    
    # 显示总体统计信息
    print(f"\n验证结果统计:")
    print(f"  总公告数: {total_announcements}")
    print(f"  可访问PDF: {total_valid}")
    print(f"  可访问率: {(total_valid/total_announcements*100):.1f}%" if total_announcements > 0 else "  可访问率: 0%")
    
    # 生成Excel报告
    print("\n===== 正在生成Excel报告... =====")
    if all_valid_reports:
        df = pd.DataFrame(all_valid_reports)
        
        # 去重处理
        print("正在进行数据去重...")
        original_count = len(df)
        df = df.drop_duplicates(subset=['股票代码', '公司名称', '财报名称', '报告日期'], keep='first')
        final_count = len(df)
        print(f"去重前: {original_count} 行，去重后: {final_count} 行")
        
        # 确保列顺序：股票代码、公司名称、财报名称、报告日期、PDF链接
        df = df[['股票代码', '公司名称', '财报名称', '报告日期', 'PDF链接']]
        
        output_filename = f'可访问财报清单_{args.report_type}_{args.start_date}_{args.end_date}.xlsx'
        # 如果文件已存在，先删除
        if os.path.exists(output_filename):
            os.remove(output_filename)
            print(f"已删除旧的 {output_filename} 文件")
        
        df.to_excel(output_filename, index=False)
        print(f"Excel报告生成完毕！已保存为 ./{output_filename}")
        print(f"共 {len(df)} 条记录")
    else:
        print("未找到可访问的PDF，不生成Excel文件。")
        
    end_time = time.time()
    print(f"\n总耗时: {(end_time - start_time):.2f} 秒")
    print("程序执行完毕")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()



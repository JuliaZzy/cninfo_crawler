# 财务数据爬虫

从巨潮资讯网爬取上市公司财务报告中的"数据资源"信息，并生成Excel报告。

## 功能特点

- 🚀 自动爬取指定日期范围内的财务报告PDF链接
- 📊 智能解析PDF中的"数据资源"相关数据
- 📈 生成长格式和宽格式的Excel报告
- 🔄 支持多线程并发处理，提高效率
- 📁 可选择下载PDF文件到本地或仅解析数据
- ⏭️ 跳过已存在的文件，避免重复下载

## 安装依赖

```bash
pip install -r requirements.txt
```

## 工作流程

项目采用两步工作流程：

### 第一步：爬取财报链接（cninfo_report_crawler.py）

从巨潮资讯网爬取指定日期范围内的财报，生成包含PDF链接的CSV文件。

```bash
python cninfo_report_crawler.py --start-date 2025-07-01 --end-date 2025-09-30 --report-type bndbg
```

**参数说明：**
- `--start-date`: 开始日期，格式：YYYY-MM-DD
- `--end-date`: 结束日期，格式：YYYY-MM-DD
- `--report-type`: 报告类型
  - `yjdbg`: 一季度
  - `bndbg`: 半年报
  - `sjdbg`: 三季度
  - `ndbg`: 年报

**输出：** `listed_companies_{start_date}_{end_date}_{report_type}_{timestamp}.csv`

### 第二步：解析PDF提取数据（report_info_collection.py）

从CSV文件读取PDF链接，解析PDF提取"数据资源"信息，生成Excel报告。

```bash
# 自动查找最新的CSV文件，会询问是否下载PDF
python report_info_collection.py

# 指定CSV文件，不下载PDF（快速模式）
python report_info_collection.py --csv-file file.csv --no-download

# 指定CSV文件，下载PDF到本地（完整模式）
python report_info_collection.py --csv-file file.csv --download-pdf
```

**参数说明：**
- `--csv-file`: 指定CSV文件路径（可选，不指定则自动查找最新的）
- `--no-download`: 不下载PDF，仅解析数据（快速模式）
- `--download-pdf`: 下载PDF到本地（完整模式）

**输出：**
- `long_output_{start_date}_{end_date}_{report_type}_{timestamp}.xlsx` - 长格式数据
- `wide_output_{start_date}_{end_date}_{report_type}_{timestamp}.xlsx` - 宽格式数据

## 输出文件

### CSV文件（第一步输出）
包含以下列：
- 股票代码
- 公司名称
- 财报名称
- 报告日期
- PDF链接

### Excel文件（第二步输出）

#### 长格式数据列
- 证券代码
- 公司名称
- 报告名称
- 报告日期
- 项目名称（存货/无形资产/开发支出）
- 金额
- 是否包含数据资产
- new_data_asset（新增：其中：数据资源）
- PDF链接

#### 宽格式数据列
- 证券代码
- 公司名称
- 报告名称
- 报告日期
- 存货
- 无形资产
- 开发支出
- 存货_new_data_asset
- 无形资产_new_data_asset
- 开发支出_new_data_asset
- 是否包含数据资产
- PDF链接

## 注意事项

1. 第一步会生成CSV文件，包含所有可访问的PDF链接
2. 第二步可以选择是否下载PDF文件：
   - 快速模式（`--no-download`）：仅解析PDF内容，不保存到本地，速度更快
   - 完整模式（`--download-pdf`）：下载并保存PDF文件到本地
3. 程序使用多线程处理，请确保网络连接稳定
4. 建议在非高峰时段运行，避免对服务器造成压力

## 错误处理

- 网络错误会自动重试
- PDF解析错误会记录并继续处理
- 程序会显示详细的进度信息
- 支持断点续传（进度保存）

## 系统要求

- Python 3.7+
- 稳定的网络连接
- 足够的磁盘空间存储PDF文件（如果选择下载模式）

## 许可证

本项目仅供学习和研究使用。

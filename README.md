# CSV BigData Tools

[English](#english) | [中文](#中文)

---

<a name="english"></a>
## English

Tools for processing large CSV and JSON files efficiently.

### Features

- **Convert**: Extract fields from large JSON files and convert to CSV
- **Split**: Split large CSV/JSON/text files into smaller manageable chunks
- **Merge**: Combine multiple CSV files into a single file
- **Sort**: Sort large CSV files using Dask or Apache Spark

### Installation

#### Basic Installation

```bash
pip install -r requirements.txt
```

#### With Optional Dependencies

For Dask-based sorting (recommended for very large files):
```bash
pip install -r requirements.txt dask[complete] psutil
```

For Spark-based sorting:
```bash
pip install -r requirements.txt pyspark
```

For all features:
```bash
pip install -r requirements.txt dask[complete] psutil pyspark
```

#### Install as Package

```bash
pip install -e .
```

This will install the package and command-line tools globally.

### Usage

#### JSON to CSV Converter

Extract specific fields from large JSON files:

```bash
python csv_bigdata_tools/converters/json_to_csv.py -i input.json -o output.csv
```

Options:
- `-i, --input`: Input JSON file path
- `-o, --output`: Output CSV file path
- `--field1`: First field name to extract (default: qq)
- `--field2`: Second field name to extract (default: phone)
- `--source-key`: Source key containing data (default: _source)
- `--no-source-key`: Data is at root level
- `-v, --verbose`: Enable verbose logging

#### Split Files

**Split CSV files:**
```bash
python csv_bigdata_tools/split/split_csv.py -i input.csv -o ./output -s 200000
```

**Split JSON files (array format):**
```bash
python csv_bigdata_tools/split/split_json.py -i input.json -o ./output -s 200000
```

**Split line-delimited JSON files:**
```bash
python csv_bigdata_tools/split/split_json_line_delimited.py -i input.jsonl -o ./output -s 200000
```

**Split text files:**
```bash
python csv_bigdata_tools/split/split_txt.py -i input.txt -o ./output -s 200000
```

Options:
- `-i, --input`: Input file path
- `-o, --output`: Output directory path
- `-s, --chunk-size`: Number of rows/lines per chunk (default: 200000)
- `-v, --verbose`: Enable verbose logging

#### Merge CSV Files

Combine multiple CSV files into one:

```bash
python csv_bigdata_tools/merge/merge_csv.py -i ./input_dir -o merged.csv
```

Options:
- `-i, --input-dir`: Directory containing CSV files to merge
- `-o, --output-file`: Output merged CSV file path
- `--pattern`: File pattern to match (default: output_*.csv)
- `-v, --verbose`: Enable verbose logging

#### Sort Large CSV Files

**Using Dask (recommended for most use cases):**
```bash
python csv_bigdata_tools/sorters/sort_csv_dask.py -i input.csv -o sorted.csv --sort-column id
```

**Using Apache Spark (for extremely large files):**
```bash
python csv_bigdata_tools/sorters/sort_csv_spark.py -i input.csv -o sorted.csv --sort-column id
```

Options:
- `-i, --input`: Input CSV file path
- `-o, --output`: Output CSV file path
- `--sort-column`: Column name to sort by (default: qq_number)
- `--max-memory`: Maximum memory in GB (Dask only, default: 30)
- `--workers`: Number of workers (Dask only, default: 10)
- `--driver-memory`: Spark driver memory (Spark only, default: 30g)
- `--executor-memory`: Spark executor memory (Spark only, default: 30g)
- `-v, --verbose`: Enable verbose logging

### Project Structure

```
csv_bigdata_tools/
├── __init__.py
├── converters/
│   ├── __init__.py
│   └── json_to_csv.py
├── split/
│   ├── __init__.py
│   ├── split_csv.py
│   ├── split_json.py
│   ├── split_json_line_delimited.py
│   └── split_txt.py
├── merge/
│   ├── __init__.py
│   └── merge_csv.py
└── sorters/
    ├── __init__.py
    ├── sort_csv_dask.py
    └── sort_csv_spark.py
```

### Requirements

- Python 3.7+
- pandas >= 1.3.0
- tqdm >= 4.60.0
- dask[complete] >= 2021.0.0 (optional, for Dask sorting)
- psutil >= 5.8.0 (optional, for Dask sorting)
- pyspark >= 3.0.0 (optional, for Spark sorting)

### License

Apache License 2.0

---

<a name="中文"></a>
## 中文

用于高效处理大型 CSV 和 JSON 文件的工具集。

### 功能特性

- **转换**: 从大型 JSON 文件中提取字段并转换为 CSV
- **拆分**: 将大型 CSV/JSON/文本文件拆分为更小的可管理块
- **合并**: 将多个 CSV 文件合并为单个文件
- **排序**: 使用 Dask 或 Apache Spark 对大型 CSV 文件进行排序

### 安装

#### 基础安装

```bash
pip install -r requirements.txt
```

#### 安装可选依赖

使用 Dask 进行排序（推荐用于超大文件）：
```bash
pip install -r requirements.txt dask[complete] psutil
```

使用 Spark 进行排序：
```bash
pip install -r requirements.txt pyspark
```

安装所有功能：
```bash
pip install -r requirements.txt dask[complete] psutil pyspark
```

#### 安装为软件包

```bash
pip install -e .
```

这将全局安装软件包和命令行工具。

### 使用方法

#### JSON 转 CSV 转换器

从大型 JSON 文件中提取特定字段：

```bash
python csv_bigdata_tools/converters/json_to_csv.py -i input.json -o output.csv
```

选项：
- `-i, --input`: 输入 JSON 文件路径
- `-o, --output`: 输出 CSV 文件路径
- `--field1`: 要提取的第一个字段名（默认: qq）
- `--field2`: 要提取的第二个字段名（默认: phone）
- `--source-key`: 包含数据的源键（默认: _source）
- `--no-source-key`: 数据在根级别
- `-v, --verbose`: 启用详细日志

#### 拆分文件

**拆分 CSV 文件：**
```bash
python csv_bigdata_tools/split/split_csv.py -i input.csv -o ./output -s 200000
```

**拆分 JSON 文件（数组格式）：**
```bash
python csv_bigdata_tools/split/split_json.py -i input.json -o ./output -s 200000
```

**拆分行分隔的 JSON 文件：**
```bash
python csv_bigdata_tools/split/split_json_line_delimited.py -i input.jsonl -o ./output -s 200000
```

**拆分文本文件：**
```bash
python csv_bigdata_tools/split/split_txt.py -i input.txt -o ./output -s 200000
```

选项：
- `-i, --input`: 输入文件路径
- `-o, --output`: 输出目录路径
- `-s, --chunk-size`: 每块的行数/项数（默认: 200000）
- `-v, --verbose`: 启用详细日志

#### 合并 CSV 文件

将多个 CSV 文件合并为一个：

```bash
python csv_bigdata_tools/merge/merge_csv.py -i ./input_dir -o merged.csv
```

选项：
- `-i, --input-dir`: 包含要合并的 CSV 文件的目录
- `-o, --output-file`: 输出合并后的 CSV 文件路径
- `--pattern`: 要匹配的文件模式（默认: output_*.csv）
- `-v, --verbose`: 启用详细日志

#### 排序大型 CSV 文件

**使用 Dask（大多数情况推荐）：**
```bash
python csv_bigdata_tools/sorters/sort_csv_dask.py -i input.csv -o sorted.csv --sort-column id
```

**使用 Apache Spark（用于超大文件）：**
```bash
python csv_bigdata_tools/sorters/sort_csv_spark.py -i input.csv -o sorted.csv --sort-column id
```

选项：
- `-i, --input`: 输入 CSV 文件路径
- `-o, --output`: 输出 CSV 文件路径
- `--sort-column`: 要排序的列名（默认: qq_number）
- `--max-memory`: 最大内存（GB）（仅 Dask，默认: 30）
- `--workers`: 工作进程数（仅 Dask，默认: 10）
- `--driver-memory`: Spark 驱动内存（仅 Spark，默认: 30g）
- `--executor-memory`: Spark 执行器内存（仅 Spark，默认: 30g）
- `-v, --verbose`: 启用详细日志

### 项目结构

```
csv_bigdata_tools/
├── __init__.py
├── converters/
│   ├── __init__.py
│   └── json_to_csv.py
├── split/
│   ├── __init__.py
│   ├── split_csv.py
│   ├── split_json.py
│   ├── split_json_line_delimited.py
│   └── split_txt.py
├── merge/
│   ├── __init__.py
│   └── merge_csv.py
└── sorters/
    ├── __init__.py
    ├── sort_csv_dask.py
    └── sort_csv_spark.py
```

### 系统要求

- Python 3.7+
- pandas >= 1.3.0
- tqdm >= 4.60.0
- dask[complete] >= 2021.0.0（可选，用于 Dask 排序）
- psutil >= 5.8.0（可选，用于 Dask 排序）
- pyspark >= 3.0.0（可选，用于 Spark 排序）

### 许可证

Apache License 2.0

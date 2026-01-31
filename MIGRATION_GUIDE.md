# Migration Guide / 迁移指南

[English](#english) | [中文](#中文)

---

<a name="english"></a>
## English

This guide helps you migrate from the old script structure to the new refactored package structure.

## What Changed?

### Directory Structure

**Old Structure:**
```
CSV-BigData-Tools/
├── big-json-to-csv/
│   ├── big-json-to-csv.py
│   ├── AscendingSort.py
│   └── AscendingSort-spark.py
├── merge/
│   └── merge_csv.py
└── split/
    ├── split_csv.py
    ├── split_json.py
    ├── split_json-adh.py
    ├── split_txt.py
    └── split_txt-ultra.py
```

**New Structure:**
```
CSV-BigData-Tools/
├── csv_bigdata_tools/
│   ├── converters/
│   │   └── json_to_csv.py
│   ├── sorters/
│   │   ├── sort_csv_dask.py
│   │   └── sort_csv_spark.py
│   ├── merge/
│   │   └── merge_csv.py
│   └── split/
│       ├── split_csv.py
│       ├── split_json.py
│       ├── split_json_line_delimited.py
│       └── split_txt.py
└── [old files remain for backward compatibility]
```

## Migration Steps

### 1. Update Dependencies

```bash
pip install -r requirements.txt
```

For optional features:
```bash
# For Dask sorting
pip install dask[complete] psutil

# For Spark sorting
pip install pyspark
```

### 2. Update Your Scripts

#### JSON to CSV Conversion

**Old Way (with hardcoded paths):**
```python
# Edit file to change paths
file_path = "H:\\Data\\input.json"
output_file = "H:\\Data\\output.csv"
# Run script
python big-json-to-csv/big-json-to-csv.py
```

**New Way (command-line arguments):**
```bash
python csv_bigdata_tools/converters/json_to_csv.py \
    -i /path/to/input.json \
    -o /path/to/output.csv
```

**With Custom Fields:**
```bash
python csv_bigdata_tools/converters/json_to_csv.py \
    -i input.json \
    -o output.csv \
    --field1 username \
    --field2 email \
    --no-source-key
```

#### CSV Splitting

**Old Way:**
```bash
# Limited options, default paths
python split/split_csv.py
```

**New Way:**
```bash
python csv_bigdata_tools/split/split_csv.py \
    -i input.csv \
    -o ./output_dir \
    -s 500000
```

#### JSON Splitting

**Old Way:**
```bash
# Two different versions: split_json.py and split_json-adh.py
python split/split_json.py -i input.json -o output/
python split/split_json-adh.py -i input.jsonl -o output/
```

**New Way:**
```bash
# For JSON arrays
python csv_bigdata_tools/split/split_json.py \
    -i input.json \
    -o ./output

# For line-delimited JSON (JSONL)
python csv_bigdata_tools/split/split_json_line_delimited.py \
    -i input.jsonl \
    -o ./output
```

#### Text File Splitting

**Old Way:**
```bash
# Two versions: split_txt.py and split_txt-ultra.py
python split/split_txt.py
```

**New Way:**
```bash
# Single efficient implementation
python csv_bigdata_tools/split/split_txt.py \
    -i input.txt \
    -o ./output \
    -s 1000000
```

#### CSV Merging

**Old Way:**
```bash
python merge/merge_csv.py -i ./data -o merged.csv
```

**New Way:**
```bash
python csv_bigdata_tools/merge/merge_csv.py \
    -i ./data \
    -o merged.csv \
    --pattern "output_*.csv"
```

#### CSV Sorting

**Old Way (with hardcoded paths):**
```python
# Edit file to change paths
input_file = r"H:\Data\input.csv"
output_file = r"H:\Data\sorted.csv"
# Run script
python big-json-to-csv/AscendingSort.py
```

**New Way - Dask:**
```bash
python csv_bigdata_tools/sorters/sort_csv_dask.py \
    -i input.csv \
    -o sorted.csv \
    --sort-column id \
    --max-memory 64 \
    --workers 16
```

**New Way - Spark:**
```bash
python csv_bigdata_tools/sorters/sort_csv_spark.py \
    -i input.csv \
    -o sorted.csv \
    --sort-column id \
    --driver-memory 32g \
    --executor-memory 32g
```

### 3. Install as Package (Optional)

For easier access, install the package:

```bash
pip install -e .
```

Then use commands directly:
```bash
json-to-csv -i input.json -o output.csv
csv-split -i input.csv -o ./output
json-split -i input.json -o ./output
jsonl-split -i input.jsonl -o ./output
txt-split -i input.txt -o ./output
csv-merge -i ./data -o merged.csv
csv-sort-dask -i input.csv -o sorted.csv
csv-sort-spark -i input.csv -o sorted.csv
```

### 4. Use as Python Library

**New Feature:** You can now import and use functions in your own scripts:

```python
from csv_bigdata_tools.split import split_csv, split_json
from csv_bigdata_tools.merge import merge_csv_files
from csv_bigdata_tools.converters import extract_json_to_csv

# Split a CSV file
split_csv('large_file.csv', './output', chunk_size=100000)

# Merge CSV files
merge_csv_files('./input_dir', 'merged.csv')

# Convert JSON to CSV
extract_json_to_csv('input.json', 'output.csv')
```

## Key Improvements

### 1. No More Hardcoded Paths
- All paths are now command-line arguments
- Works on Windows, Linux, and macOS
- No need to edit source files

### 2. Better Error Messages
- Clear error messages when files are missing
- Helpful suggestions for fixing issues
- Verbose mode for debugging

### 3. Progress Tracking
- Visual progress bars with tqdm
- Better status updates
- Estimated time remaining

### 4. Professional Logging
- Structured logging with levels (INFO, WARNING, ERROR)
- Option to enable verbose debug logging
- No more mixed print statements

### 5. Configurable Options
- All parameters are configurable
- Sensible defaults
- Flexible field extraction for JSON

### 6. Resource Management
- Proper file handle management
- Memory monitoring for large files
- Graceful error handling

### 7. Documentation
- Comprehensive help text
- Usage examples
- Bilingual documentation

## Backward Compatibility

The original files remain in their original locations, so existing workflows will continue to work. However, we recommend migrating to the new structure for:

- Better maintainability
- More features and options
- Improved error handling
- Professional logging
- Cross-platform compatibility

## Need Help?

- Check the [README.md](README.md) for detailed usage examples
- See [CODE_REVIEW.md](CODE_REVIEW.md) for technical details
- Run any script with `--help` for options
- Enable verbose mode with `-v` for debugging

---

<a name="中文"></a>
## 中文

本指南帮助您从旧的脚本结构迁移到新的重构包结构。

## 有什么变化？

### 目录结构

**旧结构：**
```
CSV-BigData-Tools/
├── big-json-to-csv/
│   ├── big-json-to-csv.py
│   ├── AscendingSort.py
│   └── AscendingSort-spark.py
├── merge/
│   └── merge_csv.py
└── split/
    ├── split_csv.py
    ├── split_json.py
    ├── split_json-adh.py
    ├── split_txt.py
    └── split_txt-ultra.py
```

**新结构：**
```
CSV-BigData-Tools/
├── csv_bigdata_tools/
│   ├── converters/
│   │   └── json_to_csv.py
│   ├── sorters/
│   │   ├── sort_csv_dask.py
│   │   └── sort_csv_spark.py
│   ├── merge/
│   │   └── merge_csv.py
│   └── split/
│       ├── split_csv.py
│       ├── split_json.py
│       ├── split_json_line_delimited.py
│       └── split_txt.py
└── [旧文件保留以保持向后兼容性]
```

## 迁移步骤

### 1. 更新依赖

```bash
pip install -r requirements.txt
```

可选功能：
```bash
# 用于 Dask 排序
pip install dask[complete] psutil

# 用于 Spark 排序
pip install pyspark
```

### 2. 更新脚本使用方式

#### JSON 转 CSV

**旧方式（硬编码路径）：**
```python
# 编辑文件更改路径
file_path = "H:\\Data\\input.json"
output_file = "H:\\Data\\output.csv"
# 运行脚本
python big-json-to-csv/big-json-to-csv.py
```

**新方式（命令行参数）：**
```bash
python csv_bigdata_tools/converters/json_to_csv.py \
    -i /path/to/input.json \
    -o /path/to/output.csv
```

**自定义字段：**
```bash
python csv_bigdata_tools/converters/json_to_csv.py \
    -i input.json \
    -o output.csv \
    --field1 username \
    --field2 email \
    --no-source-key
```

#### CSV 拆分

**旧方式：**
```bash
# 选项有限，默认路径
python split/split_csv.py
```

**新方式：**
```bash
python csv_bigdata_tools/split/split_csv.py \
    -i input.csv \
    -o ./output_dir \
    -s 500000
```

#### JSON 拆分

**旧方式：**
```bash
# 两个不同版本：split_json.py 和 split_json-adh.py
python split/split_json.py -i input.json -o output/
python split/split_json-adh.py -i input.jsonl -o output/
```

**新方式：**
```bash
# 用于 JSON 数组
python csv_bigdata_tools/split/split_json.py \
    -i input.json \
    -o ./output

# 用于行分隔的 JSON (JSONL)
python csv_bigdata_tools/split/split_json_line_delimited.py \
    -i input.jsonl \
    -o ./output
```

#### 文本文件拆分

**旧方式：**
```bash
# 两个版本：split_txt.py 和 split_txt-ultra.py
python split/split_txt.py
```

**新方式：**
```bash
# 单一高效实现
python csv_bigdata_tools/split/split_txt.py \
    -i input.txt \
    -o ./output \
    -s 1000000
```

#### CSV 合并

**旧方式：**
```bash
python merge/merge_csv.py -i ./data -o merged.csv
```

**新方式：**
```bash
python csv_bigdata_tools/merge/merge_csv.py \
    -i ./data \
    -o merged.csv \
    --pattern "output_*.csv"
```

#### CSV 排序

**旧方式（硬编码路径）：**
```python
# 编辑文件更改路径
input_file = r"H:\Data\input.csv"
output_file = r"H:\Data\sorted.csv"
# 运行脚本
python big-json-to-csv/AscendingSort.py
```

**新方式 - Dask：**
```bash
python csv_bigdata_tools/sorters/sort_csv_dask.py \
    -i input.csv \
    -o sorted.csv \
    --sort-column id \
    --max-memory 64 \
    --workers 16
```

**新方式 - Spark：**
```bash
python csv_bigdata_tools/sorters/sort_csv_spark.py \
    -i input.csv \
    -o sorted.csv \
    --sort-column id \
    --driver-memory 32g \
    --executor-memory 32g
```

### 3. 安装为软件包（可选）

为了更方便的访问，安装软件包：

```bash
pip install -e .
```

然后直接使用命令：
```bash
json-to-csv -i input.json -o output.csv
csv-split -i input.csv -o ./output
json-split -i input.json -o ./output
jsonl-split -i input.jsonl -o ./output
txt-split -i input.txt -o ./output
csv-merge -i ./data -o merged.csv
csv-sort-dask -i input.csv -o sorted.csv
csv-sort-spark -i input.csv -o sorted.csv
```

### 4. 作为 Python 库使用

**新功能：** 现在可以在自己的脚本中导入和使用函数：

```python
from csv_bigdata_tools.split import split_csv, split_json
from csv_bigdata_tools.merge import merge_csv_files
from csv_bigdata_tools.converters import extract_json_to_csv

# 拆分 CSV 文件
split_csv('large_file.csv', './output', chunk_size=100000)

# 合并 CSV 文件
merge_csv_files('./input_dir', 'merged.csv')

# 转换 JSON 到 CSV
extract_json_to_csv('input.json', 'output.csv')
```

## 主要改进

### 1. 不再有硬编码路径
- 所有路径现在都是命令行参数
- 支持 Windows、Linux 和 macOS
- 无需编辑源文件

### 2. 更好的错误消息
- 文件缺失时有清晰的错误消息
- 提供修复问题的建议
- 调试的详细模式

### 3. 进度跟踪
- 使用 tqdm 的可视化进度条
- 更好的状态更新
- 预计剩余时间

### 4. 专业的日志记录
- 带级别的结构化日志（INFO、WARNING、ERROR）
- 可选择启用详细调试日志
- 不再混用 print 语句

### 5. 可配置选项
- 所有参数都可配置
- 合理的默认值
- JSON 的灵活字段提取

### 6. 资源管理
- 正确的文件句柄管理
- 大文件的内存监控
- 优雅的错误处理

### 7. 文档
- 全面的帮助文本
- 使用示例
- 双语文档

## 向后兼容性

原始文件保留在原位置，因此现有工作流将继续工作。但是，我们建议迁移到新结构以获得：

- 更好的可维护性
- 更多功能和选项
- 改进的错误处理
- 专业的日志记录
- 跨平台兼容性

## 需要帮助？

- 查看 [README.md](README.md) 获取详细使用示例
- 查看 [CODE_REVIEW.md](CODE_REVIEW.md) 获取技术细节
- 使用 `--help` 运行任何脚本查看选项
- 使用 `-v` 启用详细模式进行调试

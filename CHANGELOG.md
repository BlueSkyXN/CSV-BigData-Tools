# Changelog / 更新日志

All notable changes to this project will be documented in this file.

本文件记录项目的所有重要更改。

## [1.0.0] - 2026-01-31

### Added / 新增

#### Package Structure / 包结构
- Created proper Python package `csv_bigdata_tools/` with module organization
- Added `__init__.py` files for all modules
- Added `setup.py` for package installation with console script entry points
- Package can now be installed with `pip install -e .`

#### New Features / 新功能
- Command-line argument support for all scripts (no more hardcoded paths)
- Verbose logging mode (`-v` flag) for debugging
- Progress tracking with tqdm for all operations
- Professional structured logging (INFO, WARNING, ERROR levels)
- Input validation and helpful error messages
- Support for path expansion with `~` for home directory
- Configurable options for all parameters

#### Documentation / 文档
- Comprehensive bilingual README (English & Chinese)
- Detailed CODE_REVIEW.md with analysis of improvements
- MIGRATION_GUIDE.md to help users transition from old structure
- CHANGELOG.md to track changes
- Docstrings for all modules and functions
- Usage examples for every tool
- Help text for all command-line tools

#### Scripts / 脚本

**Converters / 转换器:**
- `json_to_csv.py` - Extract fields from JSON and convert to CSV
  - Configurable field names
  - Support for nested JSON with source keys
  - Thread-safe progress tracking
  - Flexible field extraction

**Splitters / 拆分工具:**
- `split_csv.py` - Split large CSV files into chunks
- `split_json.py` - Split JSON array files
- `split_json_line_delimited.py` - Split line-delimited JSON (JSONL) files
- `split_txt.py` - Split large text files line by line
  - All with progress tracking
  - Configurable chunk sizes
  - File size reporting

**Merge / 合并:**
- `merge_csv.py` - Merge multiple CSV files
  - Configurable file patterns
  - Row count reporting

**Sorters / 排序:**
- `sort_csv_dask.py` - Sort large CSV files using Dask
  - Memory monitoring
  - Configurable workers and memory limits
  - Temporary file management
- `sort_csv_spark.py` - Sort large CSV files using Spark
  - Configurable driver and executor memory
  - Distributed processing

### Changed / 更改

#### Naming Conventions / 命名规范
- Renamed `big-json-to-csv/` to `csv_bigdata_tools/converters/`
- Renamed `big-json-to-csv.py` to `json_to_csv.py`
- Renamed `AscendingSort.py` to `sort_csv_dask.py`
- Renamed `AscendingSort-spark.py` to `sort_csv_spark.py`
- Renamed `split_json-adh.py` to `split_json_line_delimited.py`
- Consolidated `split_txt.py` and `split_txt-ultra.py` into single efficient implementation
- All filenames now use consistent snake_case naming

#### Code Quality / 代码质量
- Removed all hardcoded Windows-specific paths
- Replaced print statements with proper logging
- Added proper error handling and input validation
- Fixed resource management (file handles with context managers)
- Added docstrings following Google style guide
- Improved code organization and separation of concerns
- Added constants for magic numbers
- Added type hints where appropriate

#### Dependencies / 依赖
- Updated `requirements.txt` with version specifications
- Removed unused dependencies (ijson, vaex)
- Removed built-in module from requirements (argparse)
- Organized optional dependencies (Dask, Spark)
- Added tqdm for progress tracking

### Fixed / 修复

#### Resource Management / 资源管理
- Fixed unclosed file handles in `split_txt.py`
- Added try-finally blocks for proper cleanup
- Used context managers for file operations where possible
- Added graceful shutdown for threading

#### Error Handling / 错误处理
- Silent failures now log errors instead
- Added informative error messages
- Added file existence checks
- Added directory validation
- Proper exception types and handling

#### Cross-Platform / 跨平台
- Removed Windows-specific path separators
- Using pathlib for path operations
- Support for Unix and Windows paths
- Path expansion for user home directory

### Removed / 移除

- Removed hardcoded file paths from all scripts
- Removed Windows-specific path handling
- Removed duplicate/redundant script versions
- Removed unused import statements
- Removed magic numbers without explanation

### Security / 安全

- No hardcoded credentials or sensitive data
- Input validation to prevent path traversal
- Safe file operations
- No arbitrary code execution
- Passed CodeQL security analysis with 0 alerts

### Performance / 性能

- Streaming processing for large files
- Chunk-based reading to manage memory
- Proper garbage collection
- Memory monitoring in sort functions
- Efficient file I/O with proper buffering

### Backward Compatibility / 向后兼容

- Original files remain in place for backward compatibility
- Existing workflows continue to work
- New structure is opt-in
- Migration guide provided

## [Pre-1.0.0] - Before 2026-01-31

### Original Structure / 原始结构

- Basic scripts with hardcoded paths
- Windows-specific implementations
- No package structure
- Limited documentation
- Mixed Chinese and English comments
- No error handling
- Print-based output

---

## Migration Notes / 迁移说明

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for detailed migration instructions.

查看 [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) 获取详细的迁移说明。

## Future Plans / 未来计划

- Add unit tests and integration tests
- Set up CI/CD pipeline
- Add support for more data formats (Parquet, Avro)
- Add data validation features
- Add data transformation utilities
- Performance benchmarking
- Docker containerization
- Web API interface

---

[1.0.0]: https://github.com/BlueSkyXN/CSV-BigData-Tools/releases/tag/v1.0.0

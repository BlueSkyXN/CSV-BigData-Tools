#!/usr/bin/env python3
"""
Setup script for CSV BigData Tools package.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding='utf-8') if readme_file.exists() else ""

setup(
    name="csv-bigdata-tools",
    version="1.0.0",
    author="BlueSkyXN",
    description="Tools for processing large CSV and JSON files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BlueSkyXN/CSV-BigData-Tools",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.3.0",
        "tqdm>=4.60.0",
    ],
    extras_require={
        "dask": [
            "dask[complete]>=2021.0.0",
            "psutil>=5.8.0",
        ],
        "spark": [
            "pyspark>=3.0.0",
        ],
        "all": [
            "dask[complete]>=2021.0.0",
            "psutil>=5.8.0",
            "pyspark>=3.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "csv-to-json=csv_bigdata_tools.converters.json_to_csv:main",
            "csv-split=csv_bigdata_tools.split.split_csv:main",
            "json-split=csv_bigdata_tools.split.split_json:main",
            "jsonl-split=csv_bigdata_tools.split.split_json_line_delimited:main",
            "txt-split=csv_bigdata_tools.split.split_txt:main",
            "csv-merge=csv_bigdata_tools.merge.merge_csv:main",
            "csv-sort-dask=csv_bigdata_tools.sorters.sort_csv_dask:main",
            "csv-sort-spark=csv_bigdata_tools.sorters.sort_csv_spark:main",
        ],
    },
)

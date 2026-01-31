"""
Split module for splitting large files into smaller chunks.
"""

from .split_csv import split_csv
from .split_json import split_json
from .split_json_line_delimited import split_json_line_delimited
from .split_txt import split_text

__all__ = ['split_csv', 'split_json', 'split_json_line_delimited', 'split_text']

#!/usr/bin/env python3
"""
Merge multiple CSV files into a single file.

This module provides functionality to merge multiple CSV files with the same
structure into a single consolidated CSV file.
"""

import os
import argparse
import sys
import logging
from pathlib import Path

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def merge_csv_files(input_dir, output_file, file_pattern='output_*.csv'):
    """
    Merge multiple CSV files from a directory into a single file.
    
    Args:
        input_dir (str): Path to directory containing CSV files
        output_file (str): Path to output merged CSV file
        file_pattern (str): Pattern to match CSV files (default: 'output_*.csv')
    
    Returns:
        int: Number of files merged
    
    Raises:
        FileNotFoundError: If input directory doesn't exist
        ValueError: If no matching CSV files are found
    """
    input_path = Path(input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    
    if not input_path.is_dir():
        raise ValueError(f"Input path is not a directory: {input_dir}")
    
    # Find all matching CSV files
    files = list(input_path.glob(file_pattern))
    
    if not files:
        raise ValueError(
            f"No CSV files matching pattern '{file_pattern}' found in {input_dir}"
        )
    
    logger.info(f"Found {len(files)} CSV files to merge")
    
    # Read and merge all CSV files
    dfs = []
    for file in files:
        logger.debug(f"Reading file: {file}")
        df = pd.read_csv(file)
        dfs.append(df)
    
    # Concatenate all dataframes
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # Write merged data to output file
    merged_df.to_csv(output_file, index=False)
    
    logger.info(f"Successfully merged {len(dfs)} CSV files into {output_file}")
    logger.info(f"Total rows in merged file: {len(merged_df)}")
    
    return len(dfs)


def main():
    """Command-line interface for merging CSV files."""
    parser = argparse.ArgumentParser(
        description='Merge multiple CSV files into a single file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i ./data -o merged.csv
  %(prog)s -i ./data -o merged.csv --pattern "chunk_*.csv"
        """
    )
    
    parser.add_argument(
        '-i', '--input-dir',
        type=str,
        required=True,
        help='Input directory containing CSV files'
    )
    
    parser.add_argument(
        '-o', '--output-file',
        type=str,
        required=True,
        help='Output file path for merged CSV'
    )
    
    parser.add_argument(
        '--pattern',
        type=str,
        default='output_*.csv',
        help='File pattern to match (default: output_*.csv)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        merge_csv_files(args.input_dir, args.output_file, args.pattern)
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

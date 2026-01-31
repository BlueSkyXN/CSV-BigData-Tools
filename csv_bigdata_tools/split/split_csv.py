#!/usr/bin/env python3
"""
Split large CSV files into smaller chunks.

This module provides functionality to split large CSV files into smaller,
more manageable chunks with progress tracking.
"""

import os
import argparse
import sys
import logging
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def split_csv(input_file, output_dir, chunk_size=200000):
    """
    Split a large CSV file into smaller chunks.
    
    Args:
        input_file (str): Path to input CSV file
        output_dir (str): Path to output directory for split files
        chunk_size (int): Number of rows per chunk (default: 200000)
    
    Returns:
        list: List of (filename, size_in_bytes) tuples for generated files
    
    Raises:
        FileNotFoundError: If input file doesn't exist
    """
    input_path = Path(input_file).expanduser()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    output_path = Path(output_dir).expanduser()
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Splitting {input_file} into chunks of {chunk_size} rows")
    
    # Read CSV file in chunks
    df_reader = pd.read_csv(input_file, chunksize=chunk_size)
    
    # Process each chunk
    file_info = []
    for i, chunk in enumerate(tqdm(df_reader, desc="Splitting")):
        output_file = output_path / f"output_{i}.csv"
        chunk.to_csv(output_file, index=False)
        
        file_size = output_file.stat().st_size
        file_info.append((str(output_file), file_size))
        logger.debug(f"Created {output_file} ({file_size / (1024 * 1024):.2f} MB)")
    
    # Print summary
    logger.info("\nGenerated files:")
    for filename, size in file_info:
        size_mb = size / (1024 * 1024)
        logger.info(f"  {filename} ({size_mb:.2f} MB)")
    
    total_size_mb = sum(size for _, size in file_info) / (1024 * 1024)
    logger.info(f"\nTotal: {len(file_info)} files, {total_size_mb:.2f} MB")
    
    return file_info


def main():
    """Command-line interface for splitting CSV files."""
    parser = argparse.ArgumentParser(
        description='Split large CSV files into smaller chunks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i input.csv -o ./output
  %(prog)s -i ~/data/large.csv -o ~/data/chunks -s 500000
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        type=str,
        required=True,
        help='Path to input CSV file'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        required=True,
        help='Path to output directory'
    )
    
    parser.add_argument(
        '-s', '--chunk-size',
        type=int,
        default=200000,
        help='Number of rows per chunk (default: 200000)'
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
        split_csv(args.input, args.output, args.chunk_size)
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

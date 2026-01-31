#!/usr/bin/env python3
"""
Sort large CSV files using Dask for distributed computing.

This module provides functionality to sort very large CSV files that don't fit
in memory by using Dask's distributed computing capabilities.
"""

import os
import gc
import argparse
import sys
import logging
from pathlib import Path

import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import psutil

try:
    from dask.distributed import Client
except ImportError:
    Client = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration constants
CHUNKS_BEFORE_WRITE = 10  # Number of chunks to process before writing to temp file
DEFAULT_PARTITIONS = 500  # Default number of partitions for Dask processing


def sort_csv_with_dask(input_file, output_file, sort_column='qq_number',
                       chunksize=10**8, max_memory_gb=30, n_workers=10,
                       threads_per_worker=4, memory_limit='4GB'):
    """
    Sort a large CSV file using Dask for distributed processing.
    
    This function reads large CSV files in chunks, sorts them, and merges
    the results using Dask's distributed computing capabilities.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
        sort_column (str): Name of column to sort by (default: 'qq_number')
        chunksize (int): Number of rows per chunk (default: 100,000,000)
        max_memory_gb (int): Maximum memory usage in GB (default: 30)
        n_workers (int): Number of Dask workers (default: 10)
        threads_per_worker (int): Threads per worker (default: 4)
        memory_limit (str): Memory limit per worker (default: '4GB')
    
    Returns:
        bool: True if successful
    
    Raises:
        FileNotFoundError: If input file doesn't exist
        ValueError: If Dask distributed is not available when needed
    """
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Initialize progress bar
    pbar = ProgressBar()
    pbar.register()
    
    # Initialize Dask client if available
    client = None
    if Client:
        try:
            client = Client(
                n_workers=n_workers,
                threads_per_worker=threads_per_worker,
                memory_limit=memory_limit
            )
            logger.info(f"Dask client initialized: {client}")
        except Exception as e:
            logger.warning(f"Could not initialize Dask client: {e}")
    
    temp_files = []
    concatenated_chunk = pd.DataFrame()
    max_memory_bytes = max_memory_gb * 1024**3
    
    try:
        # Read large file and process in chunks
        reader = pd.read_csv(
            input_file,
            chunksize=chunksize,
            low_memory=False
        )
        
        for i, chunk in enumerate(reader):
            logger.info(f"Processing chunk {i}")
            
            # Pad the sort column for proper sorting (handles numeric sorting)
            chunk['_padded_sort_column'] = chunk[sort_column].apply(
                lambda x: str(x).zfill(15)
            )
            sorted_chunk = chunk.sort_values(by='_padded_sort_column')
            sorted_chunk.drop(columns=['_padded_sort_column'], inplace=True)
            
            # Merge multiple chunks before writing to temp file
            if i % CHUNKS_BEFORE_WRITE == 0 and not concatenated_chunk.empty:
                temp_file = f'temp_sorted_chunk_{i // CHUNKS_BEFORE_WRITE}.csv'
                concatenated_chunk.to_csv(temp_file, index=False, header=False, mode='a')
                temp_files.append(temp_file)
                concatenated_chunk = pd.DataFrame()
                gc.collect()
            else:
                concatenated_chunk = pd.concat([concatenated_chunk, sorted_chunk])
            
            # Monitor memory usage
            process = psutil.Process(os.getpid())
            memory_usage = process.memory_info().rss
            
            # Write to temp file if memory usage is high
            if memory_usage > max_memory_bytes * 0.9:
                logger.warning(
                    f"Memory usage high: {memory_usage / (1024 ** 3):.2f} GB. "
                    "Writing to temp file and clearing memory."
                )
                temp_file = f'temp_sorted_chunk_mem_cleanup_{i}.csv'
                concatenated_chunk.to_csv(temp_file, index=False, header=False, mode='a')
                temp_files.append(temp_file)
                concatenated_chunk = pd.DataFrame()
                gc.collect()
        
        # Process remaining data
        if not concatenated_chunk.empty:
            temp_file = 'temp_sorted_chunk_final.csv'
            concatenated_chunk.to_csv(temp_file, index=False, header=False, mode='a')
            temp_files.append(temp_file)
        
        # Use Dask to merge all sorted chunks
        logger.info("Merging sorted chunks with Dask...")
        ddf = dd.read_csv(temp_files, header=None)
        
        # Get column names from original file
        sample_df = pd.read_csv(input_file, nrows=1)
        column_names = list(sample_df.columns)
        ddf.columns = column_names
        
        # Increase partitions for better parallelism
        ddf = ddf.repartition(npartitions=DEFAULT_PARTITIONS)
        ddf = ddf.map_partitions(lambda df: df.sort_values(by=sort_column))
        
        # Write final result
        ddf.to_csv(output_file, index=False, single_file=True)
        logger.info(f"Sorted file written to: {output_file}")
        
        return True
        
    finally:
        # Cleanup
        if client:
            client.close()
        
        # Delete temporary files
        for temp_file in temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    logger.debug(f"Removed temp file: {temp_file}")
            except Exception as e:
                logger.warning(f"Could not remove temp file {temp_file}: {e}")


def main():
    """Command-line interface for CSV sorting with Dask."""
    parser = argparse.ArgumentParser(
        description='Sort large CSV files using Dask distributed computing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i input.csv -o output.csv
  %(prog)s -i input.csv -o output.csv --sort-column user_id
  %(prog)s -i input.csv -o output.csv --max-memory 64
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
        help='Path to output CSV file'
    )
    
    parser.add_argument(
        '--sort-column',
        type=str,
        default='qq_number',
        help='Column name to sort by (default: qq_number)'
    )
    
    parser.add_argument(
        '--chunksize',
        type=int,
        default=10**8,
        help='Number of rows per chunk (default: 100000000)'
    )
    
    parser.add_argument(
        '--max-memory',
        type=int,
        default=30,
        help='Maximum memory usage in GB (default: 30)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of Dask workers (default: 10)'
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
        sort_csv_with_dask(
            args.input,
            args.output,
            args.sort_column,
            args.chunksize,
            args.max_memory,
            args.workers
        )
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

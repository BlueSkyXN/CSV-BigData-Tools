#!/usr/bin/env python3
"""
Sort large CSV files using Apache Spark.

This module provides functionality to sort very large CSV files using
Apache Spark's distributed computing capabilities.
"""

import argparse
import sys
import logging
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def sort_csv_with_spark(input_file, output_file, sort_column='qq_number',
                        driver_memory='30g', executor_memory='30g',
                        shuffle_partitions=200):
    """
    Sort a large CSV file using Apache Spark.
    
    This function uses Apache Spark to sort large CSV files that don't fit
    in memory by distributing the processing across multiple nodes.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
        sort_column (str): Name of column to sort by (default: 'qq_number')
        driver_memory (str): Spark driver memory (default: '30g')
        executor_memory (str): Spark executor memory (default: '30g')
        shuffle_partitions (int): Number of shuffle partitions (default: 200)
    
    Returns:
        bool: True if successful
    
    Raises:
        ImportError: If PySpark is not installed
        FileNotFoundError: If input file doesn't exist
    """
    if not SPARK_AVAILABLE:
        raise ImportError(
            "PySpark is not installed. Please install it with: pip install pyspark"
        )
    
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Sort Large CSV") \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()
    
    try:
        logger.info(f"Reading CSV file: {input_file}")
        
        # Read CSV file
        df = spark.read.csv(input_file, header=True, inferSchema=True)
        
        logger.info(f"Sorting by column: {sort_column}")
        
        # Pad the sort column for proper numeric sorting
        sorted_df = df.withColumn(
            "_padded_sort_column",
            col(sort_column).cast("string").lpad(15, '0')
        ).orderBy("_padded_sort_column").drop("_padded_sort_column")
        
        # Persist the sorted data to reduce memory usage
        sorted_df = sorted_df.persist()
        
        logger.info(f"Writing sorted data to: {output_file}")
        
        # Write sorted data to output file (coalesce to single file)
        sorted_df.coalesce(1).write.csv(output_file, header=True, mode='overwrite')
        
        logger.info("Sorting complete!")
        return True
        
    finally:
        # Stop SparkSession
        spark.stop()


def main():
    """Command-line interface for CSV sorting with Spark."""
    parser = argparse.ArgumentParser(
        description='Sort large CSV files using Apache Spark',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i input.csv -o output.csv
  %(prog)s -i input.csv -o output.csv --sort-column user_id
  %(prog)s -i input.csv -o output.csv --driver-memory 64g --executor-memory 64g
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
        '--driver-memory',
        type=str,
        default='30g',
        help='Spark driver memory (default: 30g)'
    )
    
    parser.add_argument(
        '--executor-memory',
        type=str,
        default='30g',
        help='Spark executor memory (default: 30g)'
    )
    
    parser.add_argument(
        '--shuffle-partitions',
        type=int,
        default=200,
        help='Number of shuffle partitions (default: 200)'
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
        sort_csv_with_spark(
            args.input,
            args.output,
            args.sort_column,
            args.driver_memory,
            args.executor_memory,
            args.shuffle_partitions
        )
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

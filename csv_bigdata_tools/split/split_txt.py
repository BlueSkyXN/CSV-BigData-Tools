#!/usr/bin/env python3
"""
Split large text files into smaller chunks.

This module provides functionality to split large text files line by line
into smaller, more manageable chunks with progress tracking.
"""

import os
import argparse
import sys
import logging
from pathlib import Path
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def split_text(input_file, output_dir, chunk_size=200000):
    """
    Split a large text file into smaller chunks.
    
    Args:
        input_file (str): Path to input text file
        output_dir (str): Path to output directory for split files
        chunk_size (int): Number of lines per chunk (default: 200000)
    
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
    
    logger.info(f"Splitting {input_file} into chunks of {chunk_size} lines")
    
    file_info = []
    current_chunk = 0
    line_count = 0
    # Note: Manual file handle management is used here instead of context managers
    # because we need to keep the output file open across multiple loop iterations
    # and close/reopen it at chunk boundaries during streaming processing
    f_out = None
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f_in:
            for line in tqdm(f_in, desc="Processing lines"):
                # Open new output file if needed
                if line_count % chunk_size == 0:
                    # Close previous file if open
                    if f_out is not None:
                        f_out.close()
                        file_size = output_file.stat().st_size
                        file_info.append((str(output_file), file_size))
                        logger.debug(
                            f"Created {output_file} ({file_size / (1024 * 1024):.2f} MB)"
                        )
                    
                    # Open new file
                    output_file = output_path / f"output_{current_chunk}.txt"
                    f_out = open(output_file, 'w', encoding='utf-8')
                    current_chunk += 1
                
                # Write line to current output file
                f_out.write(line)
                line_count += 1
        
        # Close the last file
        if f_out is not None:
            f_out.close()
            file_size = output_file.stat().st_size
            file_info.append((str(output_file), file_size))
            logger.debug(
                f"Created {output_file} ({file_size / (1024 * 1024):.2f} MB)"
            )
    
    finally:
        # Ensure file is closed in case of error
        if f_out is not None and not f_out.closed:
            f_out.close()
    
    # Print summary
    logger.info("\nGenerated files:")
    for filename, size in file_info:
        size_mb = size / (1024 * 1024)
        logger.info(f"  {filename} ({size_mb:.2f} MB)")
    
    total_size_mb = sum(size for _, size in file_info) / (1024 * 1024)
    logger.info(f"\nTotal: {len(file_info)} files, {total_size_mb:.2f} MB")
    logger.info(f"Processed {line_count} lines")
    
    return file_info


def main():
    """Command-line interface for splitting text files."""
    parser = argparse.ArgumentParser(
        description='Split large text files into smaller chunks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i input.txt -o ./output
  %(prog)s -i ~/data/large.txt -o ~/data/chunks -s 500000
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        type=str,
        required=True,
        help='Path to input text file'
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
        help='Number of lines per chunk (default: 200000)'
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
        split_text(args.input, args.output, args.chunk_size)
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

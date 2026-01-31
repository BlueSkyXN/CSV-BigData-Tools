#!/usr/bin/env python3
"""
Extract specific fields from large JSON files and convert to CSV format.

This module provides functionality to extract QQ numbers and phone numbers
from large JSON files with progress tracking.
"""

import json
import threading
import time
import argparse
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProgressTracker:
    """Thread-safe progress tracker for counting processed lines."""
    
    def __init__(self):
        """Initialize the progress tracker."""
        self.line_count = 0
        self.lock = threading.Lock()
        self._stop_event = threading.Event()
    
    def increment(self):
        """Increment the line count safely."""
        with self.lock:
            self.line_count += 1
    
    def get_count(self):
        """Get the current line count safely."""
        with self.lock:
            return self.line_count
    
    def print_progress(self):
        """Print progress updates in a separate thread."""
        while not self._stop_event.is_set():
            count = self.get_count()
            logger.info(f"Lines processed: {count}")
            time.sleep(1)
    
    def stop(self):
        """Stop the progress tracking thread."""
        self._stop_event.set()


def extract_fields_from_json(json_data, field1='qq', field2='phone'):
    """
    Extract specified fields from a JSON object.
    
    Args:
        json_data (dict): The JSON object to extract from
        field1 (str): Name of the first field to extract (default: 'qq')
        field2 (str): Name of the second field to extract (default: 'phone')
    
    Returns:
        tuple: A tuple of (field1_value, field2_value) or None if fields are missing
    """
    value1 = json_data.get(field1)
    value2 = json_data.get(field2)
    
    if value1 and value2:
        return value1, value2
    return None


def extract_json_to_csv(input_file, output_file, field1='qq', field2='phone', 
                        source_key='_source'):
    """
    Process a large JSON file and extract specific fields to CSV format.
    
    This function reads a JSON file line by line, extracts specified fields,
    and writes them to a CSV file with progress tracking.
    
    Args:
        input_file (str): Path to the input JSON file
        output_file (str): Path to the output CSV file
        field1 (str): Name of the first field to extract (default: 'qq')
        field2 (str): Name of the second field to extract (default: 'phone')
        source_key (str): Key containing the data object (default: '_source')
    
    Returns:
        int: Number of lines successfully processed
    
    Raises:
        FileNotFoundError: If input file doesn't exist
        IOError: If there's an error reading/writing files
    """
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Initialize progress tracker
    tracker = ProgressTracker()
    progress_thread = threading.Thread(target=tracker.print_progress, daemon=True)
    progress_thread.start()
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            # Write CSV header
            outfile.write(f'{field1},{field2}\n')
            
            for line in infile:
                try:
                    # Handle JSON objects that may end with commas
                    json_data = json.loads(line.strip(',\n'))
                    
                    # Extract data from source key if specified
                    if source_key and source_key in json_data:
                        data = json_data[source_key]
                    else:
                        data = json_data
                    
                    result = extract_fields_from_json(data, field1, field2)
                    if result:
                        outfile.write(f'{result[0]},{result[1]}\n')
                    
                    tracker.increment()
                    
                except (json.JSONDecodeError, KeyError) as e:
                    logger.debug(f"Skipping invalid line: {e}")
                    continue
        
        final_count = tracker.get_count()
        logger.info(f"Processing complete! Total lines processed: {final_count}")
        return final_count
        
    finally:
        tracker.stop()
        progress_thread.join(timeout=2)


def main():
    """Command-line interface for the JSON to CSV converter."""
    parser = argparse.ArgumentParser(
        description='Extract fields from large JSON files and convert to CSV format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i input.json -o output.csv
  %(prog)s -i input.json -o output.csv --field1 username --field2 email
  %(prog)s -i input.json -o output.csv --no-source-key
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        type=str,
        required=True,
        help='Path to input JSON file'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        required=True,
        help='Path to output CSV file'
    )
    
    parser.add_argument(
        '--field1',
        type=str,
        default='qq',
        help='Name of the first field to extract (default: qq)'
    )
    
    parser.add_argument(
        '--field2',
        type=str,
        default='phone',
        help='Name of the second field to extract (default: phone)'
    )
    
    parser.add_argument(
        '--source-key',
        type=str,
        default='_source',
        help='Key containing the data object (default: _source)'
    )
    
    parser.add_argument(
        '--no-source-key',
        action='store_true',
        help='Data is at root level, no source key needed'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    source_key = None if args.no_source_key else args.source_key
    
    try:
        extract_json_to_csv(
            args.input,
            args.output,
            args.field1,
            args.field2,
            source_key
        )
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Script to compare query results between two folders line by line
Usage: python compare_queries.py <folder1> <folder2>
"""

import sys
import os
import argparse
from pathlib import Path


def read_query_file(file_path):
    """
    Read a query file and return lines after the QUERY_N header
    Returns list of lines (stripped)
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # Skip the first line if it starts with QUERY_
    start_idx = 0
    if lines and lines[0].strip().startswith('QUERY_'):
        start_idx = 1
    
    # Return lines without trailing whitespace, filter out empty lines
    return [line.strip() for line in lines[start_idx:] if line.strip()]


def parse_data_to_set(lines):
    """
    Parse data lines into a set of tuples for order-independent comparison
    First line is assumed to be header
    """
    if not lines:
        return set(), ""
    
    data_set = set()
    
    for line in lines:
        # Convert each line to a tuple (makes it hashable for set)
        data_set.add(tuple(line.split(',')))
    
    return data_set


def compare_files(file1, file2, query_num):
    """
    Compare two query files by checking if they contain the same data
    (order-independent comparison)
    Returns True if identical, False otherwise
    """
    print(f"\n{'='*60}")
    print(f"QUERY {query_num} COMPARISON")
    print(f"{'='*60}")
    
    try:
        lines1 = read_query_file(file1)
        lines2 = read_query_file(file2)
    except Exception as e:
        print(f"❌ Error reading files: {e}")
        return False
    
    # Compare number of lines
    if len(lines1) != len(lines2):
        print(f"❌ Line count mismatch!")
        print(f"   File 1: {len(lines1)} lines")
        print(f"   File 2: {len(lines2)} lines")
        return False
    
    print(f"✓ Both files have {len(lines1)} lines")
    
    if not lines1 and not lines2:
        print(f"✅ QUERY {query_num}: Both files are empty!")
        return True
    
    # Parse data into sets
    data_set1 = parse_data_to_set(lines1)
    data_set2 = parse_data_to_set(lines2)

    
    # Compare data sets (order-independent)
    if data_set1 == data_set2:
        print(f"✅ QUERY {query_num}: Results are IDENTICAL (order-independent)!")
        return True
    
    # Find differences
    print(f"❌ QUERY {query_num}: Results DIFFER!")
    
    # Find rows only in file1
    only_in_file1 = data_set1 - data_set2
    # Find rows only in file2
    only_in_file2 = data_set2 - data_set1
    
    if only_in_file1:
        print(f"\n   Rows only in File 1 ({len(only_in_file1)} rows):")
        for i, row in enumerate(sorted(only_in_file1)[:10]):  # Show first 10
            print(f"      {','.join(row)}")
        if len(only_in_file1) > 10:
            print(f"      ... and {len(only_in_file1) - 10} more")
    
    if only_in_file2:
        print(f"\n   Rows only in File 2 ({len(only_in_file2)} rows):")
        for i, row in enumerate(sorted(only_in_file2)[:10]):  # Show first 10
            print(f"      {','.join(row)}")
        if len(only_in_file2) > 10:
            print(f"      ... and {len(only_in_file2) - 10} more")
    
    print(f"\n   Total differences: {len(only_in_file1) + len(only_in_file2)} rows differ")
    
    return False


def compare_query_folders(folder1, folder2):
    """
    Compare all query_N.csv files between two folders
    """
    folder1_path = Path(folder1)
    folder2_path = Path(folder2)
    
    # Validate folders exist
    if not folder1_path.exists():
        print(f"Error: Folder '{folder1}' does not exist")
        sys.exit(1)
    
    if not folder2_path.exists():
        print(f"Error: Folder '{folder2}' does not exist")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"COMPARING QUERY RESULTS")
    print(f"{'='*60}")
    print(f"Folder 1: {folder1_path.absolute()}")
    print(f"Folder 2: {folder2_path.absolute()}")
    
    # Find all query files in folder1
    query_files1 = sorted(folder1_path.glob('query_*.csv'))
    
    if not query_files1:
        print(f"\nNo query_*.csv files found in {folder1}")
        sys.exit(1)
    
    print(f"\nFound {len(query_files1)} query files to compare")
    
    # Compare each query file
    all_identical = True
    results_summary = []
    
    for query_file1 in query_files1:
        query_name = query_file1.name
        query_num = query_name.replace('query_', '').replace('.csv', '')
        
        query_file2 = folder2_path / query_name
        
        if not query_file2.exists():
            print(f"\n❌ {query_name}: File not found in folder 2")
            all_identical = False
            results_summary.append((query_num, False, "File missing in folder 2"))
            continue
        
        try:
            is_identical = compare_files(query_file1, query_file2, query_num)
            
            if not is_identical:
                all_identical = False
                results_summary.append((query_num, False, "Data differs"))
            else:
                results_summary.append((query_num, True, "Identical"))
                
        except Exception as e:
            print(f"\n❌ Error comparing {query_name}: {e}")
            import traceback
            traceback.print_exc()
            all_identical = False
            results_summary.append((query_num, False, f"Error: {e}"))
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    
    for query_num, is_identical, message in results_summary:
        status = "✅" if is_identical else "❌"
        print(f"{status} Query {query_num}: {message}")
    
    print(f"\n{'='*60}")
    if all_identical:
        print("🎉 ALL QUERIES ARE IDENTICAL!")
    else:
        print("⚠️  SOME QUERIES DIFFER - Review details above")
    print(f"{'='*60}\n")
    
    return all_identical


def main():
    parser = argparse.ArgumentParser(
        description='Compare query results between two folders (order-independent)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python compare_queries.py ./results ./kaggle_results
  python compare_queries.py /path/to/system/output /path/to/kaggle/output
        """
    )
    
    parser.add_argument('folder1', help='Path to first folder (e.g., system output)')
    parser.add_argument('folder2', help='Path to second folder (e.g., Kaggle output)')
    
    args = parser.parse_args()
    
    all_identical = compare_query_folders(args.folder1, args.folder2)
    
    # Exit with appropriate code
    sys.exit(0 if all_identical else 1)


if __name__ == "__main__":
    main()
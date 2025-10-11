#!/usr/bin/env python3
import sys
import os
import argparse
from pathlib import Path
import traceback


def read_query_file(file_path):
    """
    Read a query file and return lines after the QUERY_N header
    Returns list of lines (stripped)
    """
    with open(file_path, "r") as f:
        lines = f.readlines()

    # Return lines without trailing whitespace, filter out empty lines
    return [line.strip() for line in lines[0:] if line.strip()]


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
        data_set.add(tuple(line.split(",")))

    return data_set


def compare_files(file1, file2, query_num):
    print(f"QUERY {query_num} COMPARISON")

    try:
        lines1 = read_query_file(file1)
        lines2 = read_query_file(file2)
    except Exception as e:
        print(f"❌ Error reading files: {e}")

    if not lines1 and not lines2:
        print(f"QUERY {query_num}: Both files are empty! ✅")

    # Parse data into sets
    data_set1 = parse_data_to_set(lines1)
    data_set2 = parse_data_to_set(lines2)

    # Compare data sets (order-independent)
    if not int(query_num) == 4 and data_set1 == data_set2:
        print(f"QUERY {query_num}: Results are OK! ✅")
        print(f"{'-' * 60}")
        return

    elif int(query_num) == 4:
        for row in data_set1:
            if row not in data_set2:
                print("Results are WRONG! ❌\n")
                print(
                    f"Row {','.join(row)} is not in the expected result but present in the actual result"
                )
                return

        print(f"QUERY {query_num}: Results are OK! ✅")
        print(f"{'-' * 60}")
        return

    # Find differences
    print(f"QUERY {query_num}: Results are WRONG! ❌")

    # Find rows only in file1
    only_in_file1 = data_set1 - data_set2
    # Find rows only in file2
    only_in_file2 = data_set2 - data_set1

    if only_in_file1:
        print(f"\nRows in actual result ({len(only_in_file1)} rows):")
        for i, row in enumerate(sorted(only_in_file1)):
            print(f"{','.join(row)}")

    if only_in_file2:
        print(f"\nRows only in expected result ({len(only_in_file2)} rows):")
        for i, row in enumerate(sorted(only_in_file2)):
            print(f"{','.join(row)}")

    print(f"{'-' * 60}")


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

    print(f"COMPARING QUERY RESULTS")
    print(f"Folder 1: {folder1_path.absolute()}")
    print(f"Folder 2: {folder2_path.absolute()}")

    # Find all query files in folder1
    query_files1 = sorted(folder1_path.glob("query_*.csv"))

    if not query_files1:
        print(f"\nNo query_*.csv files found in {folder1}")
        sys.exit(1)

    print(f"\nFound {len(query_files1)} query files to compare")

    for query_file1 in query_files1:
        query_name = query_file1.name
        query_num = query_name.removesuffix(".csv")[-1]

        query_file2 = folder2_path / query_name

        if not query_file2.exists():
            print(f"\n❌ {query_name}: File not found in folder 2")
            continue

        try:
            compare_files(query_file1, query_file2, query_num)
        except Exception as e:
            print(f"\n❌ Error comparing {query_name}: {e}")
            traceback.print_exc()

    print(f"{'-' * 60}")


def main():
    if len(sys.argv) != 3:
        print("Usage: python compare_results.py <folder1> <folder2>")
        sys.exit(1)

    compare_query_folders(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()

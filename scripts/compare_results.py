#!/usr/bin/env python3
import sys
from pathlib import Path
import traceback


def read_query_file(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()
    return [line.strip() for line in lines if line.strip()]


def parse_data_to_set(lines):
    if not lines:
        return set()
    return {tuple(line.split(",")) for line in lines}


def compare_files(file1, file2, query_num):
    print(f"QUERY {query_num} COMPARISON")

    is_reduced = "reduced" in str(Path(file2)) or "multiclient" in str(Path(file2))
    try:
        lines1 = read_query_file(file1)
        lines2 = read_query_file(file2)
    except Exception as e:
        print(f"❌ Error reading files: {e}")
        return False

    data_set1 = parse_data_to_set(lines1)
    data_set2 = parse_data_to_set(lines2)

    if is_reduced:
        expected_correct_total_results_query3 = 20 # 10 stores in two semesters, a total of 20 correct results
    else:
        expected_correct_total_results_query3 = 30 # 10 stores in three semesters, a total of 30 correct results
    expected_correct_total_results_query4 = 30 # 10 stores and top 3 per store, a total of 30 correct results

    # Convert query number once
    q = int(query_num)

    # Queries 1 and 2: simple set comparison
    if q not in (3, 4) and data_set1 == data_set2:
        print(f"QUERY {query_num}: Results are OK! ✅")
        print("-" * 60)
        return True

    # Query 3 special comparison
    if q == 3:
        for row in data_set1:
            if not any(row[:-1] == s[:-1] for s in data_set2):
                print("Results are WRONG! ❌\n")
                print(
                    f"Row {','.join(row)} is not in the expected result but present in the actual result"
                )
                return False
            expected_correct_total_results_query3 -= 1

        if expected_correct_total_results_query3 == 0:
            print(f"QUERY {query_num}: Results are OK! ✅")
            print(f"{'-' * 60}")
            return True
        else:
            print("Not enough correct results, some are missing")
            print(f"{'-' * 60}")
            return False

    # Query 4 special comparison
    if q == 4:
        for row in data_set1:
            if row not in data_set2:
                print("Results are WRONG! ❌\n")
                print(
                    f"Row {','.join(row)} is not in the expected result but present in the actual result"
                )
                return False
            expected_correct_total_results_query4 -= 1

        if expected_correct_total_results_query4 == 0:
            print(f"QUERY {query_num}: Results are OK! ✅")
            print(f"{'-' * 60}")
            return True
        else:
            print("Not enough correct results, some are missing")
            return False

    # Standard mismatch reporting
    if data_set1 != data_set2:
        print(f"❌ QUERY {query_num}: Results are WRONG!")

        only_in_file1 = data_set1 - data_set2
        only_in_file2 = data_set2 - data_set1

        if only_in_file1:
            print("\nRows only in actual:")
            for row in sorted(only_in_file1):
                print(",".join(row))

        if only_in_file2:
            print("\nRows only in expected:")
            for row in sorted(only_in_file2):
                print(",".join(row))

        print("-" * 60)
        return False

def compare_query_folders(folder1, folder2):
    folder1_path = Path(folder1)
    folder2_path = Path(folder2)

    if not folder1_path.exists():
        print(f"Error: Folder '{folder1}' does not exist")
        return False

    if not folder2_path.exists():
        print(f"Error: Folder '{folder2}' does not exist")
        return False

    print("COMPARING QUERY RESULTS")
    print(f"Folder 1: {folder1_path.absolute()}")
    print(f"Folder 2: {folder2_path.absolute()}")

    query_files1 = sorted(folder1_path.glob("query_*.csv"))

    if not query_files1:
        print(f"No query_*.csv files found in {folder1}")
        return False

    print(f"\nFound {len(query_files1)} query files to compare\n")

    all_ok = True

    for query_file1 in query_files1:
        query_name = query_file1.name
        query_num = query_name.removesuffix(".csv").split("_")[-1]

        query_file2 = folder2_path / query_name

        if not query_file2.exists():
            print(f"❌ {query_name}: missing in expected results")
            all_ok = False
            continue

        try:
            ok = compare_files(query_file1, query_file2, query_num)
            if not ok:
                all_ok = False
        except Exception as e:
            print(f"❌ Error comparing {query_name}: {e}")
            traceback.print_exc()
            all_ok = False

    print("-" * 60)
    return all_ok


def main():
    if len(sys.argv) != 3:
        print("Usage: python compare_results.py <folder1> <folder2>")
        sys.exit(1)

    success = compare_query_folders(sys.argv[1], sys.argv[2])
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

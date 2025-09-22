# utils.py

import functools
import os

import duckdb


def check_source_data(files: list[str]) -> bool:
    """
    Checks if all required source data files exist.

    Args:
        files: A list of file paths to check.

    Returns:
        True if all files exist, False otherwise.
    """
    print("🔎 Checking for presence of source data files...")
    all_files_found = True
    for f in files:
        if not os.path.exists(f):
            print(f"❌ ERROR: Missing required file: {f}")
            all_files_found = False

    if all_files_found:
        print("✅ All source data files found.")

    return all_files_found


def concat_sheets(yrs: list[int], path: str, con: duckdb.DuckDBPyConnection):
    """
    Creates a single DuckDB relation by unioning data from multiple sheets in an Excel file.
    Only data for Local Authorities (code starting with 'E0') is collected.

    Args:
        yrs: A list of integers representing the years (and sheet names).
        path: The file path to the Excel workbook.
        con: An active DuckDB connection object.

    Returns:
        A DuckDB relation object containing the combined data.
    """
    if not yrs:
        return None

    relations_list = [
        con.sql(f"""
            SELECT *
            FROM read_xlsx('{path}', header=true, normalize_names=true, sheet='{yr}', range='A5:X374')
            WHERE code LIKE 'E0%';
        """)
        for yr in yrs
    ]

    return functools.reduce(lambda r1, r2: r1.union(r2), relations_list)

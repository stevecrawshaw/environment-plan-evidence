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
    relations_list = []

    relations_list = [
        con.sql(f"""
            SELECT *, {yr} AS calendar_year
            FROM read_xlsx('{path}',
            header=true,
            normalize_names=true,
            sheet='{yr}',
            range='A5:X374')
            WHERE code LIKE 'E0%';
        """)
        for yr in yrs
    ]

    return functools.reduce(lambda r1, r2: r1.union(r2), relations_list)


def concat_energy_sheets(yrs: list[int], path: str, con: duckdb.DuckDBPyConnection):
    """
    Creates a single DuckDB relation by unioning energy consumption data from multiple sheets
    in an Excel file. Each sheet represents a different year and contains energy data by
    fuel type and sector. The function performs UNPIVOT operations to transform the data
    from wide to long format and applies data type conversions.

    Args:
        yrs: A list of integers representing the years (and sheet names).
        path: The file path to the Excel workbook.
        con: An active DuckDB connection object.

    Returns:
        A DuckDB relation object containing the combined and transformed energy data.
    """
    if not yrs:
        return None

    # Create individual relations for each year
    relations_list = []

    for year in yrs:
        # Process each year's data with UNPIVOT operation
        year_relation = con.sql(f"""
            WITH raw_energy_la_tbl AS (
                SELECT COLUMNS(* EXCLUDE(Notes)) 
                FROM read_xlsx('{path}',
                    normalize_names=true,
                    range='A6:AJ391',
                    sheet = '{year}',
                    header=true,
                    all_varchar=true)
                WHERE code LIKE 'E0%'
            ),
            unpivoted_data AS (
                UNPIVOT raw_energy_la_tbl
                ON COLUMNS(* EXCLUDE(country_or_region, local_authority, code))
                INTO
                    NAME fuel_sector
                    VALUE GTOE
            )
            SELECT 
                country_or_region,
                local_authority,
                code,
                {year} AS calendar_year,
                regexp_replace(fuel_sector, '_note.*', '') AS fuel_sector,
                if(GTOE[1] = '[', NULL, GTOE)::FLOAT GTOE
            FROM unpivoted_data
        """)
        relations_list.append(year_relation)

    # Union all years together
    combined_relation = functools.reduce(lambda r1, r2: r1.union(r2), relations_list)

    return combined_relation

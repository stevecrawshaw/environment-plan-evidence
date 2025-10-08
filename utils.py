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


def concat_electricity_sheets(
    yrs: list[int], path: str, con: duckdb.DuckDBPyConnection
):
    """
    Creates a single DuckDB relation by unioning data from multiple sheets
    in an Excel file. Only data for Local Authorities (code starting with
    'E0') is collected.

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
    Creates a single DuckDB relation by unioning energy consumption data
    from multiple sheets in an Excel file. Each sheet represents a
    different year and contains energy data by fuel type and sector.
    The function performs UNPIVOT operations to transform the data from
    wide to long format and applies data type conversions.

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


def concat_renewable_sheets(
    yrs: list[int], types: list[str], path: str, con: duckdb.DuckDBPyConnection
):
    """
    Creates a single DuckDB relation by unioning renewable energy data
    from multiple sheets in an Excel file. Each sheet represents a
    combination of type (Generation, Capacity, Sites) and year. The
    function handles the inconsistent naming convention where some types
    use comma separators and others don't, removes note columns, and
    unpivots the data from wide to long format.

    Args:
        yrs: A list of integers representing the years.
        types: A list of strings representing the types
               ('Generation', 'Capacity', 'Sites').
        path: The file path to the Excel workbook.
        con: An active DuckDB connection object.

    Returns:
        A DuckDB relation object containing the combined and transformed
        renewable energy data.
    """
    if not yrs or not types:
        return None

    relations_list = []

    for year in yrs:
        for energy_type in types:
            # Determine separator based on type
            # (Sites uses space, others use comma+space)
            separator = " " if energy_type == "Sites" else ", "
            range = "A5:R500" if energy_type == "Generation" else "A4:R500"
            sheet_name = f"LA - {energy_type}{separator}{year}"

            # Process each sheet's data and UNPIVOT
            sheet_relation = con.sql(rf"""
                WITH raw_renewable_tbl AS (
                    SELECT 
                    COLUMNS('^(.*?)((?:_?note_\d+)+)$') AS '\1',
                    COLUMNS(c -> NOT REGEXP_MATCHES(c, '_note.*$')),
                    '{year}' AS "calendar_year",
                    '{energy_type}' AS "type"
                    FROM read_xlsx('{path}',
                         sheet = '{sheet_name}',
                         range = '{range}',
                         all_varchar = true,
                         normalize_names = true)
                    WHERE local_authority_code LIKE 'E0%'
                ),
                unpivoted_data AS (
                    UNPIVOT raw_renewable_tbl
                    ON * EXCLUDE (local_authority_code,
                                  local_authority_name,
                                  estimated_number_of_households,
                                  region,
                                  country,
                                  calendar_year,
                                  type)
                    INTO
                        NAME energy_source
                        VALUE val
                )
                SELECT
                    local_authority_code,
                    local_authority_name,
                    estimated_number_of_households,
                    region,
                    country,
                    energy_source,
                    '{year}' AS calendar_year,
                    '{energy_type}' AS type,
                    CASE
                    WHEN type = 'Sites' THEN 'number'
                    WHEN type = 'Capacity' THEN 'mw'
                    WHEN type = 'Generation' THEN 'mwh'
                    END as units,
                    if(val[1] = '[', NULL, val)::INTEGER AS value
                FROM unpivoted_data
                WHERE if(val[1] = '[', NULL, val)::INTEGER IS NOT NULL
            """)  # noqa: S608
            relations_list.append(sheet_relation)

    # Union all sheets together
    combined_relation = functools.reduce(lambda r1, r2: r1.union(r2), relations_list)

    return combined_relation

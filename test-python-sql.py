# %%

import duckdb

# %%
renewable_yrs = list(range(2014, 2025))
renewable_types = ["Generation", "Capacity", "Sites"]
renewable_path = "data/Renewable_electricity_by_local_authority_2014_-_2024.xlsx"
con = duckdb.connect(":memory:")

# %%


def concat_renewable_sheets(yrs, types, path, con):
    """
    Creates a single DuckDB relation by unioning renewable energy data
    from multiple sheets in an Excel file. Each sheet represents a
    combination of type (Generation, Capacity, Sites) and year. The
    function handles the inconsistent naming convention where some types
    use different separators in the sheet names.

    Args:
        yrs: A list of integers representing the years.
        types: A list of strings representing the types (Generation, Capacity, Sites).

    """
    for year in yrs:
        for energy_type in types:
            # Determine separator based on type
            # (Sites uses space, others use comma+space)
            separator = " " if energy_type == "Sites" else ", "
            range = "A5:R500" if energy_type == "Generation" else "A4:R500"
            sheet_name = f"LA - {energy_type}{separator}{year}"

            sql = rf"""
                SELECT COLUMNS('^(.*?)((?:_?note_\d+)+)$') AS '\1',
                                COLUMNS(c -> NOT REGEXP_MATCHES(c, '_note.*$')),
                                '{year}' AS "calendar_year",
                                '{energy_type}' AS "type"
                                FROM read_xlsx('{path}',
                                    sheet = '{sheet_name}',
                                    range = '{range}',
                                    all_varchar = true,
                                    normalize_names = true)
                                    LIMIT 1;
            """
            print(f"Executing SQL for sheet: {sheet_name}")
            print(con.sql(sql).pl())


# %%

concat_renewable_sheets(renewable_yrs, renewable_types, renewable_path, con)


# %%


# %%


# %%


# %%


# %%


# %%


# %%


# %%


# %%

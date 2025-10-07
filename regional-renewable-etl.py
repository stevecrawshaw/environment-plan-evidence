# %% [markdown]
# # Regional Renewable Energy ETL Script
#
# **Purpose**: Process renewable electricity data by local authority from Excel sheets
# **Input**: `Renewable_electricity_by_local_authority_2014_-_2024.xlsx`
# **Output**: `all_renewables_tbl.csv` (combined dataset ready for analysis)
#
# This script processes multiple Excel sheets containing renewable energy data and combines them into a single, clean dataset.

# %% [markdown]
# ## Import Required Libraries

# %%
import re
from pathlib import Path

import polars as pl

# Enable string cache for better performance with categorical data
pl.enable_string_cache()

# %% [markdown]
# ## Configuration and Setup

# %%
# Define the source Excel file path
DATA_PATH = Path("data")
EXCEL_FILE = DATA_PATH / "Renewable_electricity_by_local_authority_2014_-_2024.xlsx"
OUTPUT_FILE = DATA_PATH / "all_renewables_tbl.csv"

# Sheet processing configuration
SITES_CAPACITY_SKIP_ROWS = 3  # Number of header rows to skip for Sites/Capacity sheets
GENERATION_SKIP_ROWS = 4  # Number of header rows to skip for Generation sheets

print(f"Processing file: {EXCEL_FILE}")
print(f"Output will be saved to: {OUTPUT_FILE}")

# %% [markdown]
# ## Helper Functions


# %%
def get_excel_sheet_names(file_path: Path) -> list[str]:
    """
    Get all sheet names from an Excel file using native Polars.

    Args:
        file_path: Path to the Excel file

    Returns:
        List of sheet names
    """
    # Use Polars to read all sheets and get the keys (sheet names)
    try:
        # Read all sheets using sheet_id=0, which returns a dict with sheet names as keys
        sheets_dict = pl.read_excel(file_path, sheet_id=0)
        return list(sheets_dict.keys())
    except Exception as e:
        raise RuntimeError(
            f"Could not read sheet names from {file_path} using Polars. "
            f"Please ensure the file exists and is a valid Excel file. Error: {e}"
        ) from e


def clean_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean column names by converting to snake_case and removing note suffixes.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with cleaned column names
    """
    # Convert column names to snake_case and remove _note suffixes
    new_columns = []
    for col in df.columns:
        # Convert to lowercase and replace spaces/special chars with underscores
        clean_col = re.sub(r"[^\w\s]", "", col.lower())
        clean_col = re.sub(r"\s+", "_", clean_col)
        # Remove _note and everything after
        clean_col = re.sub(r"_note.*$", "", clean_col)
        new_columns.append(clean_col)

    return df.select(
        [
            pl.col(old).alias(new)
            for old, new in zip(df.columns, new_columns, strict=False)
        ]
    )


def filter_english_authorities(df: pl.DataFrame) -> pl.DataFrame:
    """
    Filter DataFrame to include only English Local Authorities (codes starting with 'E0').

    Args:
        df: Input DataFrame

    Returns:
        Filtered DataFrame
    """
    return df.filter(pl.col("local_authority_code").str.starts_with("E0"))


# %% [markdown]
# ## Identify and Categorize Excel Sheets

# %%
# Get all sheet names from the Excel workbook
sheet_names = get_excel_sheet_names(EXCEL_FILE)
print(f"Found {len(sheet_names)} sheets in the workbook")

# Identify sheets containing Sites or Capacity data
sites_capacity_sheets = [
    sheet for sheet in sheet_names if re.search(r"Sites|Capacity", sheet, re.IGNORECASE)
]

# Identify sheets containing Generation data
generation_sheets = [
    sheet for sheet in sheet_names if re.search(r"Generation", sheet, re.IGNORECASE)
]

print(
    f"\nSites/Capacity sheets ({len(sites_capacity_sheets)}): {sites_capacity_sheets}"
)
print(f"Generation sheets ({len(generation_sheets)}): {generation_sheets}")

# %% [markdown]
# ## Process Sites and Capacity Sheets
#
# These sheets contain data about the number of renewable energy sites and their installed capacity.


# %%
def process_sites_capacity_sheet(sheet_name: str) -> pl.DataFrame:
    """
    Process a single Sites or Capacity sheet.

    Args:
        sheet_name: Name of the Excel sheet to process

    Returns:
        Processed DataFrame
    """
    # Read the Excel sheet with all data first to examine the structure
    df_raw = pl.read_excel(
        EXCEL_FILE,
        sheet_name=sheet_name,
        read_csv_options={"skip_rows": 0},  # Don't skip rows initially
    )

    # Print structure for debugging
    print(
        f"  Raw columns for {sheet_name}: {df_raw.columns[:5]}..."
    )  # Show first 5 columns
    print(f"  Raw shape: {df_raw.shape}")

    # Now read with proper skip_rows
    df = pl.read_excel(
        EXCEL_FILE,
        sheet_name=sheet_name,
        read_csv_options={"skip_rows": SITES_CAPACITY_SKIP_ROWS},
    )

    # Manual column mapping based on typical structure
    # The first few columns are typically: Local Authority, Code, then energy types
    expected_columns = ["local_authority_name", "local_authority_code"] + [
        f"col_{i}" for i in range(len(df.columns) - 2)
    ]

    # Create a mapping for renaming columns
    column_mapping = {}
    for i, col in enumerate(df.columns):
        if i < len(expected_columns):
            column_mapping[col] = expected_columns[i]
        else:
            column_mapping[col] = f"col_{i}"

    # Apply the column mapping
    df = df.rename(column_mapping)

    # Add source sheet name for tracking
    df = df.with_columns(pl.lit(sheet_name).alias("source"))

    # Filter to English Local Authorities only (if the column exists)
    if "local_authority_code" in df.columns:
        df = filter_english_authorities(df)
    else:
        print(f"  Warning: No local_authority_code column found in {sheet_name}")
        # Try to filter by the first column if it contains codes
        first_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        try:
            df = df.filter(pl.col(first_col).str.starts_with("E0"))
            print(f"  Filtered using column: {first_col}")
        except Exception as e:
            print(f"  Could not filter by {first_col}: {e}")

    # Remove total column if it exists (redundant for analysis)
    total_cols = [col for col in df.columns if "total" in col.lower()]
    if total_cols:
        df = df.drop(total_cols)

    # Convert estimated_number_of_households to numeric, handling text values
    household_cols = [col for col in df.columns if "household" in col.lower()]
    for col in household_cols:
        df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).alias(col))

    return df


# Process all Sites and Capacity sheets
sites_capacity_dataframes = []
for sheet in sites_capacity_sheets:
    print(f"Processing Sites/Capacity sheet: {sheet}")
    try:
        df = process_sites_capacity_sheet(sheet)
        sites_capacity_dataframes.append(df)
        print(f"  ✓ Successfully processed {len(df)} rows")
    except Exception as e:
        print(f"  ✗ Error processing sheet {sheet}: {e}")

# Combine all Sites and Capacity data
if sites_capacity_dataframes:
    sites_capacity_combined = pl.concat(sites_capacity_dataframes, how="vertical")
    print(f"\nCombined Sites/Capacity data: {len(sites_capacity_combined)} rows")
else:
    sites_capacity_combined = pl.DataFrame()
    print("\nNo Sites/Capacity data found")

# %% [markdown]
# ## Process Generation Sheets
#
# These sheets contain renewable electricity generation data in MWh.


# %%
def process_generation_sheet(sheet_name: str) -> pl.DataFrame:
    """
    Process a single Generation sheet.

    Args:
        sheet_name: Name of the Excel sheet to process

    Returns:
        Processed DataFrame
    """
    # Read the Excel sheet as strings first to handle mixed data types
    df = pl.read_excel(
        EXCEL_FILE,
        sheet_name=sheet_name,
        read_csv_options={"skip_rows": GENERATION_SKIP_ROWS},
        schema_overrides={
            col: pl.Utf8 for col in range(20)
        },  # Read first 20 cols as string
    )

    # Clean column names
    df = clean_column_names(df)

    # Add source sheet name for tracking
    df = df.with_columns(pl.lit(sheet_name).alias("source"))

    # Filter to English Local Authorities only
    df = filter_english_authorities(df)

    # Remove total column if it exists
    if "total" in df.columns:
        df = df.drop("total")

    # Convert numeric columns (typically columns 5-17) from text to numeric
    # This handles text values like "[c]" for confidential data
    numeric_columns = [
        col
        for col in df.columns
        if col
        not in [
            "local_authority_code",
            "local_authority_name",
            "country_or_region",
            "source",
        ]
    ]

    for col in numeric_columns[
        :13
    ]:  # Process first 13 numeric columns (equivalent to R's 5:17)
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).alias(col))

    return df


# Process all Generation sheets
generation_dataframes = []
for sheet in generation_sheets:
    print(f"Processing Generation sheet: {sheet}")
    try:
        df = process_generation_sheet(sheet)
        generation_dataframes.append(df)
        print(f"  ✓ Successfully processed {len(df)} rows")
    except Exception as e:
        print(f"  ✗ Error processing sheet {sheet}: {e}")

# Combine all Generation data
if generation_dataframes:
    generation_combined = pl.concat(generation_dataframes, how="vertical")
    print(f"\nCombined Generation data: {len(generation_combined)} rows")
    print("Generation data structure:")
    print(generation_combined.head())
else:
    generation_combined = pl.DataFrame()
    print("\nNo Generation data found")

# %% [markdown]
# ## Combine All Renewable Energy Data
#
# Create a single comprehensive dataset from all processed sheets.


# %%
def extract_year_from_source(source_col: pl.Expr) -> pl.Expr:
    """
    Extract year from source sheet name (assumes year is at the end).

    Args:
        source_col: Polars expression for the source column

    Returns:
        Polars expression that extracts the year as integer
    """
    return source_col.str.extract(r"(\d{4})$").cast(pl.Int32)


def categorize_measure_type(source_col: pl.Expr) -> pl.Expr:
    """
    Create measure type indicator based on sheet name patterns.

    Args:
        source_col: Polars expression for the source column

    Returns:
        Polars expression that categorizes the measure type
    """
    return (
        pl.when(source_col.str.contains("Sites"))
        .then(pl.lit("sites_number"))
        .when(source_col.str.contains("Capacity"))
        .then(pl.lit("capacity_mw"))
        .when(source_col.str.contains("Generation"))
        .then(pl.lit("generation_mwh"))
        .otherwise(None)
    )


# Combine all renewable energy data into a single comprehensive table
dataframes_to_combine = []
if not sites_capacity_combined.is_empty():
    dataframes_to_combine.append(sites_capacity_combined)
if not generation_combined.is_empty():
    dataframes_to_combine.append(generation_combined)

if dataframes_to_combine:
    all_renewables_df = pl.concat(dataframes_to_combine, how="vertical")

    # Add year and measure type columns
    all_renewables_df = all_renewables_df.with_columns(
        [
            extract_year_from_source(pl.col("source")).alias("year"),
            categorize_measure_type(pl.col("source")).alias("measure"),
        ]
    )

    print(f"\nFinal combined dataset: {len(all_renewables_df)} rows")
    print(f"Years covered: {sorted(all_renewables_df['year'].unique().to_list())}")
    print(f"Measure types: {all_renewables_df['measure'].unique().to_list()}")

else:
    print("No data to combine!")
    all_renewables_df = pl.DataFrame()

# %% [markdown]
# ## Data Quality Checks and Summary

# %%
if not all_renewables_df.is_empty():
    print("=== DATA QUALITY SUMMARY ===")
    print(f"Total rows: {len(all_renewables_df):,}")
    print(f"Total columns: {len(all_renewables_df.columns)}")

    # Check for missing years or measure types
    print(f"\nYears with data: {sorted(all_renewables_df['year'].unique().to_list())}")
    print(
        f"Measure types: {all_renewables_df['measure'].value_counts().sort('measure')}"
    )

    # Check for any null values in key columns
    null_counts = all_renewables_df.null_count()
    print("\nNull value counts:")
    for col in ["local_authority_code", "year", "measure"]:
        if col in null_counts.columns:
            null_val = null_counts[col].item()
            print(f"  {col}: {null_val}")

    # Sample of the data
    print("\nSample data (first 5 rows):")
    print(all_renewables_df.head())

# %% [markdown]
# ## Export Processed Data
#
# Save the clean, analysis-ready dataset to CSV.

# %%
if not all_renewables_df.is_empty():
    # Create output directory if it doesn't exist
    OUTPUT_FILE.parent.mkdir(exist_ok=True)

    # Export to CSV
    all_renewables_df.write_csv(OUTPUT_FILE)
    print(f"✓ Successfully exported data to: {OUTPUT_FILE}")
    print(f"  File size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")

    # Verify the export by reading it back
    verification_df = pl.read_csv(OUTPUT_FILE)
    print(f"✓ Verification: Re-read {len(verification_df)} rows from exported file")

else:
    print("✗ No data to export!")

print("\n=== ETL PROCESS COMPLETE ===")

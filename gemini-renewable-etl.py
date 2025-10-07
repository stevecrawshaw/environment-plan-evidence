# %% [markdown]
# # Regional Renewable Energy ETL Script (Python/Polars)
#
# **Purpose**: Process renewable electricity data by local authority from Excel sheets.
#
# - **Input**: `Renewable_electricity_by_local_authority_2014_-_2024.xlsx`
# - **Output**: `all_renewables_tbl.csv` (a combined, analysis-ready dataset)

# %%
# Load required packages
import re
from pathlib import Path

import polars as pl

# %%
# Define the source Excel file path and output path.
# Note: Ensure the 'data' directory exists or adjust paths as needed.
DATA_DIR = Path("data")
EXCEL_FILE = DATA_DIR / "Renewable_electricity_by_local_authority_2014_-_2024.xlsx"
OUTPUT_CSV = DATA_DIR / "all_renewables_tbl.csv"

# Create the data directory if it doesn't exist
DATA_DIR.mkdir(exist_ok=True)

# %% [markdown]
# ## Step 1: Ingest Sheet Information
# We'll start by reading the Excel file to get the names of all the sheets, which we'll need to categorize and process.

# %%
# Get all sheet names from the Excel workbook.
# pl.read_excel with sheet_id=None reads all sheets into a dictionary.
# We then extract the keys (sheet names) from this dictionary.
try:
    all_sheets_dict = pl.read_excel(EXCEL_FILE, sheet_id=0)
    sheet_names = list(all_sheets_dict.keys())
    print("✅ Successfully read sheet names from the Excel file.")
except Exception as e:
    print(f"❌ Error reading Excel file: {e}")
    print("Please ensure the file path is correct and the file is not corrupted.")
    sheet_names = []

print(f"Found {len(sheet_names)} sheets in total.")

# %%
# Categorize sheets based on their content type (Sites/Capacity vs. Generation)
# This determines how many header rows to skip.
sites_capacity_sheets = [s for s in sheet_names if "Sites" in s or "Capacity" in s]
skip_sc = 3
print(
    f"Found {len(sites_capacity_sheets)} 'Sites' or 'Capacity' sheets (skipping {skip_sc} rows)."
)

generation_sheets = [s for s in sheet_names if "Generation" in s]
skip_gen = 4
print(f"Found {len(generation_sheets)} 'Generation' sheets (skipping {skip_gen} rows).")

# %% [markdown]
# ## Step 2: Process "Sites" and "Capacity" Data
# Now we'll process the first group of sheets. We'll read each one, clean the column names, and combine them into a single DataFrame.


# %%
# Helper function to clean column names, similar to R's janitor::clean_names
def clean_column_names(name: str) -> str:
    """
    Cleans a column name by:
    1. Removing '_note...' suffixes.
    2. Converting to lowercase snake_case.
    """
    name = re.sub(r"_note.+", "", name)
    name = name.lower().strip()
    name = re.sub(r"\s+", "_", name)
    return name


# %%
# Process each "Sites" and "Capacity" sheet
processed_sc_dfs = []
for sheet in sites_capacity_sheets:
    df = pl.read_excel(
        EXCEL_FILE, sheet_name=sheet, read_options={"skip_rows": skip_sc}
    )

    # Clean column names and add a 'source' column to track origin
    rename_map = {col: clean_column_names(col) for col in df.columns}
    df = df.rename(rename_map).with_columns(source=pl.lit(sheet))
    processed_sc_dfs.append(df)
# %%

processed_sc_dfs[0].glimpse()

# %%
# Combine sheets and perform final transformations
if processed_sc_dfs:
    sites_capacity_df = (
        pl.concat(processed_sc_dfs, how="diagonal_relaxed")
        .with_columns(
            # Coerce household count to a number, turning errors into nulls
            pl.col("estimated_number_of_households").cast(pl.Float64, strict=False)
        )
        # Filter for English Local Authorities only (codes starting with "E0")
        .filter(pl.col("local_authority_code").str.starts_with("E0"))
        .drop("total")  # Remove the redundant total column
    )
    print("🔎 Preview of processed Sites/Capacity Data:")
    print(sites_capacity_df.head(3))
else:
    sites_capacity_df = pl.DataFrame()
    print("⚠️ No 'Sites' or 'Capacity' sheets were found or processed.")


# %% [markdown]
# ## Step 3: Process "Generation" Data
# Next, we'll do the same for the "Generation" sheets. These have a slightly different structure, so we read all data as text first to handle special values like `[c]` before converting columns to numbers.

# %%
# Process each "Generation" sheet
processed_gen_dfs = []
for sheet in generation_sheets:
    # Read all columns as text first (infer_schema_length=0)
    df = pl.read_excel(
        EXCEL_FILE,
        sheet_name=sheet,
        read_options={"skip_rows": skip_gen},
        infer_schema_length=0,
    )

    rename_map = {col: clean_column_names(col) for col in df.columns}
    df = df.rename(rename_map).with_columns(source=pl.lit(sheet))
    processed_gen_dfs.append(df)

# Combine and process "Generation" sheets
if processed_gen_dfs:
    temp_gen_df = pl.concat(processed_gen_dfs, how="diagonal_relaxed")

    # Dynamically find columns that represent energy generation values
    id_cols = [
        "local_authority_code",
        "local_authority",
        "region",
        "country",
        "source",
        "total",
    ]
    columns_to_convert = [col for col in temp_gen_df.columns if col not in id_cols]

    generation_df = (
        temp_gen_df.drop("total")
        # Filter for English Local Authorities
        .filter(pl.col("local_authority_code").str.starts_with("E0"))
        # Convert generation columns to numeric, making errors null
        .with_columns(pl.col(columns_to_convert).cast(pl.Float64, strict=False))
    )
    print("🔎 Preview of processed Generation Data:")
    generation_df.glimpse()
else:
    generation_df = pl.DataFrame()
    print("⚠️ No 'Generation' sheets were found or processed.")


# %% [markdown]
# ## Step 4: Combine, Finalize, and Export
# Finally, we'll combine the two datasets, extract the year and a measure type from the sheet names, and save the result to a new CSV file.

# %%
# Combine all data into a single comprehensive table
if not sites_capacity_df.is_empty() or not generation_df.is_empty():
    all_renewables_df = pl.concat(
        [sites_capacity_df, generation_df], how="diagonal_relaxed"
    )

    # Add 'year' and 'measure' type columns
    all_renewables_df = all_renewables_df.with_columns(
        # Extract the 4-digit year from the end of the sheet name
        year=pl.col("source").str.extract(r"(\d{4})$").cast(pl.Int32),
        # Create a 'measure' category based on sheet name patterns
        measure=pl.when(pl.col("source").str.contains("Sites"))
        .then(pl.lit("sites_number"))
        .when(pl.col("source").str.contains("Capacity"))
        .then(pl.lit("capacity_mw"))
        .when(pl.col("source").str.contains("Generation"))
        .then(pl.lit("generation_mwh"))
        .otherwise(pl.lit(None)),
    )

    print("📊 Final Combined Data Preview:")
    print(all_renewables_df.head(3))
    print("...")
    print(all_renewables_df.tail(3))
    print(f"\nFinal DataFrame shape: {all_renewables_df.shape}")

    # Export the processed data to CSV
    try:
        all_renewables_df.write_csv(OUTPUT_CSV)
        print(f"\n✅ Successfully exported the final data to: {OUTPUT_CSV}")
    except Exception as e:
        print(f"❌ Error writing to CSV: {e}")
else:
    print("🤷 No data was processed, so no file will be exported.")

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


# %%

renewables_raw = pl.read_excel(EXCEL_FILE, sheet_id=0)

# %%

renewables_raw.keys()


# %%
# %%


# %%
# %%


# %%
# %%


# %%
# %%


# %%

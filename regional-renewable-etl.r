# Regional Renewable Energy ETL Script
# Purpose: Process renewable electricity data by local authority from Excel sheets
# Input: Renewable_electricity_by_local_authority_2014_-_2024.xlsx
# Output: all_renewables_tbl.csv (combined dataset ready for analysis)

# Load required packages
pacman::p_load(tidyverse, glue, janitor, duckdb, DBI, readxl)

# Define the source Excel file path
path <- "data/Renewable_electricity_by_local_authority_2014_-_2024.xlsx"

# Get all sheet names from the Excel workbook
sheets <- excel_sheets(path)

# Identify sheets containing Sites or Capacity data (skip 3 header rows)
# These sheets typically have similar structure with 3 rows to skip
(ingest_sheets_skip3 <- sheets |> keep(~ str_detect(.x, "Sites|Capacity")))
skip_sc <- 3

# Identify sheets containing Generation data (skip 4 header rows)
# Generation sheets have a different header structure requiring 4 rows to skip
(generation_sheets <- sheets |> keep(~ str_detect(.x, "Generation")))
skip_gen <- 4

# Process Sites and Capacity sheets
# These sheets contain data about number of renewable energy sites and their capacity
sites_capacity_tbl <- map(ingest_sheets_skip3, \(x) {
  read_excel(path, sheet = x, skip = skip_sc) |>
    clean_names() |> # Convert column names to snake_case
    # Remove "_note" suffixes and everything after from column names
    rename_with(~ str_remove(.x, "_note.+")) |>
    mutate(source = x) # Add source sheet name for tracking
}) |>
  bind_rows() |> # Combine all sheets into one dataframe
  mutate(
    # Convert households column to numeric (may contain text that needs cleaning)
    estimated_number_of_households = as.numeric(estimated_number_of_households)
  ) |>
  # Filter to English Local Authorities only (codes starting with "E0")
  filter(str_starts(local_authority_code, "E0")) |>
  select(-total) # Remove total column as it's redundant for analysis

# Process Generation sheets
# These sheets contain renewable electricity generation data in MWh
generation_tbl <- map(generation_sheets, \(x) {
  # Read as text first to handle mixed data types cleanly
  read_excel(path, sheet = x, skip = skip_gen, col_types = "text") |>
    clean_names() |> # Convert column names to snake_case
    # Remove "_note" suffixes and everything after from column names
    rename_with(~ str_remove(.x, "_note.+")) |>
    mutate(source = x) # Add source sheet name for tracking
}) |>
  bind_rows() |> # Combine all sheets into one dataframe
  select(-total) |> # Remove total column
  # Filter to English Local Authorities only
  filter(str_starts(local_authority_code, "E0")) |>
  # Convert numeric columns (columns 5-17) from text to numeric
  # This handles any text values like "[c]" for confidential data
  mutate(across(5:17, as.numeric)) |>
  glimpse() # Display structure for verification

# Combine all renewable energy data into a single comprehensive table
all_renewables_tbl <-
  bind_rows(
    sites_capacity_tbl,
    generation_tbl
  ) |>
  mutate(
    # Extract year from sheet name (assumes year is at the end of sheet name)
    year = str_extract(source, "\\d{4}$") |> as.integer(),
    # Create a measure type indicator based on sheet name patterns
    measure = case_when(
      str_detect(source, "Sites") ~ "sites_number", # Number of renewable sites
      str_detect(source, "Capacity") ~ "capacity_mw", # Installed capacity in MW
      str_detect(source, "Generation") ~ "generation_mwh", # Generation in MWh
      TRUE ~ NA_character_ # Fallback for unexpected sheet names
    )
  )


# Export the processed data to CSV for use in other systems
# This creates a clean, analysis-ready dataset
all_renewables_tbl |>
  write_csv("data/all_renewables_tbl.csv")

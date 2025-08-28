# ingest the energy consumprion data for local authorities
# minimally clean and stack the data by year for all LAs and output as parquet
#  for onward analysis

pacman::p_load(tidyverse, janitor, readxl, glue, fs, duckdb, arrow)
path <- "data/Subnational_total_final_energy_consumption_2005_2023.xlsx"
skip_rows <- 5
cells <- "A6:AJ391"


# get the sheets
data_sheets <- excel_sheets(
  path = path
) |>
  keep(~ str_detect(.x, "^2"))

# ingest into a list of sheets, one per year, cleaning the column names and adding a year column
la_energy_list <- map(
  data_sheets,
  ~ read_excel(
    path = path,
    sheet = .x,
    # skip = skip_rows,
    range = cells,
    col_names = TRUE,
    col_types = "text"
  ) |>
    clean_names() |>
    mutate(year = .x |> as.integer())
)

# some initial cleaning
raw_energy_la_tbl <- la_energy_list |>
  bind_rows() |>
  filter(!local_authority %in% c("Unallocated", "All local authorities")) |>
  mutate(across(
    -c(code, country_or_region, local_authority, notes, year),
    as.numeric
  )) |>
  glimpse()

# cleaning the column names to remove notes
new_names_no_notes <- map_chr(
  names(raw_energy_la_tbl),
  ~ str_remove(.x, "_note.+$")
)
# more cleaning and reshaping
energy_la_tbl <- raw_energy_la_tbl |>
  set_names(new_names_no_notes) |>
  select(-notes) |>
  rename_with(~ str_replace(.x, "local_authority", "ladnm")) |>
  rename_with(~ str_replace(.x, "code", "ladcd")) |>
  relocate(year, country_or_region, ladnm, ladcd) |>
  arrange(country_or_region, ladnm, year)

# pivoting longer
energy_la_long_tbl <- energy_la_tbl |>
  pivot_longer(
    -c(year, country_or_region, ladnm, ladcd),
    names_to = "fuel_sector",
    values_to = "value"
  ) |>
  arrange(country_or_region, ladnm, year, fuel_sector)
# sourcing a dataset to map fuel and sector names against the combined names
# gemini used to create this file
fuel_sector_map_tbl <- read_csv("fuel_sector.csv") |>
  mutate(across(
    c(fuel, sector),
    ~ str_replace_all(.x, "_", " ") |>
      str_to_sentence()
  ))

# joining the fuel and sector names to the main dataset
clean_energy_la_long_tbl <- energy_la_long_tbl |>
  left_join(
    fuel_sector_map_tbl,
    by = join_by("fuel_sector" == "original_string")
  ) |>
  select(-fuel_sector) |>
  glimpse()

clean_energy_la_long_tbl |>
  write_parquet("data/clean_energy_la_long_tbl.parquet")

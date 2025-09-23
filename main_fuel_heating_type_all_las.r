pacman::p_load(tidyverse, readxl, janitor)

# using the ONS spreadsheet
path <- "data/mainfueltypeenglandandwales.xlsx"
skip_rows <- 3
sheet <- "2a"
cells <- "A4:L322"

domestic_fuel_tbl <- read_excel(
  path = path,
  sheet = sheet,
  range = cells,
  col_names = TRUE,
  col_types = "text"
) |>
  clean_names() |>
  # glimpse()

  rename(
    "ladnm" = "local_authority_district_name",
    "ladcd" = "local_authority_district_code"
  ) |>
  mutate(across(
    -c(region_code, region_name, ladnm, ladcd),
    as.numeric
  )) |>
  arrange(region_code, region_name, ladnm, ladcd) |>
  pivot_longer(
    -c(region_code, region_name, ladnm, ladcd),
    names_to = "fuel_type",
    values_to = "pct_domestic_properties"
  ) |>
  glimpse()

domestic_fuel_tbl |>
  distinct(fuel_type)

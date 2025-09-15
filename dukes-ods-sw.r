# create a dataset of energy consumption by fuel use, year and la for the south west
# to publish on the open data portal
# and to use in the energy dashboard

pacman::p_load(tidyverse, glue, janitor, readxl, arrow)

# read in the data

path <- "data/Subnational_total_final_energy_consumption_2005_2023.xlsx"
skip <- 5

col_types = c(
  rep("text", 4),
  rep("numeric", 32)
)

sheets <- readxl::excel_sheets(path) |> keep(~ str_detect(., "^2"))

sheet_list <- map(
  sheets,
  ~ read_excel(path, sheet = ., skip = skip, col_types = col_types) |>
    clean_names()
)

names(sheet_list) <- sheets

fuel_sector_tbl <- read_csv("data/fuel_sector_energy_la.csv") |>
  mutate(across(c(Fuel, Sector), str_to_sentence)) |>
  rename(fuel_sector = Original, fuel = Fuel, sector = Sector) |>
  glimpse()

raw_data_wide_tbl <- sheet_list |> bind_rows(.id = "year")

energy_by_la_sector_year_ods_tbl <- raw_data_wide_tbl |>
  filter(
    country_or_region == "South West",
    local_authority != "All local authorities"
  ) |>
  rename_with(~ str_remove(., "_no.+")) |>
  select(
    -c(starts_with("all"), ends_with("total"), notes, country_or_region)
  ) |>
  pivot_longer(
    cols = -c(year, local_authority, code),
    names_to = "fuel_sector",
    values_to = "consumption_ktoe"
  ) |>
  mutate(consumption_gwh = consumption_ktoe * 11.63, year = as.integer(year)) |>
  inner_join(fuel_sector_tbl, by = "fuel_sector") |>
  glimpse()

write_csv(
  energy_by_la_sector_year_ods_tbl,
  "data/energy_by_la_sector_year_ods.csv",
  na = ""
)

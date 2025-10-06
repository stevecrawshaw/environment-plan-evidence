pacman::p_load(tidyverse, glue, janitor, duckdb, DBI, readxl)

path <- "data/Renewable_electricity_by_local_authority_2014_-_2024.xlsx"
sheets <- excel_sheets(path)

# different sheets have different header rows to skip
(ingest_sheets_skip3 <- sheets |> keep(~ str_detect(.x, "Sites|Capacity")))
skip_sc <- 3

(generation_sheets <- sheets |> keep(~ str_detect(.x, "Generation")))
skip_gen <- 4

sites_capacity_tbl <- map(ingest_sheets_skip3, \(x) {
  read_excel(path, sheet = x, skip = skip_sc) |>
    clean_names() |>
    rename_with(~ str_remove(.x, "_note.+")) |>
    mutate(source = x)
}) |>
  bind_rows() |>
  mutate(
    estimated_number_of_households = as.numeric(estimated_number_of_households)
  ) |>
  filter(str_starts(local_authority_code, "E0")) |>
  select(-total)

generation_tbl <- map(generation_sheets, \(x) {
  read_excel(path, sheet = x, skip = skip_gen, col_types = "text") |>
    clean_names() |>
    rename_with(~ str_remove(.x, "_note.+")) |>
    mutate(source = x)
}) |>
  bind_rows() |>
  select(-total) |>
  filter(str_starts(local_authority_code, "E0")) |>
  mutate(across(5:17, as.numeric)) |>
  glimpse()

all_renewables_tbl <-
  bind_rows(
    sites_capacity_tbl,
    generation_tbl
  ) |>
  mutate(
    year = str_extract(source, "\\d{4}$") |> as.integer(),
    measure = case_when(
      str_detect(source, "Sites") ~ "sites_number",
      str_detect(source, "Capacity") ~ "capacity_mw",
      str_detect(source, "Generation") ~ "generation_mwh",
      TRUE ~ NA_character_
    )
  )


all_renewables_tbl |>
  write_csv("data/all_renewables_tbl.csv")

# ingest the energy consumprion data for local authorities
# minimally clean and stack the data by year for all LAs and output as parquet
#  for onward analysis

pacman::p_load(tidyverse, janitor, readxl, glue, arrow, duckdb, DBI, ellmer)
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


# using the EPC data

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "../mca-data/data/ca_epc.duckdb",
  read_only = TRUE
)

con |> dbExecute("INSTALL SPATIAL;")
con |> dbExecute("LOAD SPATIAL;")

tables <- con |> dbGetQuery("SHOW TABLES;")


epc_domestic_fuel_tbl <- dbGetQuery(
  con,
  "SELECT
    COUNT(*) AS n_properties,
    MAINHEAT_DESCRIPTION mainheat,
   FROM epc_domestic_ods_vw
   GROUP BY mainheat"
) |>
  as_tibble() |>
  glimpse()

epc_domestic_fuel_tbl |>
  arrange(desc(n_properties)) |>
  write_delim(
    file = "data/epc_domestic_fuel_types.txt",
    delim = "|"
  )

# fuel source categories derived from the EPC data by gemini
#https://g.co/gemini/share/c5c2c07ca31d
#https://docs.google.com/spreadsheets/d/1I0xf4KV98edBeGvQoQu5_0XCS-uzba4910EyWypUoXg/edit?gid=2063792901#gid=2063792901

fuel_mainheat_categories_tbl <- read_tsv(
  "data/epc_fuel_main_heating_lep_2025.tsv"
)

summary_mainheat_category_lep_tbl <- fuel_mainheat_categories_tbl |>
  group_by(main_fuel_category) |>
  summarise(n = sum(n_properties), .groups = "drop") |>
  mutate(pct = n / sum(n)) |>
  arrange(desc(n)) |>
  glimpse()

# Using an LLM to help classify the main heating categories

api_key = config::get(
  config = "anthropic",
  file = "../config.yml",
  value = "apikey"
)
# painfully slow with perplexity, rate limits hit with gemini
# anthropic probably the way to go for this sort of thing but so expensive!
# how to fix - do it in json or batches?
heat_chat <- chat_anthropic(
  system_prompt = "You are a helpful data analyst assistant focused on accuracy.
  You respond tersely with no additional commentary.",
  # model = "sonar",
  api_key = api_key
)


categorise_heat_type <- function(chat = heat_chat, description) {
  prompt <- glue::glue(
    "What is the main category of heating for the description.
'{description}'
The possible broad categories are:

Gas Central Heating
Oil Central Heating
Community Scheme
Heat pump
Electric Storage Heating
Other

Take your time and be diligent and accurate.
Answer ONLY with one of the categories above."
  )
  resp <- as.character(heat_chat$chat(prompt))
  return(resp)
}

# testing the function
out <- categorise_heat_type(
  description = "Boiler and radiators, electric"
)

categorised_fuel_type_heating <- epc_domestic_fuel_tbl |>
  rowwise() |>
  mutate(
    main_fuel_category = categorise_heat_type(
      chat = heat_chat,
      description = mainheat
    )
  )

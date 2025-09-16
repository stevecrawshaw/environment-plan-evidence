# get the total renewable capacity in the West of England
# for the latest year
pacman::p_load(
  tidyverse,
  ggplot2,
  ggthemes,
  ggrepel,
  patchwork,
  scales,
  duckdb,
  DBI,
  jsonlite
)

p_colors <- read_json(
  "https://raw.githubusercontent.com/westofengland-ca/weca_templates/refs/heads/main/General_branding/brand_guidelines.json"
) |>
  pluck(3, "colors", "primary_colors")

s_colors <- read_json(
  "https://raw.githubusercontent.com/westofengland-ca/weca_templates/refs/heads/main/General_branding/brand_guidelines.json"
) |>
  pluck(3, "colors", "secondary_colors")

west_green <- p_colors$west_green$hex
grey <- s_colors$warm_grey$hex

# data from regional-renewable-etl.r in this folder
renewable_data_raw_tbl <- read_csv("data/all_renewables_tbl.csv")


con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "../mca-data/data/ca_epc.duckdb",
  read_only = FALSE
)

ca_la_tbl <- tbl(con, "ca_la_tbl") |>
  collect()


unique(renewable_data_raw_tbl$measure)

latest_capacity_la_tbl <-
  renewable_data_raw_tbl |>
  filter(measure == "capacity_mw") |>
  inner_join(ca_la_tbl, by = join_by(local_authority_code == ladcd)) |>
  filter(
    cauthnm == "West of England",
    year == max(renewable_data_raw_tbl$year)
  ) |>
  select(
    -c(
      local_authority_code,
      measure,
      cauthcd,
      cauthnm,
      year,
      ladnm,
      estimated_number_of_households,
      source,
      region,
      country
    )
  ) |>
  pivot_longer(
    cols = c(
      photovoltaics,
      onshore_wind,
      hydro,
      anaerobic_digestion,
      offshore_wind,
      wave_tidal,
      sewage_gas,
      landfill_gas,
      municipal_solid_waste,
      animal_biomass,
      plant_biomass,
      cofiring
    ),
    names_to = "technology",
    values_to = "capacity_mw"
  ) |>
  glimpse()

latest_capacity_la_tbl |>
  summarise(total_capacity_mw = sum(capacity_mw, na.rm = TRUE)) |>
  pull()

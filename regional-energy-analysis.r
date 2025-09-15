pacman::p_load(
  tidyverse,
  janitor,
  readxl,
  glue,
  fs,
  duckdb,
  arrow,
  DBI,
  gt,
  gtExtras,
  paletteer
)


energy_data_all_la_tbl <- read_parquet(
  "../environment-plan-evidence/data/clean_energy_la_long_tbl.parquet"
)
# motherduck extension not available in windows!
con <- dbConnect(duckdb::duckdb("data/regional_energy.duckdb"))
con |> dbExecute("INSTALL spatial;")
con |> dbExecute("LOAD spatial;")
emissions_sw_tbl <- dbGetQuery(con, "FROM emissions_sw_tbl") |> collect()
sw_la_tbl <- dbGetQuery(con, "FROM sw_la_tbl") |> collect()
weca_epc_tbl <- dbGetQuery(con, "FROM weca_epc_tbl") |> collect()
dfes_la_tbl <- dbGetQuery(con, "FROM dfes_la_tbl") |> collect()
regional_carbon_intensity_tbl <- dbGetQuery(
  con,
  "FROM regional_carbon_intensity_tbl"
) |>
  collect()
carbon_intensity_categories_tbl <- dbGetQuery(
  con,
  "FROM carbon_intensity_categories_tbl"
) |>
  collect()


con |> dbDisconnect()
# Emissions plot

emissions_plot_sw_tbl <- emissions_sw_tbl |>
  select(calendar_year, ends_with("total"), -grand_total) |>
  group_by(calendar_year) |>
  summarise(across(ends_with("total"), ~ sum(.x, na.rm = TRUE))) |>
  glimpse()

emissions_plot_sw_long_tbl <- emissions_plot_sw_tbl |>
  pivot_longer(
    cols = -calendar_year,
    names_to = "sector",
    values_to = "emissions_ktco2e"
  ) |>
  mutate(
    emissions_mtco2e = emissions_ktco2e / 1000,
    sector = str_remove_all(sector, "_total") |>
      str_replace("_", " ") |>
      str_to_sentence()
  ) |>
  glimpse()

emissions_plot_sw_long_tbl |>
  ggplot(aes(x = calendar_year, y = emissions_mtco2e, color = sector)) +
  geom_line(size = 1.5) +
  scale_colour_paletteer_d(
    "rcartocolor::Temps"
  ) +
  labs(
    title = "Total greenhouse gas emissions in the South West",
    subtitle = "Territorial emissions by sector",
    x = "Year",
    y = "Emissions (MtCO2e)",
    color = "Sector",
    caption = "Source: UK greenhouse gas emissions: local authority and regional"
  ) +
  theme_minimal()

# DFES - what to do?
lep_dfes_tbl <- dfes_la_tbl |>
  filter(lad_name %in% weca_epc_tbl$ladnm) |>
  mutate(value = as.numeric(value), year = as.integer(year)) |>
  glimpse()

unique(lep_dfes_tbl$technology)

lep_dfes_tbl |>
  filter(scenario == "Counterfactual", technology == "Heat pumps") |>
  ggplot(aes(x = year, y = value, color = subtechnology)) +
  geom_line(size = 1.5) +
  scale_colour_paletteer_d(
    "rcartocolor::Vivid"
  ) +
  facet_wrap(~lad_name, scales = "free_y") +
  theme_minimal()

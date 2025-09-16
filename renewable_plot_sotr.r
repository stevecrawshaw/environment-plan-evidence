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

generation_cauth_wide_tbl <- renewable_data_raw_tbl |>
  filter(measure == "generation_mwh") |>
  inner_join(ca_la_tbl, by = join_by(local_authority_code == ladcd)) |>
  glimpse()


generation_long_tbl <-
  generation_cauth_wide_tbl |>
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
    names_to = "renewable_type",
    values_to = "generation_mwh"
  ) |>
  glimpse()

generation_plot_tbl <- generation_long_tbl |>
  group_by(year, cauthnm) |>
  summarise(
    total_generation = sum(generation_mwh, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    color = case_when(
      cauthnm == "West of England" ~ west_green,
      TRUE ~ grey
    )
  ) |>
  glimpse()

prop_renewables_lep_latest_tbl <- generation_long_tbl |>
  filter(year == max(year), cauthnm == "West of England") |>
  arrange(desc(generation_mwh)) |>
  mutate(
    renewable_source = str_replace_all(renewable_type, "_", " ") |>
      str_to_sentence()
  )

latest_generation_lep <- prop_renewables_lep_latest_tbl |>
  summarise(total_generation = sum(generation_mwh, na.rm = TRUE)) |>
  pull(total_generation)

latest_generation_lep

latest_renewable_by_source_la_plot <-
  prop_renewables_lep_latest_tbl |>
  filter(generation_mwh > 0) |>
  ggplot(aes(
    x = fct_reorder(renewable_source, generation_mwh),
    y = generation_mwh,
    fill = local_authority_name
  )) +
  geom_col() +
  theme_minimal() +
  theme(axis.text.x = element_text(size = 12)) +
  labs(
    title = "West of England Renewable Energy Generation by Source",
    subtitle = paste0(
      "Total Renewable Energy Generation (MWh) in ",
      max(generation_long_tbl$year)
    ),
    x = "Renewable Source",
    y = "Total Generation (MWh)",
    fill = "Local Authority",
    caption = "Data Source: DESNZ"
  ) +
  scale_y_continuous(labels = comma)

latest_renewable_by_source_la_plot |>
  ggsave(
    filename = "plots/renewable_generation_lep_by_source_la.png",
    plot = _,
    width = 10,
    height = 6,
    bg = "white"
  )


lep_renewable_plot_data <-
  generation_plot_tbl |>
  filter(cauthnm == "West of England") |>
  arrange(year) |>
  glimpse()

lep_renewable_plot <- lep_renewable_plot_data |>
  ggplot(aes(x = year, y = total_generation)) +
  geom_col(fill = west_green) +
  theme_minimal() +
  scale_y_continuous(labels = comma) +
  scale_x_continuous(breaks = seq(2014, 2023, by = 1)) +
  labs(
    title = "West of England Renewable Energy Generation",
    subtitle = "Total Renewable Energy Generation (MWh)",
    x = "Year",
    y = "Total Generation (MWh)",
    caption = "Data Source: DESNZ"
  ) +
  theme(axis.text.x = element_text(size = 12))


ggsave(
  plot = lep_renewable_plot,
  filename = "plots/renewable_generation_weca.png",
  width = 10,
  height = 6,
  bg = "white"
)


generation_plot_tbl |>

  ggplot(aes(x = year, y = total_generation, color = color, group = cauthnm)) +
  scale_color_identity() +
  geom_line(size = 1) +
  theme_minimal() +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Renewable Energy Generation by Combined Authority",
    subtitle = "Total Renewable Energy Generation (MWh)",
    x = "Year",
    y = "Total Generation (MWh)",
    color = "Combined Authority",
    caption = "Data Source: DESNZ"
  ) +
  theme(legend.position = "bottom")
# +
#   ggsave("outputs/renewable_generation_by_cauth.png", width = 10, height = 6)

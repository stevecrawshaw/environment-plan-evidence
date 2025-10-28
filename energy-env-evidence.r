# iterating script to create renewable energy generation bar chart for WECA local authorities
# this is now implemented in the env-plan-evidence-optimised.qmd file

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
  treemapify,
  openair,
  ggtext,
  patchwork,
  showtext,
  fs,
  ragg
)

## AMENDED: Created a function to avoid repeating the JSON parsing logic.
# This function fetches a colour palette from the WECA brand guidelines.
fetch_weca_palette <- function(palette_type) {
  jsonlite::read_json(
    "https://raw.githubusercontent.com/westofengland-ca/weca_templates/refs/heads/main/General_branding/brand_guidelines.json"
  ) |>
    pluck(3, "colors", palette_type) |>
    map_chr("hex") |>
    unname()
}

weca_primary_palette <- fetch_weca_palette("primary_colors")
weca_secondary_palette <- fetch_weca_palette("secondary_colors")

con_epc <- dbConnect(duckdb::duckdb("../mca-data/data/ca_epc.duckdb"))
# invisible(dbExecute(con_epc, "INSTALL spatial"))
invisible(dbExecute(con_epc, "LOAD spatial"))
con_env <- dbConnect(duckdb::duckdb("data/regional_energy.duckdb"))
invisible(dbExecute(con_env, "LOAD SPATIAL"))

# Query necessary tables
ca_la_tbl <- dbGetQuery(con_epc, "FROM ca_la_tbl")

# Get names and codes for WECA and North Somerset local authorities
weca_ns_la_codes <- ca_la_tbl |>
  filter(cauthnm == "West of England") |>
  pull(ladcd)

weca_ns_la_names <- ca_la_tbl |>
  filter(cauthnm == "West of England") |>
  pull(ladnm)

# Create a named vector of 4 colours for plots
weca_palette_4 <- weca_primary_palette[2:5] |>
  set_names(weca_ns_la_names)

dbListTables(con_env)


renewable <- tbl(con_env, "renewable_la_long_tbl")

max_renewable_year <- renewable |>
  summarise(max_year = max(calendar_year, na.rm = TRUE)) |>
  pull(max_year)

total_generation <- renewable |>
  filter(
    type == "Generation",
    energy_source == "total",
    calendar_year == max_renewable_year,
    local_authority_code %in% weca_ns_la_codes
  ) |>
  # group_by(local_authority_name) |>
  summarise(total_gen_gwh = sum(value, na.rm = TRUE) / 1000) |>
  collect()

renewable_generation_latest_year_tbl <-
  renewable |>
  filter(
    local_authority_code %in% weca_ns_la_codes,
    calendar_year == max_renewable_year,
    type == "Generation",
    energy_source != "total",
    value > 0
  ) |>
  collect() |>
  mutate(
    Source = str_replace_all(energy_source, "_", " ") |>
      str_to_sentence(),
    gen_GWH = value / 1000
  )

renewable_plot_gg <-
  renewable_generation_latest_year_tbl |>
  ggplot(aes(
    x = Source,
    y = gen_GWH,
    fill = local_authority_name
  )) +
  geom_col(position = "stack") +
  scale_fill_manual(values = weca_palette_4) +
  labs(
    title = ("Renewable Energy Generation"),
    subtitle = glue("WECA Local Authorities in {max_renewable_year}"),
    caption = "Source: DESNZ Regional Renewable Statistics",
    x = "Renewable Energy Source",
    y = "GWh",
    fill = "Local Authority"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(face = "bold", size = 14, hjust = 0),
    plot.caption = element_text(size = 10),
    legend.position = "right",
    legend.key.size = unit(0.5, "cm"),
    legend.text = element_text(size = 10),
    legend.title = element_text(size = 12),
    strip.text = element_text(size = 14),
    axis.title.x = element_text(size = 12),
    axis.title.y = element_text(size = 12),
    axis.text = element_text(size = 10, face = "bold")
  ) +
  coord_flip()

renewable_plot_gg


ggsave(
  "plots/renewable_bar_chart.png",
  renewable_plot_gg,
  width = 8,
  height = 6,
  bg = "white",
  dpi = 300
)

dbDisconnect(con_epc, shutdown = TRUE)
dbDisconnect(con_env, shutdown = TRUE)

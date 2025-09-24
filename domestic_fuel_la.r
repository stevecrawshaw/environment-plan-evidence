# Script to classify EPC main heating types into broader categories
# and aggregate to LA level or combined authorities to understand
# fuel use in housing stock

# Libraries ----
pacman::p_load(
  tidyverse,
  glue,
  duckdb,
  DBI,
  ellmer,
  jsonlite,
  readxl,
  janitor
)

# Get the ONS fuel type by LA data ----

path <- "data/mainfueltypeenglandandwales.xlsx"
skip_rows <- 3
sheet <- "2a"
cells <- "A4:L322"

ons_domestic_fuel_tbl <- read_excel(
  path = path,
  sheet = sheet,
  range = cells,
  col_names = TRUE,
  col_types = "text"
) |>
  pivot_longer(
    -c(
      `Region code`,
      `Region name`,
      `Local authority district code`,
      `Local authority district name`
    ),
    names_to = "fuel_type",
    values_to = "pct_domestic_properties_ons"
  ) |>
  clean_names() |>
  rename(
    "ladnm" = "local_authority_district_name",
    "ladcd" = "local_authority_district_code"
  ) |>
  mutate(across(
    -c(region_code, region_name, ladnm, ladcd, fuel_type),
    as.numeric
  )) |>
  arrange(region_code, region_name, ladnm, ladcd) |>
  glimpse()

(fuel_categories <- unique(ons_domestic_fuel_tbl$fuel_type))


# Access EPC data from a DuckDB database ----

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "../mca-data/data/ca_epc.duckdb",
  read_only = TRUE
)

con |> dbExecute("LOAD SPATIAL;")

# Utility function to chunk a vector into smaller pieces
chunk_vector <- function(vec, n) {
  # Create the grouping factor
  grouping_factor <- (seq_along(vec) - 1) %/% n

  # Split the vector and return the list
  split(vec, grouping_factor)
}

# Just get the unique main heating descriptions and counts
epc_domestic_fuel_tbl <- dbGetQuery(
  con,
  "SELECT
   DISTINCT MAINHEAT_DESCRIPTION mainheat,
   FROM epc_domestic_vw
   WHERE MAINHEAT_DESCRIPTION IS NOT NULL
   AND LODGEMENT_DATE <= date'2024-03-31'
   GROUP BY mainheat"
) |>
  as_tibble() |>
  rownames_to_column("id") |>
  mutate(
    # include an ID to help with tracking items through the process
    mainheat_clean = str_c(
      id,
      "_",
      str_replace_all(mainheat, "[^a-zA-Z\\d\\s:]", " ") |>
        str_squish()
    )
  ) |>
  glimpse()

# a list of the unique main heating descriptions
# The EPC data is very messy so there are over 700 unique descriptions
# From a dataset that covers all combined authorities in England
description_list <- (epc_domestic_fuel_tbl$mainheat_clean)

description_list |> sample(10)

# chunk up the input vector as the LLM API struggles with a long context
chunked_description_list <- chunk_vector(description_list, 100)

# Using an LLM to help classify the main heating categories ----

# Get the API key from a config file
api_key = config::get(
  config = "openrouter",
  file = "../config.yml",
  value = "apikey"
)

# create a chat object with a system prompt to set the behaviour
# Model chosen to give a good balance of cost and performance
heat_chat <- chat_openrouter(
  system_prompt = "You are an expert data categorization assistant.
  You follow instructions precisely and return only the requested format.",
  api_key = api_key,
  model = "google/gemini-2.5-flash-lite",
  echo = "none"
)

categorise_heat_type <- function(
  chat = heat_chat,
  description_list,
  fuel_categories
) {
  # The improved prompt is more structured with clear sections, rules, and examples.
  prompt <- glue(
    "## TASK
You will classify each item in a list of home heating descriptions into a single, predefined category.

## CATEGORIES
You must use ONLY one of the following categories for each item:
{toString(fuel_categories)}

## RULES
1.  **'Mains gas'**: Assign for systems using natural gas from the grid (e.g., 'gas central heating', 'gas boiler').
2.  **'Electricity'**: Assign for systems primarily powered by electricity (e.g., 'electric storage heaters', 'electric boiler').
3.  **'Oil'**: Assign for systems that use heating oil.
4.  **'Community heating scheme'**: Assign for any shared or communal source (e.g., 'district heating', 'communal boiler').
5.  **'Renewable energy(including heat pumps)'**: Assign for a *single* renewable source like 'air source heat pump', 'ground source heat pump', or 'solar heating'.
6.  **'Other and unknown'**: This is a catch-all category. Use it for:
    -   Solid fuels (coal, wood, biomass).
    -   Tank or bottled gas (LPG).
    -   Vague descriptions ('unknown', 'n/a', 'warm air unit' without a fuel type).
    -   Other specific biofuels (B30K, bioethanol).
7.  **'Two or more(not including renewable energy)'**: Assign ONLY when two or more distinct, non-renewable systems are listed (e.g., 'mains gas and electric storage heaters').
8.  **'Two or more(including renewable energy)'**: Assign ONLY when a renewable system is listed alongside any other system (e.g., 'air source heat pump with gas boiler').
9.  ** The number and underscore at the start of each item is an ID and must be retained in the output.

## OUTPUT FORMAT
- The output must be ONLY the original item (with the id number), a pipe and a space '| ', and the category.
- Each item must be on a new line.
- Do not add any commentary, explanations, or text before or after the list.

## EXAMPLES
- Input: '1_Gas Central Heating' -> Output: 1_Gas Central Heating| Mains gas
- Input: '3_ASHP' -> Output: 3_ASHP| Renewable energy(including heat pumps)
- Input: '101_LPG boiler' -> Output: 101_LPG boiler| Other and unknown
- Input: '324_Communal System' -> Output: 324_Communal System| Community heating scheme
- Input: '6_Oil boiler and solar panels' -> Output: 6_Oil boiler and solar panels| Two or more(including renewable energy)

## YOUR TURN
Now, categorize every item in this list:
'{toString(description_list)}'"
  )
  resp <- (heat_chat$chat(prompt))
  return(resp)
}
# provide the entire list of descriptions to categorise
# rather than one at a time to avoid rate limits
# this uses very few tokens

mainheat_category_table <- read_rds("data/mainheat_category_table.rds")

# THIS LINE COSTS MONEY TO RUN ----------------
# Apply the function to each chunk of the description list
# cr_list <- chunked_description_list |>
#   map(~ categorise_heat_type(heat_chat, .x, fuel_categories))

#------------------------------------------

# now parse the output into a table, joining with the original data
mainheat_category_table <- cr_list |>
  map(
    ~ read_delim(
      .x,
      delim = "| ",
      col_names = c("mainheat_clean", "category")
    )
  ) |>
  bind_rows() |>
  mutate(mainheat_clean = str_remove_all(mainheat_clean, "'")) |>
  separate_wider_delim(
    cols = mainheat_clean,
    delim = "_",
    names = c("id", "mainheat_chat_out")
  ) |>
  inner_join(
    epc_domestic_fuel_tbl,
    by = join_by("id" == "id")
  )

mainheat_category_table |> write_rds("data/mainheat_category_table.rds")

# Query the EPC data again to get counts of properties
# by main heating description and Combined Authority

cauth_qry <- "SELECT 
    ca.cauthnm combined_authority,
    epc.MAINHEAT_DESCRIPTION mainheat_description,
    COUNT(*) AS n_properties
   FROM epc_domestic_vw epc INNER JOIN ca_la_tbl ca
   ON epc.LOCAL_AUTHORITY = ca.ladcd
   WHERE epc.LODGEMENT_DATE <= date'2024-03-31'
   GROUP BY cauthnm, mainheat_description"

la_query <- "SELECT 
    epc.LOCAL_AUTHORITY_LABEL local_authority_label,
    epc.LOCAL_AUTHORITY local_authority_code,
    epc.MAINHEAT_DESCRIPTION mainheat_description,
    COUNT(*) AS n_properties
   FROM epc_domestic_vw epc
   WHERE epc.LODGEMENT_DATE <= date'2024-03-31'
   GROUP BY LOCAL_AUTHORITY_LABEL,
            LOCAL_AUTHORITY,
            mainheat_description"

epc_ca_mainheat_tbl <- dbGetQuery(
  con,
  la_query
) |>
  as_tibble() |>
  glimpse()


summary_mainheat_category_tbl <- epc_ca_mainheat_tbl |>
  inner_join(
    mainheat_category_table,
    by = join_by("mainheat_description" == "mainheat")
  ) |>
  group_by(local_authority_code, local_authority_label, category) |>
  summarise(n = sum(n_properties), .groups = "drop_last") |>
  mutate(pct_domestic_properties_epc_llm = n * 100 / sum(n)) |>
  arrange(local_authority_label, category) |>
  glimpse()

# Now we need to vaidate against
# the LA data in main_fuel_heating_type_all_la.r -------

la_codes <- summary_mainheat_category_tbl |>
  distinct(local_authority_code) |>
  arrange(local_authority_code) |>
  pull()

ons_domestic_fuel_la_tbl <- ons_domestic_fuel_tbl |>
  filter(ladcd %in% la_codes, fuel_type %in% fuel_categories) |>
  select(ladcd, ladnm, fuel_type, pct_domestic_properties_ons) |>
  glimpse()

# Join the two datasets for comparison

comparison_tbl <- summary_mainheat_category_tbl |>
  rename(
    "ladcd" = "local_authority_code",
    "fuel_type" = "category"
  ) |>
  inner_join(
    ons_domestic_fuel_la_tbl,
    by = join_by("ladcd", "fuel_type")
  ) |>
  mutate(
    diff_pct = pct_domestic_properties_epc_llm - pct_domestic_properties_ons,
    abs_diff_pct = abs(diff_pct)
  ) |>
  arrange(ladcd, ladnm, fuel_type) |>
  glimpse()

# Visualise the comparison ----
# using a scatter plot with facets for each fuel type
comparison_scatter_plot <- comparison_tbl |>
  ggplot(aes(
    x = pct_domestic_properties_ons,
    y = pct_domestic_properties_epc_llm
  )) +
  labs(
    x = "ONS category percentage",
    y = "predicted\npercentage\nfrom\nEPC LLM",
    title = "Comparison of domestic fuel type proportions",
    subtitle = "Predicted EPC fuel type category vs ONS LA level data for combined authorities",
    caption = "Source: ONS and MCA analysis of EPC data to March 2024"
  ) +
  geom_point() +
  facet_wrap(~fuel_type, scales = "free") +
  theme_minimal() +
  theme(
    axis.title.y = element_text(size = 10, angle = 0),
    axis.title.x = element_text(size = 12)
  )

comparison_scatter_plot

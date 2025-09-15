pacman::p_load(tidyverse, janitor, glue, fs, duckdb, DBI)

lep_electricity_2023_tbl <- read_csv(
  "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/energy_by_la_sector_year_ods/exports/csv?lang=en&refine=local_authority%3A%22Bath%20and%20North%20East%20Somerset%22&refine=local_authority%3A%22Bristol%2C%20City%20of%22&refine=local_authority%3A%22South%20Gloucestershire%22&refine=local_authority%3A%22North%20Somerset%22&refine=year%3A%222023%22&refine=fuel%3A%22Electricity%22&facet=facet(name%3D%22local_authority%22%2C%20disjunctive%3Dtrue)&timezone=Europe%2FLondon&use_labels=false&delimiter=%2C"
)

total_electricity_region <- lep_electricity_2023_tbl |>
  summarise(total_consumption_gwh = sum(consumption_gwh, na.rm = TRUE)) |>
  pull(total_consumption_gwh)

total_electricity_region

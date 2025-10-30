# Use the fingertipsR package to get PM2.5 mortality data for West of England Combined Authority (WECA) area
# We need to calculate the percentage mortality for WECA as the estimates are only provided at UA level
# So we need population over 30 to weight the mortality estimates

# Enable repository from ropensci

if (all(grepl("fingertipsR", installed.packages()[, "Package"])) == FALSE) {
  options(
    repos = c(
      ropensci = 'https://ropensci.r-universe.dev',
      CRAN = 'https://cloud.r-project.org'
    )
  )
}

# Download and install fingertipsR in R
#install.packages('fingertipsR')
pacman::p_load(tidyverse, glue, janitor, fingertipsR, jsonlite)

# get the available profiles and filter for the one that includes air pollution

indicators_tbl <- indicators()

pm_25_mortality_inds <-
  indicators_tbl[
    grepl(
      "mortality.*pollution|pollution.*mortality",
      indicators_tbl$IndicatorName
    ) &
      !grepl(
        "old",
        indicators_tbl$IndicatorName
      ),
  ]

if (var(pm_25_mortality_inds$IndicatorID) == 0) {
  indicator_id_pollution <- pm_25_mortality_inds$IndicatorID[1]
}

# we want the area ID for counties and unitary authorities (502)
area_id <- 502
#"Counties & UAs (from Apr 2023)"
# get the data for the indicator and area type for all years and areas
pm25_data <- fingertips_data(
  IndicatorID = indicator_id_pollution,
  AreaTypeID = area_id
)

max_year <- pm25_data |>
  summarise(max_year = max(TimeperiodSortable)) |>
  mutate(max_year = as.character(max_year) |> str_sub(1, 4)) |>
  pull()

# get the latest mortality % for each of the UA's
mortality_latest_weca_tbl <- pm25_data |>
  filter(
    AreaCode %in% c("E06000022", "E06000023", "E06000024", "E06000025"),
    Timeperiod == max_year
  ) |>
  select(AreaCode, AreaName, Value, Timeperiod) |>
  glimpse()

# Now we need population over 30 to weight the mortality estimates - from the open data portal census data
# data for the population over 30 - which is what the indicator relates to
pop_ods_url <- "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/age-sex-ods-lep/records?select=sum%28population%29%20AS%20population&where=mid_range_int%20%3E%3D%2032&group_by=local_authority&limit=10&offset=0&timezone=UTC&include_links=false&include_app_metas=false"

pop_ods_json <- fromJSON(pop_ods_url)
pop_ods_gte_30_tbl <- pop_ods_json$results |>
  as_tibble()

# join the population data to the mortality data and calculate the
# percentage mortality by weighting by population (over 30)
# note that we should really be using mortality in UA's rather than population
# but this is likely to be too variable
mortality_pop_latest_weca_tbl <- mortality_latest_weca_tbl |>
  left_join(
    pop_ods_gte_30_tbl,
    by = join_by("AreaName" == "local_authority")
  ) |>
  group_by(AreaName) |>
  mutate(
    deaths_estimate = (Value / 100) * population
  ) |>
  adorn_totals() |>
  filter(AreaCode == "Total") |>
  transmute(
    weca_pm25_mortality_gte_30 = (deaths_estimate * 100) / population
  ) |>
  pull()

mortality_pop_latest_weca_tbl

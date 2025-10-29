pacman::p_load(tidyverse, janitor, glue, DBI, duckdb, timetk)

# con_envnect to DuckDB database

con_env <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = "data/regional_energy.duckdb",
    read_only = TRUE
)

seabank_tbl <- tbl(con_env, "seabank_tbl") |>
    group_by(date = as.Date(halfhourendtime), bmunit) |>
    summarise(
        generation_mwh = sum(generation_mwh, na.rm = TRUE),
        .groups = "drop"
    ) |>
    arrange(bmunit, date) |>
    collect()


seabank_plotly <- seabank_tbl |>
    plot_time_series(
        .date_var = date,
        .value = generation_mwh,
        .color_var = bmunit,
        .smooth = FALSE,
        .facet_vars = bmunit,
        .facet_ncol = 2,
        .title = "Seabank Power Station Generation by Generating Unit",
        .x_lab = "Date",
        .y_lab = "Generation (MWh)"
    )

seabank_gg <- seabank_tbl |>
    ggplot(aes(x = date, y = generation_mwh, color = bmunit)) +
    geom_line() +
    facet_wrap(~bmunit, ncol = 1) +
    labs(
        title = "Seabank Power Station Generation by Generating Unit",
        x = "Date",
        y = "Generation (MWh)",
        color = "Generating Unit"
    ) +

    theme_minimal()


total_gwh_2024 <- dbGetQuery(
    con_env,
    "SELECT sum(generation_mwh) / 1000 AS generation_gwh FROM seabank_tbl"
) |>
    pull()


dbGetQuery(
    con_env,
    "SELECT year(min(halfhourendtime)) FROM seabank_tbl"
) |>
    pull()

con_env %>% dbDisconnect()

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "anthropic==0.69.0",
#     "duckdb==1.3.2",
#     "marimo",
#     "openai==2.2.0",
#     "polars==1.34.0",
#     "pyarrow==21.0.0",
#     "sqlglot==27.25.2",
# ]
# ///

import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _():
    import duckdb

    DATABASE_URL = "../mca-data/data/ca_epc.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=True)
    return (engine,)


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        INSTALL SPATIAL;
        LOAD SPATIAL;
        """
    )
    return


@app.cell
def _(engine, ghg_emissions_tbl, mo):
    emissions = mo.sql(
        f"""
        SELECT * FROM ghg_emissions_tbl
        """,
        engine=engine
    )
    return (emissions,)


@app.cell
def _(emissions):
    # Find the latest year in the emissions dataset
    latest_year = emissions["calendar_year"].max()
    latest_year
    return (latest_year,)


@app.cell
def _(emissions, latest_year, pl):
    # Check if 2023 exists in the dataset
    if 2023 in emissions["calendar_year"].unique():
        target_year = 2023
    else:
        # Use the latest year available (which we determined earlier)
        target_year = latest_year
    
    print(f"Using data from {target_year} (latest available year)")

    # Calculate total territorial emissions by local authority for the target year
    total_emissions_by_authority = (
        emissions
        .filter(pl.col("calendar_year") == target_year)
        .group_by("local_authority")
        .agg(
            total_territorial_emissions=pl.col("territorial_emissions_kt_co2e").sum()
        )
        .sort("total_territorial_emissions", descending=True)
    )

    # Display the local authority with the highest emissions
    highest_emissions = total_emissions_by_authority.head(1)
    highest_emissions
    return


@app.cell
def _(emissions):
    emissions
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r""" """)
    return


@app.cell
def _(engine, ghg_emissions_tbl, mo):
    _df = mo.sql(
        f"""
        -- Explore dataset to find transport sector data
        SELECT DISTINCT la_ghg_sector
        FROM ghg_emissions_tbl
        ORDER BY la_ghg_sector
        """,
        engine=engine
    )
    return


@app.cell
def _(emissions, pl):
    import altair as alt


    # Filter emissions data for Bristol transport sector since 2014
    bristol_transport = (emissions
        .filter(
        (pl.col("local_authority") == "Bristol, City of") &
        (pl.col("la_ghg_sector") == "Transport") &
        (pl.col("calendar_year") >= 2014))
        .group_by("calendar_year")
        .agg(
        territorial_emissions_kt_co2e=pl.col("territorial_emissions_kt_co2e").sum())
        .sort("calendar_year"))

    # Create line chart using Altair
    chart = alt.Chart(bristol_transport).mark_line(
        point=True,
        strokeWidth=3,
        color="#1f77b4"
    ).encode(
        x=alt.X('calendar_year:O', title='Year', axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('territorial_emissions_kt_co2e:Q', title='Emissions (kt CO2e)'),
        tooltip=[
            alt.Tooltip('calendar_year:O', title='Year'),
            alt.Tooltip('territorial_emissions_kt_co2e:Q', title='Emissions (kt CO2e)', format='.2f')
        ]
    ).properties(
        title='Transport Sector Emissions in Bristol (2014-Present)',
        width=700,
        height=400
    ).interactive()

    chart
    return


@app.cell
def _(mo):
    mo.md(f"""
    ## Bristol's Transport Emissions Trend

    The interactive chart above displays the territorial emissions from Bristol's transport sector since 2014, measured in kilotonnes of CO2 equivalent (kt CO2e). 

    ### Key Insights:
    - You can hover over data points to see exact emission values for each year
    - The chart shows the yearly trend in transport-related greenhouse gas emissions
    - This visualization helps identify periods of significant changes in emissions

    Transport is typically one of the largest sources of emissions in urban areas, making it a critical focus for climate action strategies in cities like Bristol.
    """)
    return


if __name__ == "__main__":
    app.run()

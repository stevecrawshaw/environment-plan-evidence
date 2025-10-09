# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "duckdb==1.4.1",
#     "polars==1.34.0",
#     "pyarrow==21.0.0",
# ]
# ///

import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return (mo,)


@app.cell
def _():
    import duckdb

    DATABASE_URL = "data/regional_energy.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=True)
    return (engine,)


@app.cell
def _(engine, mo):
    df = mo.sql(
        f"""
        SELECT * FROM renewable_la_long_tbl
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()

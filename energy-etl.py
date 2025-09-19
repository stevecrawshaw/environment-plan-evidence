# %% [markdown]
# # Regional Energy Data Processing
# This notebook connects to a DuckDB database and processes various datasets related to regional energy, including renewables, EV charging, and consumption statistics.

# %%
# Import the necessary library
import functools
import os

import duckdb

print("libraries imported.")
# %%

os.remove("data/regional_energy.duckdb") if os.path.exists(
    "data/regional_energy.duckdb"
) else None

# %%
# Connect to a DuckDB database file and load extensions
# The database file will be created if it doesn't exist
con = duckdb.connect("data/regional_energy.duckdb")

# Load required extensions for reading remote files and handling spatial data
con.sql("LOAD HTTPFS;")
con.sql("LOAD SPATIAL;")

print("✅ Successfully connected to DuckDB and loaded extensions.")

# %% [markdown]
# ---
# ## 1. Renewable Energy Planning Database (REPD)
# This table contains data on renewable energy projects across the UK. We also create a spatial geometry point for each site.

# %%
repd_sql = """
CREATE OR REPLACE TABLE repd_tbl AS
SELECT *,
    ST_Point(xcoordinate, ycoordinate)
    .ST_Transform('EPSG:27700', 'EPSG:4326', always_xy := true) geometry
FROM read_csv('data/repd-q2-jul-2025.csv', normalize_names=true, ignore_errors=true);
"""
con.sql(repd_sql)
print("✅ Created table: repd_tbl")

# Display a sample of the created table
con.table("repd_tbl").limit(5).pl()

# %% [markdown]
# ---
# ## 2. Electric Vehicle (EV) Charging Point Data
# We create two tables: one for the total number of public chargers by local authority, and another for the number of chargers per 100,000 people.

# %%
# Table for all public chargers by speed
ev_chargepoints_sql = """
CREATE OR REPLACE TABLE ev_chargepoints_all_speeds_uk_la_tbl AS
SELECT local_authority_region_code, local_authority_region_name, apr25
FROM read_xlsx('data/electric-vehicle-public-charging-infrastructure-statistics-april-2025.xlsx',
sheet='1a', range = 'A3:Y436', normalize_names=true, ignore_errors=true)
WHERE local_authority_region_code LIKE 'E0%' AND apr25 IS NOT NULL;
"""
con.sql(ev_chargepoints_sql)
print("✅ Created table: ev_chargepoints_all_speeds_uk_la_tbl")
con.table("ev_chargepoints_all_speeds_uk_la_tbl").limit(5).pl()

# %%
# Table for public chargers per 100,000 population
ev_chargepoints_per_cap_sql = """
CREATE OR REPLACE TABLE ev_chargepoints_all_speeds_uk_la_per_cap_tbl AS
SELECT local_authority_region_code_note_5 AS local_authority_region_code,
       local_authority_region_name,
       apr25
FROM read_xlsx('data/electric-vehicle-public-charging-infrastructure-statistics-april-2025.xlsx',
sheet='2a', range = 'A3:Y436', normalize_names=true, ignore_errors=true)
WHERE local_authority_region_code_note_5 LIKE 'E0%' AND apr25 IS NOT NULL;
"""
con.sql(ev_chargepoints_per_cap_sql)
print("✅ Created table: ev_chargepoints_all_speeds_uk_la_per_cap_tbl")
con.table("ev_chargepoints_all_speeds_uk_la_per_cap_tbl").limit(5).pl()


# %% [markdown]
# ---
# ## 3. South West Local Authority Boundaries
# This table ingests GeoJSON data to define the geographical boundaries of local authorities in the South West of England.

# %%
sw_la_sql = """
CREATE OR REPLACE TABLE sw_la_tbl AS
FROM ST_Read('https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/local-authorities-districts-south-west-england/exports/geojson?lang=en&timezone=Europe%2FLondon');
"""
con.sql(sw_la_sql)
print("✅ Created table: sw_la_tbl")
con.table("sw_la_tbl").limit(5).pl()

# %% [markdown]
# ---
# ## 4. EV Registrations
# This table processes data on vehicle registrations by fuel type at the LSOA (Lower Layer Super Output Area) level.

# %%
ev_reg_sql = """
CREATE OR REPLACE TABLE ev_reg_lsoa11_all_tbl AS
SELECT lsoa11cd, lsoa11nm, fuel, _2025_q1 AS q1_2025_count
FROM read_csv('data/df_VEH0135.csv', normalize_names=true, ignore_errors=true)
WHERE q1_2025_count != '[c]' AND (fuel = 'Battery electric' OR fuel LIKE 'Plug%');
"""
con.sql(ev_reg_sql)
print("✅ Created table: ev_reg_lsoa11_all_tbl")
con.table("ev_reg_lsoa11_all_tbl").limit(5).pl()


# %% [markdown]
# ---
# ## 5. Subnational Electricity Consumption
# This table contains the latest (2023) electricity consumption data for local authorities.

# %%

# %%


def concat_sheets(yrs: list[int], path: str, con: duckdb.DuckDBPyConnection):
    """
    Description:
    Function to extract the data from sheets in an excel file which are named by year.
    The format of the data changed in 2011 so data are just collected from 2012 onwards.
    Only data for LA's are collected.

    Returns:
    A list of duckDB relation objects
    """

    # Create a list of relations, one for each year/sheet
    relations_list = [
        con.sql(f"""
            SELECT *
            FROM read_xlsx('{path}', header=true, normalize_names=true, sheet='{yr}', range='A5:X374')
            WHERE code LIKE '%E0%';
        """)
        for yr in yrs
    ]

    # Union all relations in the list at once
    if not relations_list:
        return None  # Or handle as you see fit

    concatenated_relation = functools.reduce(
        lambda r1, r2: r1.union(r2), relations_list
    )
    return concatenated_relation


# %%
yrs = range(2012, 2024)
# yrs = [2012]
path = "data/Subnational_electricity_consumption_statistics_2005-2023.xlsx"


# %%
electricity_la_tbl = concat_sheets(yrs, path, con)
electricity_la_tbl.create("electricity_la_tbl")
print("✅ Created table: electricity_la_tbl")
# %%

# %%

# %% [markdown]
# ---
# ## 6. Fuel Poverty Statistics
# This table processes fuel poverty data by local authority, casting numeric columns to the correct data type.

# %%
fuel_poverty_sql = """
CREATE OR REPLACE TABLE fuel_poverty_2023_lsoa21_tbl AS
SELECT * FROM read_xlsx('data/Sub-regional_fuel_poverty_statistics_2023.xlsx',
                range = 'A3:H33758',
                sheet='Table 4',
                normalize_names=true);
"""
con.sql(fuel_poverty_sql)
print("✅ Created table: fuel_poverty_2023_lsoa21_tbl")
con.table("fuel_poverty_2023_lsoa21_tbl").limit(5)


# %% [markdown]
# ---
# ## 7. LSOA to Local Authority Lookup
# A utility table to map 2011 LSOAs to their corresponding 2021 local authorities.

# %%
lsoa_lookup_sql = """
CREATE OR REPLACE TABLE lsoa11_la_lookup_tbls AS
SELECT lsoa11cd, lsoa11nm, ctyua21cd AS lad_code, ctyua21nm AS lad_name
FROM read_xlsx('data/LSOA11_UTLA21_EW_LU.xlsx', normalize_names=true);
"""
con.sql(lsoa_lookup_sql)
print("✅ Created table: lsoa11_la_lookup_tbls")
con.table("lsoa11_la_lookup_tbls").limit(5)


# %% [markdown]
# ---
# ## 8. Emissions and EPC Data
# These cells attach to another DuckDB database to import emissions data and also process EPC (Energy Performance Certificate) data for the West of England Combined Authority (WECA).

# %%
# Attach to an external DuckDB database to import emissions data
con.sql("ATTACH '../mca-data/data/ca_epc.duckdb';")
con.sql("CREATE OR REPLACE TABLE emissions_tbl AS SELECT * FROM ca_epc.emissions_tbl;")
con.sql(
    "CREATE OR REPLACE TABLE epc_domestic_cauth_tbl AS FROM ca_epc.epc_domestic_vw;"
)
con.sql("DETACH ca_epc;")
print("✅ Created table: emissions_tbl")
print("✅ Created table: epc_domestic_cauth_tbl")


# %% [markdown]
# ---
# ## 9. Boundary and Renewables Data
# These tables define the Local Enterprise Partnership (LEP) boundary and ingest a pre-built dataset of renewable energy sites.

# %%
# Get the LEP boundary
lep_boundary_sql = """
CREATE OR REPLACE TABLE lep_boundary_tbl AS
FROM ST_Read('https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/lep-boundary/exports/fgb?lang=en&timezone=Europe%2FLondon');
"""
con.sql(lep_boundary_sql)
print("✅ Created table: lep_boundary_tbl")
con.table("lep_boundary_tbl").limit(5)

# %%
# Get renewables generation, capacity, and sites by LA
uk_renewables_sql = """
CREATE OR REPLACE TABLE uk_renewables_tbl AS
FROM read_csv('data/all_renewables_tbl.csv');
"""
con.sql(uk_renewables_sql)
print("✅ Created table: uk_renewables_tbl")
con.table("uk_renewables_tbl").limit(5)

# %% [markdown]
# ---
# ## 10. Carbon Intensity Data
# Creates tables for regional carbon intensity forecasts and their corresponding categories.

# %%
# Get regional carbon intensity forecast data
regional_carbon_sql = """
CREATE OR REPLACE TABLE regional_carbon_intensity_tbl AS
SELECT * FROM read_csv('data/regional_carbon_intensity.csv', normalize_names = true);
"""
con.sql(regional_carbon_sql)
print("✅ Created table: regional_carbon_intensity_tbl")
con.table("regional_carbon_intensity_tbl").limit(5).pl()

# %%
# Get the defined categories for carbon intensity levels
carbon_categories_sql = """
CREATE OR REPLACE TABLE carbon_intensity_categories_tbl AS
SELECT * EXCLUDE(very_high_upper_limit)
FROM read_csv('data/carbon_intensity_categories.csv');
"""
con.sql(carbon_categories_sql)
print("✅ Created table: carbon_intensity_categories_tbl")
con.table("carbon_intensity_categories_tbl").limit(5)


# %% [markdown]
# ---
# ## 11. Finalization
# Show all tables created in the database and close the connection.

# %%
print("All tables have been created successfully.")
print("Final list of tables in the database:")
con.sql("SHOW TABLES;").show()

# %%
# Close the database connection
con.close()
print("🛑 Database connection closed.")

# %%

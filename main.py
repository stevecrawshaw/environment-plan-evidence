# main.py

import os
import sys

import duckdb

from queries import TABLE_CREATION_QUERIES
from utils import check_source_data, concat_sheets

# --- Configuration ---
DB_FILE = "data/regional_energy.duckdb"
EXTERNAL_DB_PATH = "../mca-data/data/ca_epc.duckdb"
VEHICLE_DATA_TIME_PERIOD = "_2025_q1"  # Current time period for vehicle data

REQUIRED_FILES = [
    "data/repd-q2-jul-2025.csv",
    "data/electric-vehicle-public-charging-infrastructure-statistics-april-2025.xlsx",
    "data/df_VEH0125.csv",
    "data/df_VEH0135.csv",
    "data/df_VEH0145.csv",
    "data/Subnational_electricity_consumption_statistics_2005-2023.xlsx",
    "data/Sub-regional_fuel_poverty_statistics_2023.xlsx",
    "data/LSOA11_UTLA21_EW_LU.xlsx",
    "data/all_renewables_tbl.csv",
    "data/regional_carbon_intensity.csv",
    "data/carbon_intensity_categories.csv",
    "data/tra8901-miles-by-local-authority.xlsx",
    EXTERNAL_DB_PATH,
]


# --- Main Execution ---
def main():
    """Main function to run the ETL process."""
    # 1. Check for source data before doing anything else
    if not check_source_data(REQUIRED_FILES):
        sys.exit("ETL process aborted due to missing files.")

    # Clean up previous database file if it exists
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"🧹 Removed old database file: {DB_FILE}")

    con = None  # Initialize connection to None
    try:
        # 2. Connect to DuckDB and start a transaction
        con = duckdb.connect(DB_FILE)
        print(f"✅ Successfully connected to DuckDB at '{DB_FILE}'")

        # All subsequent operations are part of a single transaction
        print("\n▶️  Starting transaction to create all tables...")
        con.begin()

        # Load required extensions
        con.sql("LOAD HTTPFS;")
        con.sql("LOAD SPATIAL;")
        print("✅ Loaded HTTPFS and SPATIAL extensions.")

        # 3. Execute all standard "CREATE TABLE" queries
        vehicle_table_names = {
            "veh0135_latest_tbl",
            "veh0145_latest_tbl",
            "veh0125_latest_tbl",
        }

        for query_info in TABLE_CREATION_QUERIES:
            table_name = query_info["name"]
            sql_query = query_info["sql"]

            # Handle parameterized vehicle queries
            if table_name in vehicle_table_names:
                sql_query = sql_query.format(time_period=VEHICLE_DATA_TIME_PERIOD)

            con.sql(sql_query)
            print(f"  - Successfully executed query for table: {table_name}")

        # 4. Handle special table creations
        # Subnational electricity consumption
        elec_yrs = list(range(2012, 2024))
        elec_path = "data/Subnational_electricity_consumption_statistics_2005-2023.xlsx"
        electricity_la_relation = concat_sheets(elec_yrs, elec_path, con)
        electricity_la_relation.create("electricity_la_tbl")
        print("  - Successfully created table: electricity_la_tbl")

        # Emissions and EPC data from external DB
        con.sql(f"ATTACH '{EXTERNAL_DB_PATH}' (READ_ONLY);")
        con.sql(
            "CREATE OR REPLACE TABLE emissions_tbl AS SELECT * FROM ca_epc.emissions_tbl;"
        )
        con.sql(
            "CREATE OR REPLACE TABLE epc_domestic_cauth_tbl AS FROM ca_epc.epc_domestic_vw;"
        )
        con.sql("DETACH ca_epc;")
        print(
            "  - Successfully created tables from external DB: emissions_tbl, epc_domestic_cauth_tbl"
        )

        # 5. Commit the transaction if all steps succeed
        con.commit()
        print("\n✅ Transaction committed successfully! All tables are created.")

        # Final verification
        print("\nFinal list of tables in the database:")
        con.sql("SHOW TABLES;").show()

    except duckdb.Error as e:
        print(f"\n❌ DATABASE ERROR: {e}")
        if con:
            print("▶️  Rolling back transaction...")
            con.rollback()
            print("🛑 Transaction has been rolled back. No changes were saved.")
        sys.exit("ETL process failed.")

    except Exception as e:
        print(f"\n❌ AN UNEXPECTED ERROR OCCURRED: {e}")
        if con:
            print("▶️  Rolling back transaction due to unexpected error...")
            con.rollback()
            print("🛑 Transaction has been rolled back.")
        sys.exit("ETL process failed.")

    finally:
        # 6. Close the database connection
        if con:
            con.close()
            print("\n🛑 Database connection closed.")


if __name__ == "__main__":
    main()

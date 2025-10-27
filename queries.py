# queries.py

"""
This module stores all the SQL queries and table creation commands
for the energy data processing pipeline.
"""

# Macro definitions
MACRO_DEFINITIONS = [
    {
        "name": "str_to_sentence_macro",
        "sql": """
            CREATE OR REPLACE MACRO str_to_sentence(str) AS 
            upper(str[1]) || (str[2:]).replace('_', ' ');
        """,
    },
]

TABLE_CREATION_QUERIES = [
    {
        "name": "repd_tbl",
        "sql": """
            CREATE OR REPLACE TABLE repd_tbl AS
            SELECT *,
                ST_Point(xcoordinate, ycoordinate)
                .ST_Transform('EPSG:27700', 'EPSG:4326', always_xy := true) geometry
            FROM read_csv('data/repd-q2-jul-2025.csv', normalize_names=true, ignore_errors=true);
        """,
    },
    {
        "name": "ev_chargepoints_all_speeds_uk_la_tbl",
        "sql": """
            CREATE OR REPLACE TABLE ev_chargepoints_all_speeds_uk_la_tbl AS
            WITH up_chargepoints_raw AS
            (UNPIVOT
            (FROM read_sheet('data/electric-vehicle-public-charging-infrastructure-statistics-april-2025.ods',
            sheet='1a', range = 'A3:Y436', analyze_rows = 400, error_as_null = true))
            ON COLUMNS(* EXCLUDE ("Local authority / region code", "Local authority / region name"))
            INTO
            name q_y
            VALUE installs)
            SELECT "Local authority / region code" la_region_code,
             "Local authority / region name" la_region_name,
             strptime(q_y[1:4] || '20' || q_y[5:6], '%b-%Y') quarter_ending,
             if(installs[1] = '[', NULL, CAST(installs AS INTEGER)) installs_clean
            FROM up_chargepoints_raw
            WHERE la_region_code LIKE 'E0%' AND installs_clean IS NOT NULL;
        """,
    },
    {
        "name": "ev_chargepoints_all_speeds_uk_la_per_cap_tbl",
        "sql": """
            CREATE OR REPLACE TABLE ev_chargepoints_all_speeds_uk_la_per_cap_tbl AS
            WITH up_chargepoints_raw AS
            (UNPIVOT
            (FROM read_sheet('data/electric-vehicle-public-charging-infrastructure-statistics-april-2025.ods',
            sheet='2a', range = 'A3:Y436', analyze_rows = 400, error_as_null = true))
            ON COLUMNS('^[A-Z][a-z]{2}-[2-9]{2}.*')
            INTO
            name q_y
            VALUE cp_100k)
            SELECT "Local authority / region code 
[Note 5]" la_region_code,
             "Local authority / region name" la_region_name,
             strptime(q_y[1:4] || '20' || q_y[5:6], '%b-%Y') quarter_ending,
             if(cp_100k[1] = '[', NULL, CAST(cp_100k AS INTEGER)) cp_100k_clean
            FROM up_chargepoints_raw
            WHERE la_region_code LIKE 'E0%' AND cp_100k_clean IS NOT NULL;
        """,
    },
    {
        "name": "sw_la_tbl",
        "sql": """
            CREATE OR REPLACE TABLE sw_la_tbl AS
            FROM ST_Read('https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/local-authorities-districts-south-west-england/exports/geojson?lang=en&timezone=Europe%2FLondon');
        """,
    },
    {
        "name": "ev_reg_lsoa11_all_tbl",
        "sql": """
            CREATE OR REPLACE TABLE ev_reg_lsoa11_all_tbl AS
            SELECT lsoa11cd, lsoa11nm, fuel, _2025_q1 AS _count
            FROM read_csv('data/df_VEH0135.csv', normalize_names=true, ignore_errors=true)
            WHERE _count != '[c]' AND (fuel = 'Battery electric' OR fuel LIKE 'Plug%');
        """,
    },
    {
        "name": "fuel_poverty_2023_lsoa21_tbl",
        "sql": """
            CREATE OR REPLACE TABLE fuel_poverty_2023_lsoa21_tbl AS
            SELECT * FROM read_xlsx('data/Sub-regional_fuel_poverty_statistics_2023.xlsx',
                            range = 'A3:H33758',
                            sheet='Table 4',
                            normalize_names=true);
        """,
    },
    {
        "name": "lsoa11_la_lookup_tbls",
        "sql": """
            CREATE OR REPLACE TABLE lsoa11_la_lookup_tbls AS
            SELECT lsoa11cd, lsoa11nm, ctyua21cd AS lad_code, ctyua21nm AS lad_name
            FROM read_xlsx('data/LSOA11_UTLA21_EW_LU.xlsx', normalize_names=true);
        """,
    },
    # Note: External DB attachment is handled as a separate step in the main script
    {
        "name": "lep_boundary_tbl",
        "sql": """
            CREATE OR REPLACE TABLE lep_boundary_tbl AS
            FROM ST_Read('https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/lep-boundary/exports/fgb?lang=en&timezone=Europe%2FLondon');
        """,
    },
    {
        "name": "uk_renewables_tbl",
        "sql": """
            CREATE OR REPLACE TABLE uk_renewables_tbl AS
            FROM read_csv('data/all_renewables_tbl.csv');
        """,
    },
    {
        "name": "regional_carbon_intensity_tbl",
        "sql": """
            CREATE OR REPLACE TABLE regional_carbon_intensity_tbl AS
            SELECT * FROM read_csv('data/regional_carbon_intensity.csv', normalize_names = true);
        """,
    },
    {
        "name": "carbon_intensity_categories_tbl",
        "sql": """
            CREATE OR REPLACE TABLE carbon_intensity_categories_tbl AS
            SELECT * EXCLUDE(very_high_upper_limit)
            FROM read_csv('data/carbon_intensity_categories.csv');
        """,
    },
    {
        "name": "veh0135_latest_tbl",
        "sql": """
            CREATE OR REPLACE TABLE veh0135_latest_tbl AS
            SELECT LSOA11CD, LSOA11NM, Fuel, "{time_period}"::INTEGER AS "{time_period}"
            FROM read_csv('data/df_VEH0135.csv', strict_mode=false, normalize_names=true)
            WHERE "{time_period}"[0:1] != '['
            AND fuel != 'Total'
            AND LSOA11CD[0:1] = 'E';
        """,
    },
    {
        "name": "veh0145_latest_tbl",
        "sql": """
            CREATE OR REPLACE TABLE veh0145_latest_tbl AS
            SELECT LSOA11CD, LSOA11NM, Fuel, "{time_period}"::INTEGER AS "{time_period}" 
            FROM read_csv('data/df_VEH0145.csv', strict_mode=false, normalize_names=true)
            WHERE "{time_period}"[0:1] != '['
            AND fuel != 'Total'
            AND LSOA11CD[0:1] = 'E';
        """,
    },
    {
        "name": "veh0125_latest_tbl",
        "sql": """
            CREATE OR REPLACE TABLE veh0125_latest_tbl AS
            SELECT LSOA11CD, LSOA11NM, BodyType, Keepership, LicenceStatus, "{time_period}"::INTEGER AS "{time_period}"
            FROM read_csv('data/df_VEH0125.csv', strict_mode=false, normalize_names=true)
            WHERE "{time_period}"[0:1] != '[' 
            AND bodytype != 'Total'
            AND keepership != 'Total'
            AND licencestatus != 'Total'
            AND LSOA11CD[0:1] = 'E';
        """,
    },
    {
        "name": "vehicle_mileage_la_tbl",
        "sql": """
            CREATE OR REPLACE TABLE vehicle_mileage_la_tbl AS
            SELECT * REPLACE(year[2:5]::INTEGER AS year,
            if(mileage_millions[1] = '[',
            NULL,
            mileage_millions::DOUBLE) AS mileage_millions)
            FROM
            (UNPIVOT
            (SELECT * EXCLUDE(notes, units, _)
            FROM read_xlsx('data/tra8901-miles-by-local-authority.xlsx',
                            sheet='TRA8901',
                            range='A5:AM240',
                            normalize_names=true,
                            all_varchar=true,
                            header=true
                            ) 
            WHERE local_authority_or_region_code[0:2] = 'E0')
            ON COLUMNS('\\d$')
            INTO
            NAME "year"
            VALUE mileage_millions
            );
        """,
    },
    {
        "name": "fuel_sector_lookup_tbl",
        "sql": """CREATE OR REPLACE TABLE fuel_sector_lookup_tbl AS
                SELECT 
                    str_to_sentence(fuel) fuel,
                    str_to_sentence(sector) sector,
                    original_string fuel_sector
                FROM read_csv('fuel_sector.csv');""",
    },
    {
        "name": "energy_la_year_fuel_sector_long_vw",
        "sql": """
            CREATE OR REPLACE VIEW energy_la_year_fuel_sector_long_vw AS
            SELECT
              el.country_or_region,
              el.code ladcd,
              el.local_authority ladnm,
              el.GTOE gigatonnes_oil_equivalent,
              el.calendar_year,
              fl.fuel,
              fl.sector
            FROM energy_la_long_tbl el
            JOIN fuel_sector_lookup_tbl fl
            USING(fuel_sector);""",
    },
]

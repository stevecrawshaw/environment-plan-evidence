cp ~/weca_postgres.duckdb_secret ~/.duckdb/stored_secrets

duckdb
LOAD SPATIAL;

-- renewable is defined differently in DUKES compared to 
-- Renewable_electricity_by_local_authority_2014_-_2023.xlsx
-- it doesn't include EFW in DUKES

CREATE OR REPLACE TABLE raw_dukes_tbl AS
SELECT * FROM read_xlsx('data/DUKES_5.11.xlsx',
                         sheet = '5.11 Full list',
                         range = 'A6:Q1375',
                         all_varchar = true,
                         normalize_names = true);
DESCRIBE raw_dukes_tbl;

-- get latest year for use in query
SET VARIABLE max_year = (SELECT max(year) FROM read_csv('data/all_renewables_tbl.csv'));
SELECT getvariable('max_year');

-- get the CAPACITY data from Renewable_electricity_by_local_authority_2014_-_2023.xlsx
-- which has been pre - processed in R
-- for just the latest year and the LEP authorities
CREATE OR REPLACE TABLE raw_latest_year_lep_wide_tbl AS
SELECT photovoltaics,
        onshore_wind,
        hydro,
        anaerobic_digestion,
        offshore_wind,
        wave_tidal,
        sewage_gas,
        landfill_gas,
        municipal_solid_waste,
        animal_biomass,
        plant_biomass,
        cofiring        
        FROM read_csv('data/all_renewables_tbl.csv', all_varchar = true)
        WHERE "year" = getvariable('max_year') AND 
        local_authority_code IN ('E06000022', 'E06000023', 'E06000024', 'E06000025')
        AND measure = 'capacity_mw';

-- Unpivot to allow for filtering and casting and addition
CREATE OR REPLACE TABLE latest_year_lep_long_tbl AS
UNPIVOT raw_latest_year_lep_wide_tbl 
ON *
INTO
name technology
value capacity;
-- derive total capacity and set as a variable
SET VARIABLE latest_year_total_renewable_capacity = 
(SELECT SUM(capacity::DOUBLE) total_capacity
FROM latest_year_lep_long_tbl
WHERE capacity != 'NA');

SELECT getvariable('latest_year_total_renewable_capacity');
-- ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);

-- SELECT * FROM weca_postgres.information_schema.tables;

-- CREATE OR REPLACE TABLE lep_boundary_tbl AS
-- SELECT ST_GeomFromWKB(shape).ST_Transform('EPSG:27700', 'EPSG:4326') geometry
-- FROM weca_postgres.os.bdline_ua_lep_diss;


-- The ODS sourced LEP boundary isn't working as a polygon?
-- use the WECA GIS one
CREATE OR REPLACE TABLE lep_boundary_tbl AS
SELECT * FROM ST_Read('../opendatasoft/data/lep_boundary.geojson');


COPY
(SELECT
"type"
, technology
, primary_fuel
, installedcapacity_mw::FLOAT capacity
, postcode
, ST_Transform(ST_Point(xcoordinate::DOUBLE, ycoordinate::DOUBLE), 'EPSG:27700', 'EPSG:4326').ST_Affine(0, 1, 1, 0, 0, 0) geom
FROM raw_dukes_tbl)
TO 'data/power_generators.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- table of all generators within the LEP using sptial join on LEP polygon from weca_postgres
CREATE OR REPLACE TABLE lep_generators_tbl AS
WITH cte_geom AS
(SELECT
"type"
, technology
, primary_fuel
, installedcapacity_mw::FLOAT capacity
, postcode
, ST_Transform(ST_Point(xcoordinate::DOUBLE, ycoordinate::DOUBLE), 'EPSG:27700', 'EPSG:4326').ST_Affine(0, 1, 1, 0, 0, 0) geom
FROM raw_dukes_tbl)
SELECT c.*
FROM cte_geom c
JOIN lep_boundary_tbl l
ON ST_Within(c.geom, l.geom);

SET VARIABLE fossil_capacity = 
(SELECT SUM(capacity)  AS fossil_capacity
FROM lep_generators_tbl
WHERE technology ILIKE '%fossil%');

SELECT getvariable('fossil_capacity');

CREATE OR REPLACE TABLE renewables_fossil_tbl (category VARCHAR, capacity DOUBLE);

INSERT INTO renewables_fossil_tbl 
VALUES 
('Fossil', getvariable('fossil_capacity')),
('Renewable', getvariable('latest_year_total_renewable_capacity'));

FROM renewables_fossil_tbl;

COPY renewables_fossil_tbl TO 'data/renewables_fossil_capacity_lep_latest_year.csv';

---------------------------------------------------------

SET VARIABLE total_capacity = (SELECT SUM(capacity) FROM lep_generators_tbl);

COPY lep_generators_tbl TO 'data/lep_generators_tbl.csv';

CREATE OR REPLACE TABLE energy_source_lep_summary_tbl AS 
WITH source_type AS
(SELECT if(technology = 'Fossil Fuel', 'Fossil', 'Renewable') "Fuel category", capacity
FROM lep_generators_tbl)
SELECT 
"Fuel category"
, SUM(capacity).round(1) "Installed capacity (MW)"
, ("Installed capacity (MW)" * 100/ getvariable('total_capacity')).round(1) "Proportion of total"
FROM source_type
GROUP BY "Fuel category";

DESCRIBE energy_source_lep_summary_tbl;

FROM energy_source_lep_summary_tbl;

COPY energy_source_lep_summary_tbl TO 'data/energy_source_lep_summary_tbl.csv';

CREATE OR REPLACE TABLE repd_tbl AS 
FROM read_csv('data/repd-q1-apr-2025.csv',
normalize_names=true,
ignore_errors=true);

DESCRIBE repd_tbl;

CREATE OR REPLACE TABLE weca_rep_tbl AS 
SELECT 
site_name
,strptime(operational, '%d/%m/%Y')::DATE AS operational_date
,extract(YEAR FROM operational_date) AS year
,record_last_updated_ddmmyyyy
,technology_type
,storage_type
,installed_capacity_mwelec::FLOAT installed_capacity_mwelec
,share_community_scheme
,development_status
,planning_authority
,county
,post_code
,xcoordinate bng_x
,ycoordinate bng_y
, ST_Transform(ST_Point(xcoordinate, ycoordinate), 'EPSG:27700', 'EPSG:4326') geo_point_2d
FROM repd_tbl
WHERE 
    county IN('South Gloucestershire', 'North Somerset', 'Bristol, City of', 'Bath and North East Somerset', 'Avon')
    AND
    installed_capacity_mwelec IS NOT NULL
    AND 
    development_status_short = 'Operational';


SET VARIABLE start_year = (SELECT MAX("year") - 5 FROM weca_rep_tbl);

SELECT getvariable('start_year');

SELECT 
SUM(installed_capacity_mwelec) capacity_added
FROM weca_rep_tbl
WHERE year >= getvariable('start_year');

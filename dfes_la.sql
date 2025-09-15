duckdb data/regional_energy.duckdb
LOAD HTTPFS;
LOAD SPATIAL;
-- renewable energy planning database

SHOW TABLES;

CREATE OR REPLACE TABLE repd_tbl AS
SELECT *, ST_Point(xcoordinate, ycoordinate).ST_Transform('EPSG:27700', 'EPSG:4326').ST_Affine(0, 1, 1, 0, 0, 0) geometry
FROM read_csv('data/repd-q2-jul-2025.csv', normalize_names=true, ignore_errors=true);

-- COPY repd_tbl TO 'data/repd.geojson' (FORMAT GDAL, DRIVER 'geoJSON');

-- Get ev charging point data - converted manually from ODS to excel
-- all speeds sheet 1a
-- https://www.gov.uk/government/statistics/electric-vehicle-public-charging-infrastructure-statistics-april-2025

-- CREATE OR REPLACE TABLE ev_chargepoints_all_speeds_uk_la_tbl AS
-- SELECT local_authority_region_code, local_authority_region_name, apr25 
-- FROM read_xlsx('data/electric-vehicle-public-charging-infrastructure-statistics-april-2025.xlsx',
-- sheet='1a', range = 'A3:Y436', normalize_names=true, ignore_errors=true)
-- WHERE local_authority_region_code LIKE 'E0%' AND apr25 IS NOT NULL;

CREATE OR REPLACE TABLE ev_chargepoints_all_speeds_uk_la_per_cap_tbl AS
SELECT local_authority_region_code_note_5 local_authority_region_code, local_authority_region_name, apr25 
FROM read_xlsx('data/electric-vehicle-public-charging-infrastructure-statistics-april-2025.xlsx',
sheet='2a', range = 'A3:Y436', normalize_names=true, ignore_errors=true)
WHERE local_authority_region_code_note_5 LIKE 'E0%' AND apr25 IS NOT NULL;

-- get the boundaries and names of SW LA's from open data portal st_read and jeojson gets us a spatial dataset
CREATE OR REPLACE TABLE sw_la_tbl AS FROM ST_Read('https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/local-authorities-districts-south-west-england/exports/geojson?lang=en&timezone=Europe%2FLondon');

DESCRIBE  sw_la_tbl;

-- get EV registrations

CREATE OR REPLACE TABLE ev_reg_lsoa11_all_tbl AS
SELECT lsoa11cd, lsoa11nm, fuel, _2025_q1 q1_2025_count
FROM read_csv('data/df_VEH0135.csv', normalize_names=true, ignore_errors=true)
WHERE q1_2025_count != '[c]' AND (fuel = 'Battery electric' OR fuel LIKE 'Plug%');

-- get subnational electricity consumption

CREATE OR REPLACE TABLE electricity_la_2023_tbl AS
SELECT  * 
FROM read_xlsx('data/Subnational_electricity_consumption_statistics_2005-2023 (1).xlsx',
normalize_names=true, sheet='2023', range='A5:X374')
WHERE code LIKE 'E0%';

-- get fuel poverty stats by LA
-- jiggery pokery necessary as excel parser fails when empty cells encountered at start of file
CREATE OR REPLACE TABLE fuel_poverty_2023_la_tbl AS
WITH initial_fp_cte AS
(SELECT *
FROM read_xlsx('data/Sub-regional_fuel_poverty_statistics_2023.xlsx',
                range = 'A3:G338',
                sheet='Table 2',
                normalize_names=true,
                all_varchar=true)
WHERE county_or_unitary_authority IS NOT NULL)
SELECT
local_authority_code,
nation_or_region,
county_or_unitary_authority,
local_authority_district,
number_of_households::DOUBLE number_of_households,
number_of_households_in_fuel_poverty::DOUBLE number_of_households_in_fuel_poverty,
proportion_of_households_fuel_poor::DOUBLE proportion_of_households_fuel_poor
FROM initial_fp_cte;


-- get 2011 LSOA lookups to LA
CREATE OR REPLACE TABLE sw_lsoa11_la_lookup_tbls AS
SELECT lsoa11cd, lsoa11nm, ctyua21cd lad_code, ctyua21nm lad_name FROM read_xlsx('data/LSOA11_UTLA21_EW_LU.xlsx', normalize_names=true)
WHERE ctyua21cd IN (SELECT list_extract(lad_code, 1) FROM sw_la_tbl);


-- get emissions data from pre - build duckdb database which holds regional (WECA and UK) data

ATTACH '../mca-data/data/ca_epc.duckdb';

CREATE OR REPLACE TABLE emissions_sw_tbl AS
SELECT * 
FROM ca_epc.emissions_tbl ca
WHERE ca.region_country = 'South West';

DETACH ca_epc;

CREATE OR REPLACE TABLE weca_epc_tbl AS
SELECT * 
FROM read_csv('https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/lep-epc-domestic-point/exports/csv?select=count%28%2A%29%20as%20count%2Cladnm%2Ccurrent_energy_rating%2Cconstruction_epoch&group_by=ladnm%2Ccurrent_energy_rating%2Cconstruction_epoch&limit=-1&timezone=UTC&use_labels=false&compressed=false&epsg=4326');



-- get the distribution future energy scenatios (DFES)
-- manually downloaded zip file from https://connecteddata.nationalgrid.co.uk/dataset/dfes
CREATE OR REPLACE TABLE dfes_la_tbl AS
SELECT sw.lad_name, list_extract(sw.lad_code, 1) lad_code, df.*
FROM read_csv('data/dfes_volume_projections_LA.csv') df
INNER JOIN sw_la_tbl sw
ON df.local_authority = sw.lad_name;

-- correcting the base year given as 0 to 2024
UPDATE dfes_la_tbl
SET year = 2024
WHERE year = 0;

-- now lets look at carbon intensity
CREATE OR REPLACE TABLE regional_carbon_intensity_tbl AS
SELECT * FROM read_csv('data/regional_carbon_intensity.csv', normalize_names = true);

-- now get the categories which have been derived from the table here:
-- https://www.neso.energy/data-portal/regional-carbon-intensity-forecast
-- and digitised using gemini https://g.co/gemini/share/80e17111f59c

CREATE OR REPLACE TABLE carbon_intensity_categories_tbl AS 
SELECT * EXCLUDE(very_high_upper_limit) FROM read_csv('data/carbon_intensity_categories.csv');

SHOW TABLES;

.quit
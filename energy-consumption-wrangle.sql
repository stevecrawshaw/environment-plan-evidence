duckdb

-- CREATE A MACRO TO FORMAT STRINGS
CREATE OR REPLACE MACRO str_to_sentence(str VARCHAR) AS upper(str[1]) || (str[2:]).replace('_', ' ');
-- TESTING THE QUERY FOR A SINGLE YEAR
SET VARIABLE year = '2023';

-- CREATE OR REPLACE TABLE energy_la_long_tbl AS
WITH raw_energy_la_tbl AS
(SELECT COLUMNS(* EXCLUDE(Notes)) FROM read_xlsx('data/Subnational_total_final_energy_consumption_2005_2023.xlsx',
normalize_names=true,
range='A6:AJ391',
sheet = getvariable('year'),
header=true,
all_varchar=true)
WHERE code LIKE 'E0%')
UNPIVOT
(SELECT
    -- 1. Select and rename columns ending in '_note...'
    COLUMNS('(.*)_note.*$') AS '\1',

    -- 2. Select all other columns that do not match the pattern
    COLUMNS(c -> NOT REGEXP_MATCHES(c, '_note.*$')),
    calendar_year: getvariable('year')::INTEGER

FROM raw_energy_la_tbl)
ON COLUMNS(* EXCLUDE(country_or_region, local_authority, code, calendar_year))
INTO
    NAME fuel_sector
    VALUE GTOE;

-- DESCRIBE energy_la_long_tbl;


-- This following sql can be executed on the unioned table of all years
SELECT * EXCLUDE(GTOE), if(GTOE[1] = '[', NULL, GTOE)::FLOAT GTOE
FROM energy_la_long_tbl;


-- IGNORE THIS CODE BELOW
CREATE OR REPLACE TABLE fuel_sector_lookup_tbl AS
SELECT 
    str_to_sentence(fuel) fuel,
    str_to_sentence(sector) sector,
    original_string fuel_sector
FROM read_csv('fuel_sector.csv');


-- -- CREATE A VIEW IN THE DATABASE TO JOIN THE LOOKUP TABLE
-- FROM energy_la_long_tbl el
-- JOIN fuel_sector_lookup_tbl fl
-- USING(fuel_sector);
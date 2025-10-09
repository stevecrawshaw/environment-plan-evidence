duckdb
-- script to test extracting data from multiple sheets in an excel file
-- Get data from sheets named 
-- set up variables for dynamic sheet names
SET VARIABLE year = 2024;
-- ' nice inconsistent naming of sheets, some with commas some not'
SET VARIABLE type = 'Generation'; -- or 'Capacity' or 'Generation,' or 'Sites'
SET VARIABLE sep = (SELECT if(getvariable('type') = 'Sites', ' ', ', '));
SET VARIABLE sheet = 'LA - ' || getvariable('type') || getvariable('sep') || getvariable('year');
SELECT getvariable('sheet');


CREATE OR REPLACE TABLE ren_tbl AS
SELECT 
    -- 1. Select and rename columns ending in '_note...'
    COLUMNS('^(.*?)((?:_?note_\d+)+)$') AS '\1',
    -- 2. Select all other columns that do not match the pattern
    COLUMNS(c -> NOT REGEXP_MATCHES(c, '_note.*$')),
    getvariable('year') AS "calendar_year",
    getvariable('type') AS "type"
    -- calendar_year: getvariable('year')::INTEGER
FROM read_xlsx('data/Renewable_electricity_by_local_authority_2014_-_2024.xlsx',
                         sheet = getvariable('sheet'),
                         range = 'A4:R500',
                         all_varchar = true,
                         normalize_names = true)
WHERE local_authority_code LIKE 'E0%';

-- Transform wide to long format for sites data
WITH long_ren_sample_sites_tbl AS
(UNPIVOT
ren_tbl
ON * EXCLUDE (local_authority_code,
              local_authority_name,
              estimated_number_of_households,
              region, country, calendar_year, "type")
INTO
NAME energy_source
VALUE val)
SELECT * EXCLUDE(val), if(val[1] = '[', NULL, val)::INTEGER AS value
FROM long_ren_sample_sites_tbl WHERE value IS NOT NULL;


-- create a view for the renewables dataset
-- EXPORT TO motherduck

../lnrs/./duckdb
ATTACH 'data/regional_energy.duckdb' as re;
ATTACH 'md:';
DROP DATABASE regional_energy CASCADE;
-- md authentication is in the .env variable
CREATE OR REPLACE DATABASE regional_energy FROM re;

DETACH re;
DETACH md;


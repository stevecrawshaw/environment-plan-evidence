duckdb

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
ON COLUMNS('\d$')
INTO
NAME "year"
VALUE mileage_millions
);

duckdb

LOAD rusty_sheet;

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
duckdb
-- now incorporated into main.py and queries.py
CREATE OR REPLACE TABLE veh0135_latest_tbl AS
SELECT LSOA11CD, LSOA11NM, Fuel, "_2025_q1"::INTEGER AS "_2025_q1"
FROM read_csv('data/df_VEH0135.csv', strict_mode=false, normalize_names=true)
WHERE "_2025_q1"[0:1] != '['
AND fuel != 'Total'
AND LSOA11CD[0:1] = 'E';

CREATE OR REPLACE TABLE veh0145_latest_tbl AS
SELECT LSOA11CD, LSOA11NM, Fuel, "_2025_q1"::INTEGER AS "_2025_q1" 
FROM read_csv('data/df_VEH0145.csv', strict_mode=false, normalize_names=true)
WHERE "_2025_q1"[0:1] != '['
AND fuel != 'Total'
AND LSOA11CD[0:1] = 'E';

CREATE OR REPLACE TABLE veh0125_latest_tbl AS
SELECT LSOA11CD, LSOA11NM, BodyType, Keepership, LicenceStatus, "_2025_q1"::INTEGER AS "_2025_q1"
FROM read_csv('data/df_VEH0125.csv', strict_mode=false, normalize_names=true)
WHERE "_2025_q1"[0:1] != '[' 
AND bodytype != 'Total'
AND keepership != 'Total'
AND licencestatus != 'Total'
AND LSOA11CD[0:1] = 'E';

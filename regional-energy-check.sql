duckdb data/regional_energy.duckdb

.tables

DESCRIBE (FROM electricity_la_tbl);

CREATE OR REPLACE TABLE electricity_total_consumption_wide AS(
PIVOT
(SELECT total_consumption_gwh_all_meters, calendar_year as "year"
FROM electricity_la_tbl
WHERE code in ('E06000022', 'E06000023', 'E06000024', 'E06000025'))
ON year
USING SUM(total_consumption_gwh_all_meters));


SELECT ("2023" - "2014") * 100 / "2023" AS change_in_consumption_10_yrs FROM electricity_total_consumption_wide;


FROM energy_la_year_fuel_sector_long_vw;

DESCRIBE energy_la_year_fuel_sector_long_vw;

-- Show all unique fuel sectors after removing the "_note" part
.mode line
SELECT DISTINCT regexp_replace(fuel_sector, '_note.*', '') AS fuel_sector 
FROM energy_la_long_tbl 
ORDER BY fuel_sector;




-- / -o 'data/regional_energy.duckdb'
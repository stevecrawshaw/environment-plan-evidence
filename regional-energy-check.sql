duckdb data/regional_energy.duckdb

.tables

FROM energy_la_year_fuel_sector_long_vw;

DESCRIBE energy_la_year_fuel_sector_long_vw;

-- Show all unique fuel sectors after removing the "_note" part
.mode line
SELECT DISTINCT regexp_replace(fuel_sector, '_note.*', '') AS fuel_sector 
FROM energy_la_long_tbl 
ORDER BY fuel_sector;




-- / -o 'data/regional_energy.duckdb'
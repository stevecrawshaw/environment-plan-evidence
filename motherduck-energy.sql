duckdb
ATTACH 'data/regional_energy.duckdb' AS re;
ATTACH 'md:';
DROP DATABASE regional_energy CASCADE;
-- md authentication is in the .env variable
CREATE OR REPLACE DATABASE regional_energy FROM re;

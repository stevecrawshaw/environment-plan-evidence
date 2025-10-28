duckdb
-- just playing around with loading excel sheets and cleaning up column names
-- can't seem to do this with a list to rename column names directly yet
LOAD rusty_sheet;


SELECT 
column_name.regexp_replace('[^[:alnum:]]', '_', 'g').regexp_replace('_+', '_', 'g').rtrim('_').lcase().list() AS column_name,
FROM 
(DESCRIBE(
SELECT * FROM read_sheet('data/WofE Community Energy Sector datasheet.xlsx',
                            sheet = "Summary & baseline",
                            range = 'A8:I8')));
-- copy the secrets file to the secrets folder (sometimes it needs to be moved - bug)
cp ~/weca_postgres.duckdb_secret ~/.duckdb/stored_secrets

duckdb
LOAD SPATIAL;
LOAD HTTPFS;
-- connect to corp DB VPN on
ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);
-- examine schema
SELECT * FROM weca_postgres.information_schema.tables;

-- test various options for transformations of co - ordinates when converting between BNG (EPSG:27700) and
-- WGS84 (EPSG:4326) - lat and long


-- https://duckdb.org/docs/stable/core_extensions/spatial/functions#st_transform

-- Ingest the data using the ST_Transform() function to change CRS
-- (no need for always_xy := true here)

CREATE OR REPLACE TABLE lep_boundary_tbl AS
SELECT ST_GeomFromWKB(shape).ST_Transform('EPSG:27700', 'EPSG:4326') geometry
FROM weca_postgres.os.bdline_ua_lep_diss;

-- # but for exporting we need to flip the co - ordinate order
-- This syntax produces positionally correct geoJSON. Note the walrus operator for the 
-- always_xy parameter
COPY (
    SELECT 
        -- "Transform" to the same CRS just to apply the axis-order flag
        ST_Transform(geom, 'EPSG:4326', 'EPSG:4326', always_xy := true) AS geometry,
        * EXCLUDE geom
    FROM 
        weca_postgres.os.bdline_ua_lep_diss
) 
TO 'data/exported_boundary_correct_order.geojson' 
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- Using ST_Affine() applies a matrix transformation to the coordinates, which also has the same effect, ie
-- it swaps the order of the coordinates for each vertex
COPY (
    SELECT 
        -- Apply the affine transformation to swap x and y for each vertex
        ST_Affine(geom, 0, 1, 1, 0, 0, 0) AS geometry,
        -- Select any other columns you need
        * EXCLUDE geom
    FROM 
        weca_postgres.os.bdline_ua_lep_diss
) 
TO 'data/exported_boundary_correct_order.geojson' 
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- Reading the geojson export from the Open Data Portal export endpoint
-- https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/lep-boundary/exports/geojson?lang=en&timezone=Europe%2FLondon
-- or geoJSON file renders the layer correctly, without the need for transformation.
FROM ST_Read('data/lep-boundary_ods.geojson');


--  this is from the duckdb guidance on ST_Transform() and relates to using grid files for more 
-- precise conversion between CRSs. 
-- need to download, unzip and reference these files
-- https://www.ordnancesurvey.co.uk/documents/resources/OSTN15-NTv2.zip
-- note the full file path is needed
CREATE OR REPLACE TABLE lep_boundary_precise_tbl AS
SELECT ST_GeomFromWKB(shape).ST_Transform('+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs +nadgrids=C:\\Users\\steve.crawshaw\\OSTN15-NTv2\\OSTN15_NTv2_OSGBtoETRS.gsb +type=crs',
        'EPSG:4326') AS geometry
FROM weca_postgres.os.bdline_ua_lep_diss;


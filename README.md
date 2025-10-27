## Regional Environmental Analysis

This repo contains scripts and data for a few tasks:

1. ETL processes to gather, clean and transform energy related data from various sources. and store in a duckdb database which is pushed to Motherduck.

`ATTACH 'md:_share/regional_energy/996c823f-64d2-4378-8723-6f0f8e357d97';`

- The ETL process is implemented in 3 scripts in python.
    - main.py : Orchestrates the ETL process.
    - queries.py : Contains SQL queries to extract data from source files.
    - utils.py : Utility functions for data cleaning and transformation.
Generally duckdb's python relational API is used for data manipulation.

2. Analysis scripts to perform regional environmental analysis using the cleaned data. and create an analysis report in quarto which is published to quarto - pub. Images are also generated to populate a report. The analysis is implemented using R in a quarto document env-plan-evidence-optimised.qmd which is rendered to HTML and [published on quarto-pub](https://stevecrawshaw.quarto.pub/evidence-base-for-2025-environment-plan/):

# Original Brief:

As discussed, can you start collating data for the Environment Plan? I suggest as set out here

Can you convert the data to graphs

Please save anything into this folder   GHG MW Data

## Net Zero Data Picture
- [Broadly following p 11 and 13](https://www.wmca.org.uk/media/wumiikpt/wm-net-zero-fyp-summary-tech-report.pdf )

## Energy Use 

- Total for the region 
- By local authority area  
- By sector 
- By fuel type (pie) 
- By heating in homes 


## Greenhouse gas emissions ktCO2 
- Total for the region 
- By local authority area  

## Regional 
- Energy consumption and GHG emissions over time 
- Tables in grey on p 13 

## Energy Generation not in the WM report 
- Total energy being generated in the region 
- Total renewable energy generated in the region 
- Total community owned energy generated in the region 
- The above by local authority area? 

All local authority data to include North Somerset 
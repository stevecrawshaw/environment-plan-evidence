## SeaBank Generation
This script collects electricity generation data from the Seabank power station using the # B1610 API endpoint.

# API Endpoint:
Actual Generation Output Per Generation Unit (B1610)
This endpoint provides the actual metered volume output (MWh) per Settlement Period for all BM units (Positive, Negative or zero MWh values).

The settlement period to query must be specified as a date and settlement period. The date must be given in the format yyyy-MM-dd.

a "Settlement Period" is a half-hour (30-minute) block of time.

The B1610 endpoint provides the actual metered generation volume (in MWh) for each of these half-hour periods. The settlement process, which this data is used for, tracks all generation and demand volumes for every half hour of each day. This half-hourly granularity is a standard interval for UK electricity market data.

It returns the specific, total metered volume (in MWh) for each individual half-hour Settlement Period that you query.   

To use your example of getting data for a full day:

You would need to query the API 48 separate times (once for Period=1, once for Period=2, and so on, up to Period=48 for your chosen SettlementDate). "SettlementPeriod must be between 1 and 50 (inclusive) noting SP 49 & 50 will only return data on a 'long day' during clock change"

The generation output returned by this endpoint is the metered volume and not the instantaneous power output as often appears in other specifications.

There are two generating units (bmUnit) at Seabank: T_SEAB-1 and T_SEAB-2.

 GET
 /datasets/B1610
 Actual Generation Output Per Generation Unit (B1610)

 Parameters
 Cancel
 Name	Description
 settlementDate *
 string($date)
 (query)
 2022-10-12

 settlementPeriod *
 integer($int32)
 (query)
 48

 bmUnit
 array<string>
 (query)
 T_SEAB-1
 T_SEAB-2
 format
 string

 (query)
 Response data format. Use json/xml to include metadata.

# json response example
curl -X 'GET' \
'https://data.elexon.co.uk/bmrs/api/v1/datasets/B1610?settlementDate=2022-10-12&settlementPeriod=48&bmUnit=T_SEAB-1&bmUnit=T_SEAB-2&format=json' \
   -H 'accept: text/plain'
# Request URL
https://data.elexon.co.uk/bmrs/api/v1/datasets/B1610?settlementDate=2022-10-12&settlementPeriod=48&bmUnit=T_SEAB-1&bmUnit=T_SEAB-2&format=json

 Server response
 Code	Details
 200	
 Response body
 Download
 {
   "data": [
     {
       "dataset": "B1610",
       "psrType": "Generation",
       "bmUnit": "T_SEAB-1",
       "nationalGridBmUnitId": "SEAB-1",
       "settlementDate": "2022-10-12",
       "settlementPeriod": 48,
       "halfHourEndTime": "2022-10-12T23:00:00",
       "quantity": -2.124
     },
     {
       "dataset": "B1610",
       "psrType": "Generation",
       "bmUnit": "T_SEAB-2",
       "nationalGridBmUnitId": "SEAB-2",
       "settlementDate": "2022-10-12",
       "settlementPeriod": 48,
       "halfHourEndTime": "2022-10-12T23:00:00",
       "quantity": -1.985
     }
   ]
 }
 Response headers
  cache-control: no-store,must-revalidate,no-cache 
  content-type: application/json; charset=utf-8 
  expires: Tue,28 Oct 2025 13:12:08 GMT 
  pragma: no-cache

  ## Task

Write a script in seabank-generation.py that retrieves and displays the actual generation output for both Seabank generating units (T_SEAB-1 and T_SEAB-2) for **every settlement date and settlement period in 2024** using the B1610 API endpoint. 

The script should handle the API request, parse the JSON response, and create a Polars dataframe for the output in a `long` format with a field for the each of the keys in the response. Use the httpx library for making HTTP requests. Do not overload the server, be polite. Use the polars library to store and manipulate the data. Do not use pandas.

Use python best practices, including error handling for the API requests.
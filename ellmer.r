pacman::p_load(
  tidyverse,
  janitor,
  readxl,
  glue,
  arrow,
  duckdb,
  DBI,
  ellmer,
  config
)

api_key = config::get(
  config = "gemini",
  file = "../config.yml",
  value = "apikey"
)

chat <- chat_google_gemini(
  system_prompt = "You are a helpful data analyst assistant focused on accuracy.
  You respond tersely with no additional commentary.",
  api_key = api_key
)

prompt <- "What is the main category of heating for the description .
'Boiler and radiators, electric'
The possible broad categories are:

Gas Central Heating
Oil Central Heating
Community Scheme
Heat pump
Electric Storage Heating
Other

Take your time and be diligent and accurate.
Answer ONLY with one of the categories above."

resp <- chat$chat(prompt)

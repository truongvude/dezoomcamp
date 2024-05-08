CREATE OR REPLACE TABLE deproject36.trips_data_all.materialized_table_green_taxi_data
AS (
  SELECT * FROM `deproject36.trips_data_all.external_table_green_taxi_data`
  )
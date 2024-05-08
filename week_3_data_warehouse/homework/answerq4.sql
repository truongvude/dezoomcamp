CREATE OR REPLACE TABLE `deproject36.trips_data_all.green_taxi_partitioned_table`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `deproject36.trips_data_all.materialized_table_green_taxi_data`
  )
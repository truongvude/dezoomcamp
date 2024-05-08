CREATE EXTERNAL TABLE `deproject36.trips_data_all.external_table_green_taxi_data`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://dezoomcamp_bucket/raw/green_tripdata_*']
    );
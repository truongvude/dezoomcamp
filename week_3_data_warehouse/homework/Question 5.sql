SELECT DISTINCT PULocationID 
FROM `trips_data_all.materialized_table_green_taxi_data`
WHERE lpep_pickup_datetime >= '2022-06-01' 
AND lpep_pickup_datetime <= '2022-06-30'



SELECT DISTINCT PULocationID 
FROM `trips_data_all.green_taxi_partitioned_table`
WHERE lpep_pickup_datetime >= '2022-06-01' 
AND lpep_pickup_datetime <= '2022-06-30'
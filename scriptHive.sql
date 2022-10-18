#Authorities
CREATE TABLE IF NOT EXISTS authorities (local_authority_ons_code STRING, local_authority_id INT, local_authority_name STRING, region_ons_code STRING, region_id INT, region_name STRING)
STORED AS PARQUET;

#roads
CREATE TABLE IF NOT EXISTS roads (road_name STRING, road_type STRING, road_category STRING)
STORED AS PARQUET;


#weather
CREATE TABLE IF NOT EXISTS weather (weather_id INT, weather_condition STRING)
STORED AS PARQUET;


#time
CREATE TABLE IF NOT EXISTS time (time_measured STRING, time_hour INTEGER, time_day INTEGER, time_month INTEGER, time_year INTEGER, time_quarter INTEGER)
STORED AS PARQUET;


#vehicles
CREATE TABLE IF NOT EXISTS vehicles (vehicle_name STRING, vehicle_category STRING)
STORED AS PARQUET;


#facts
CREATE TABLE IF NOT EXISTS facts (local_authority_ons_code STRING, time_measured STRING, road_name STRING, vehicle_name STRING, num_vehicles INTEGER)
STORED AS PARQUET;
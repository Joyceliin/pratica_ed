-- VIEW: nyc_taxi.data_area.v_bronze_yellow_tripdata
CREATE OR REPLACE VIEW `nyc_taxi`.`data_area`.`v_bronze_yellow_tripdata` AS
SELECT * FROM delta.`/Volumes/nyc_taxi/data_area/lake_area/bronze/`;

-- Equivalent TABLE structure from Delta path:
CREATE TABLE `nyc_taxi`.`data_area`.`v_bronze_yellow_tripdata` (
  `vendorid` INT,
  `tpep_pickup_datetime` TIMESTAMP,
  `tpep_dropoff_datetime` TIMESTAMP,
  `passenger_count` INT,
  `trip_distance` DOUBLE,
  `ratecodeid` INT,
  `store_and_fwd_flag` STRING,
  `pulocationid` INT,
  `dolocationid` INT,
  `payment_type` INT,
  `fare_amount` DOUBLE,
  `extra` DOUBLE,
  `mta_tax` DOUBLE,
  `tip_amount` DOUBLE,
  `tolls_amount` DOUBLE,
  `improvement_surcharge` DOUBLE,
  `total_amount` DOUBLE,
  `congestion_surcharge` DOUBLE,
  `airport_fee` DOUBLE,
  `dataset_year` INT,
  `dataset_month` INT,
  `pickup_year` INT,
  `pickup_month` INT
) USING DELTA
LOCATION '/Volumes/nyc_taxi/data_area/lake_area/bronze/';

-- VIEW: nyc_taxi.data_area.v_gold_fact_hour
CREATE OR REPLACE VIEW `nyc_taxi`.`data_area`.`v_gold_fact_hour` AS
SELECT * FROM delta.`/Volumes/nyc_taxi/data_area/lake_area/gold/fact_hour`;

-- Equivalent TABLE structure from Delta path:
CREATE TABLE `nyc_taxi`.`data_area`.`v_gold_fact_hour` (
  `dataset_year` INT,
  `dataset_month` INT,
  `dt` DATE,
  `hour` INT,
  `qtd_viagens` BIGINT,
  `receita_total` DOUBLE,
  `ticket_medio` DOUBLE,
  `dist_media_mi` DOUBLE,
  `duracao_media_min` DOUBLE,
  `velocidade_media_mph` DOUBLE
) USING DELTA
LOCATION '/Volumes/nyc_taxi/data_area/lake_area/gold/fact_hour';

-- VIEW: nyc_taxi.data_area.v_gold_fact_payment
CREATE OR REPLACE VIEW `nyc_taxi`.`data_area`.`v_gold_fact_payment` AS
SELECT * FROM delta.`/Volumes/nyc_taxi/data_area/lake_area/gold/fact_payment`;

-- Equivalent TABLE structure from Delta path:
CREATE TABLE `nyc_taxi`.`data_area`.`v_gold_fact_payment` (
  `dataset_year` INT,
  `dataset_month` INT,
  `payment_type` INT,
  `payment_desc` STRING,
  `qtd_viagens` BIGINT,
  `receita_total` DOUBLE,
  `ticket_medio` DOUBLE,
  `tip_pct_medio` DOUBLE,
  `rev_por_milha_medio` DOUBLE
) USING DELTA
LOCATION '/Volumes/nyc_taxi/data_area/lake_area/gold/fact_payment';

-- VIEW: nyc_taxi.data_area.v_gold_fat_diario
CREATE OR REPLACE VIEW `nyc_taxi`.`data_area`.`v_gold_fat_diario` AS
SELECT * FROM delta.`/Volumes/nyc_taxi/data_area/lake_area/gold/fact_daily`;

-- Equivalent TABLE structure from Delta path:
CREATE TABLE `nyc_taxi`.`data_area`.`v_gold_fat_diario` (
  `dataset_year` INT,
  `dataset_month` INT,
  `dt` DATE,
  `qtd_viagens` BIGINT,
  `receita_total` DOUBLE,
  `ticket_medio` DOUBLE,
  `dist_media_mi` DOUBLE,
  `duracao_media_min` DOUBLE,
  `velocidade_media_mph` DOUBLE,
  `tip_pct_medio` DOUBLE
) USING DELTA
LOCATION '/Volumes/nyc_taxi/data_area/lake_area/gold/fact_daily';

-- VIEW: nyc_taxi.data_area.v_silver_invalid
CREATE OR REPLACE VIEW `nyc_taxi`.`data_area`.`v_silver_invalid` AS
SELECT * FROM delta.`/Volumes/nyc_taxi/data_area/lake_area/rejects/silver_invalid/`;

-- Equivalent TABLE structure from Delta path:
CREATE TABLE `nyc_taxi`.`data_area`.`v_silver_invalid` (
  `vendorid` INT,
  `tpep_pickup_datetime` TIMESTAMP,
  `tpep_dropoff_datetime` TIMESTAMP,
  `passenger_count` INT,
  `trip_distance` DOUBLE,
  `ratecodeid` INT,
  `store_and_fwd_flag` STRING,
  `pulocationid` INT,
  `dolocationid` INT,
  `payment_type` INT,
  `fare_amount` DOUBLE,
  `extra` DOUBLE,
  `mta_tax` DOUBLE,
  `tip_amount` DOUBLE,
  `tolls_amount` DOUBLE,
  `improvement_surcharge` DOUBLE,
  `total_amount` DOUBLE,
  `congestion_surcharge` DOUBLE,
  `airport_fee` DOUBLE,
  `dataset_year` INT,
  `dataset_month` INT,
  `pickup_year` INT,
  `pickup_month` INT,
  `is_pickup_outside_dataset` INT
) USING DELTA
LOCATION '/Volumes/nyc_taxi/data_area/lake_area/rejects/silver_invalid/';

-- VIEW: nyc_taxi.data_area.v_silver_yellow_tripdata
CREATE OR REPLACE VIEW `nyc_taxi`.`data_area`.`v_silver_yellow_tripdata` AS
SELECT * FROM delta.`/Volumes/nyc_taxi/data_area/lake_area/silver/`;

-- Equivalent TABLE structure from Delta path:
CREATE TABLE `nyc_taxi`.`data_area`.`v_silver_yellow_tripdata` (
  `vendorid` INT,
  `tpep_pickup_datetime` TIMESTAMP,
  `tpep_dropoff_datetime` TIMESTAMP,
  `passenger_count` INT,
  `trip_distance` DOUBLE,
  `ratecodeid` INT,
  `store_and_fwd_flag` STRING,
  `pulocationid` INT,
  `dolocationid` INT,
  `payment_type` INT,
  `fare_amount` DOUBLE,
  `extra` DOUBLE,
  `mta_tax` DOUBLE,
  `tip_amount` DOUBLE,
  `tolls_amount` DOUBLE,
  `improvement_surcharge` DOUBLE,
  `total_amount` DOUBLE,
  `congestion_surcharge` DOUBLE,
  `airport_fee` DOUBLE,
  `dataset_year` INT,
  `dataset_month` INT,
  `pickup_year` INT,
  `pickup_month` INT,
  `is_pickup_outside_dataset` INT,
  `vendor_desc` STRING,
  `ratecode_desc` STRING,
  `payment_desc` STRING
) USING DELTA
LOCATION '/Volumes/nyc_taxi/data_area/lake_area/silver/';

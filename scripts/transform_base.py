# %%
import json
from pathlib import Path
from pyspark.sql import functions as F

# --- configuracoes
CONFIG_FILE = "/Workspace/Users/joycelnog@gmail.com/pratica_ed/config.json"
cfg = json.loads(Path(CONFIG_FILE).read_text())
BASE = cfg["base_path"]
SILVER_PATH  = f'{BASE}{cfg["paths"]["silver"]}'        
REJECT_PATH  = f'{BASE}rejects/silver_invalid/'           # pasta para inválidos

spark.sql("USE CATALOG nyc_taxi")
spark.sql("USE SCHEMA data_area")

# --- limpar objetos antigos, caso existam fazer a carga completa
spark.sql("DROP VIEW IF EXISTS nyc_taxi.data_area.v_silver_yellow_tripdata")
spark.sql("DROP VIEW IF EXISTS nyc_taxi.data_area.v_silver_invalid")
dbutils.fs.rm(SILVER_PATH, recurse=True)
dbutils.fs.rm(REJECT_PATH, recurse=True)

# --- base bronze
bronze = spark.table("nyc_taxi.data_area.v_bronze_yellow_tripdata")

# --- normalizações 
s0 = (
    bronze
    .withColumn("ratecodeid",      F.coalesce(F.col("ratecodeid"), F.lit(99)))
    .withColumn("store_and_fwd_flag",
                F.when(F.col("store_and_fwd_flag").isNull() | (F.trim(F.col("store_and_fwd_flag")) == ""), "N")
                 .otherwise(F.upper(F.col("store_and_fwd_flag"))))
    .withColumn("store_and_fwd_flag",
                F.when(F.col("store_and_fwd_flag").isin("Y","N"), F.col("store_and_fwd_flag")).otherwise(F.lit("N")))
    .withColumn("passenger_count", F.coalesce(F.col("passenger_count"), F.lit(1)))
    .withColumn("vendorid",     F.col("vendorid").cast("int"))
    .withColumn("ratecodeid",   F.col("ratecodeid").cast("int"))
    .withColumn("payment_type", F.col("payment_type").cast("int"))
    .withColumn("pulocationid", F.col("pulocationid").cast("int"))
    .withColumn("dolocationid", F.col("dolocationid").cast("int"))
)

# --- verificacao de qualidade 
# --- 1) avalia os pickup e dropfoff, distancia e locations, se estão de acordo e consistente
# --- 2) avalia datas pickups fora da data esperada no dataset de origem
cond_pickup_before_dropoff = F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")
cond_trip_distance_non_neg = F.col("trip_distance") >= 0
cond_invalid_zero_distance = (
    (F.col("trip_distance") == 0) &
    ~((F.col("tpep_pickup_datetime") == F.col("tpep_dropoff_datetime")) &
      (F.col("pulocationid") == F.col("dolocationid")))
)
flag_outside_dataset = ((F.col("pickup_year") != F.col("dataset_year")) |
                        (F.col("pickup_month") != F.col("dataset_month"))).cast("int")

s1 = s0.withColumn("is_pickup_outside_dataset", flag_outside_dataset)

# ---- separar registros que vou considerar como inválidos e válidos
invalid = s1.where(
    (~cond_pickup_before_dropoff) |
    (F.col("trip_distance") < 0) |
    cond_invalid_zero_distance |
    (F.col("is_pickup_outside_dataset") == 1)
)

silver = s1.where(
    cond_pickup_before_dropoff &
    cond_trip_distance_non_neg &
    ~cond_invalid_zero_distance &
    (F.col("is_pickup_outside_dataset") == 0)
)

# --- adiciono as descrições legíveis
vendor_map = F.create_map(
    [F.lit(1), F.lit("Creative Mobile Technologies"),
     F.lit(2), F.lit("Curb Mobility"),
     F.lit(6), F.lit("Myle Technologies"),
     F.lit(7), F.lit("Helix")]
)
ratecode_map = F.create_map(
    [F.lit(1), F.lit("Standard rate"),
     F.lit(2), F.lit("JFK"),
     F.lit(3), F.lit("Newark"),
     F.lit(4), F.lit("Nassau/Westchester"),
     F.lit(5), F.lit("Negotiated fare"),
     F.lit(6), F.lit("Group ride"),
     F.lit(99),F.lit("Null/Unknown")]
)
payment_map = F.create_map(
    [F.lit(0), F.lit("Flex Fare"),
     F.lit(1), F.lit("Credit card"),
     F.lit(2), F.lit("Cash"),
     F.lit(3), F.lit("No charge"),
     F.lit(4), F.lit("Dispute"),
     F.lit(5), F.lit("Unknown"),
     F.lit(6), F.lit("Voided trip")]
)

silver = (silver
          .withColumn("vendor_desc",   F.coalesce(vendor_map[F.col("vendorid")],   F.lit("Other/Unknown")))
          .withColumn("ratecode_desc", F.coalesce(ratecode_map[F.col("ratecodeid")], F.lit("Other/Unknown")))
          .withColumn("payment_desc",  F.coalesce(payment_map[F.col("payment_type")], F.lit("Other/Unknown")))
)

# --- carrego os dados validos na delta principal e os invalidos para verificação posterior em outro lugar
(silver.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .partitionBy("dataset_year","dataset_month")
   .save(SILVER_PATH))

(invalid.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .partitionBy("dataset_year","dataset_month")
   .save(REJECT_PATH))

# --- persistir as views apontando para os caminhos do delta
spark.sql(f"""
CREATE OR REPLACE VIEW nyc_taxi.data_area.v_silver_yellow_tripdata AS
SELECT * FROM delta.`{SILVER_PATH}`
""")

spark.sql(f"""
CREATE OR REPLACE VIEW nyc_taxi.data_area.v_silver_invalid AS
SELECT * FROM delta.`{REJECT_PATH}`
""")

# --- checks da carga
spark.sql(f"""
SELECT dataset_year, dataset_month, COUNT(*) AS rows
FROM nyc_taxi.data_area.v_silver_yellow_tripdata
GROUP BY 1,2 ORDER BY 1,2
""").display()

spark.sql(f"""
SELECT dataset_year, dataset_month, COUNT(*) AS rows
FROM nyc_taxi.data_area.v_silver_invalid
GROUP BY 1,2 ORDER BY 1,2
""").display()

print("Silver carregada.")




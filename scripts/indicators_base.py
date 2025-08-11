# %%
import json
from pathlib import Path
from pyspark.sql import functions as F

# --- configuracoes
CONFIG_FILE = "/Workspace/Users/joycelnog@gmail.com/pratica_ed/config.json"
cfg  = json.loads(Path(CONFIG_FILE).read_text())
BASE = cfg["base_path"]
GOLD_PATH   = f'{BASE}{cfg["paths"]["gold"]}'

spark.sql("USE CATALOG nyc_taxi")
spark.sql("USE SCHEMA data_area")

s = spark.table("nyc_taxi.data_area.v_silver_yellow_tripdata")

# --- definindo métricas temporais e métricas auxiliares
s_aug = (s
    .withColumn("dt", F.to_date("tpep_pickup_datetime"))
    .withColumn("hour", F.hour("tpep_pickup_datetime"))
    .withColumn("dur_min", (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))/60.0)
    .withColumn("dur_hr",  F.when(F.col("dur_min") > 0, F.col("dur_min")/60.0))
    .withColumn("speed_mph", F.when(F.col("dur_hr").isNotNull(), F.col("trip_distance")/F.col("dur_hr")))
    .withColumn("revenue_per_mile", F.when(F.col("trip_distance") > 0, F.col("total_amount")/F.col("trip_distance")))
    .withColumn("tip_pct", F.when(F.col("fare_amount") > 0, F.col("tip_amount")/F.col("fare_amount")))
)

# --- FATO: diário
fact_daily = (s_aug.groupBy("dataset_year","dataset_month","dt")
  .agg(F.count("*").alias("qtd_viagens"),
       F.sum("total_amount").alias("receita_total"),
       F.avg("total_amount").alias("ticket_medio"),
       F.avg("trip_distance").alias("dist_media_mi"),
       F.avg("dur_min").alias("duracao_media_min"),
       F.avg("speed_mph").alias("velocidade_media_mph"),
       F.avg("tip_pct").alias("tip_pct_medio"))
)
(fact_daily.write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .partitionBy("dataset_year","dataset_month")
 .save(f"{GOLD_PATH}fact_daily"))
spark.sql(f"""CREATE OR REPLACE VIEW nyc_taxi.data_area.v_gold_fact_daily AS
SELECT * FROM delta.`{GOLD_PATH}fact_daily`""")

# --- FATO: por tipo de pagamento
fact_payment = (s_aug.groupBy("dataset_year","dataset_month","payment_type","payment_desc")
  .agg(F.count("*").alias("qtd_viagens"),
       F.sum("total_amount").alias("receita_total"),
       F.avg("total_amount").alias("ticket_medio"),
       F.avg("tip_pct").alias("tip_pct_medio"),
       F.avg("revenue_per_mile").alias("rev_por_milha_medio"))
)
(fact_payment.write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .partitionBy("dataset_year","dataset_month")
 .save(f"{GOLD_PATH}fact_payment"))
spark.sql(f"""CREATE OR REPLACE VIEW nyc_taxi.data_area.v_gold_fact_payment AS
SELECT * FROM delta.`{GOLD_PATH}fact_payment`""")

# --- FATO: por hora do dia
fact_hour = (s_aug.groupBy("dataset_year","dataset_month","dt","hour")
  .agg(F.count("*").alias("qtd_viagens"),
       F.sum("total_amount").alias("receita_total"),
       F.avg("total_amount").alias("ticket_medio"),
       F.avg("trip_distance").alias("dist_media_mi"),
       F.avg("dur_min").alias("duracao_media_min"),
       F.avg("speed_mph").alias("velocidade_media_mph"))
)
(fact_hour.write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .partitionBy("dataset_year","dataset_month")
 .save(f"{GOLD_PATH}fact_hour"))
spark.sql(f"""CREATE OR REPLACE VIEW nyc_taxi.data_area.v_gold_fact_hour AS
SELECT * FROM delta.`{GOLD_PATH}fact_hour`""")






# %%

import json, re
from pathlib import Path
from pyspark.sql import functions as F
 
# --- configurações ---
CONFIG_FILE = "/Workspace/Users/joycelnog@gmail.com/pratica_ed/config.json"
cfg = json.loads(Path(CONFIG_FILE).read_text())

BASE = cfg["base_path"]
RAW_PATH = f'{BASE}{cfg["paths"]["raw"]}'
BRONZE_PATH = f'{BASE}{cfg["paths"]["bronze"]}'

spark.sql("USE CATALOG nyc_taxi")
spark.sql("USE SCHEMA data_area")


mapped_cols = {
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "ratecodeid": "int",
    "store_and_fwd_flag": "string",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double"
}

# -- tratamento para snake case, deixando as bases que serão apendadas terem os mesmos padrões de nome
def to_snake(c: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", c).lower()

# -- verifica se existe arquivo para subir
files = sorted([f.path for f in dbutils.fs.ls(RAW_PATH) if f.path.endswith(".parquet")])
assert files, f"Nenhum arquivo parquet em {RAW_PATH}"

df_union = None
for p in files:
    # --- pega ano/mês do dataset a partir do nome do arquivo no padrão 2023 01
    m = re.search(r"(\d{4})-(\d{2})\.parquet$", p)
    if not m:
        raise ValueError(f"Não consegui extrair ano/mês do caminho: {p}")
    ds_year = int(m.group(1))
    ds_month = int(m.group(2))

    dfi = spark.read.parquet(p)
    dfi = dfi.toDF(*[to_snake(c) for c in dfi.columns])

    # --- normaliza 'airport_fee' (variações de capitalização)
    for c in list(dfi.columns):
        if c.lower() == "airport_fee" and c != "airport_fee":
            dfi = dfi.withColumnRenamed(c, "airport_fee")

    # --- verifica campos mapeados, não existindo, cria com valor nulo
    for col, dtype in mapped_cols.items():
        if col not in dfi.columns:
            dfi = dfi.withColumn(col, F.lit(None).cast(dtype))
        else:
            dfi = dfi.withColumn(col, F.col(col).cast(dtype))

    # --- adiciona colunas de partição da **origem** do dataset de acordo com o year e month do parquet
    dfi = (dfi
           .select(*mapped_cols.keys())
           .withColumn("dataset_year", F.lit(ds_year).cast("int"))
           .withColumn("dataset_month", F.lit(ds_month).cast("int")))

    # --- mantém ano/mês do pickup para análise
    dfi = (dfi
           .withColumn("pickup_year",  F.year("tpep_pickup_datetime"))
           .withColumn("pickup_month", F.month("tpep_pickup_datetime")))

    df_union = dfi if df_union is None else df_union.unionByName(dfi, allowMissingColumns=True)

df_final = df_union

# --- grava particionando pelos dados da **origem** (estável)
(df_final.write
   .format("delta")
   .mode("overwrite")
   .option("mergeSchema", "true")   
   .partitionBy("dataset_year","dataset_month")
   .save(BRONZE_PATH))

# --- persisto a view no catalogo apontando para o delta 
spark.sql(f"""
CREATE OR REPLACE VIEW nyc_taxi.data_area.v_bronze_yellow_tripdata AS
SELECT * FROM delta.`{BRONZE_PATH}`
""")

# --- checks da carga
spark.sql(f"""
SELECT dataset_year, dataset_month,
       pickup_year, pickup_month,
       COUNT(*) AS rows
FROM nyc_taxi.data_area.v_bronze_yellow_tripdata
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
""").display()

print("Bronze atualizada")




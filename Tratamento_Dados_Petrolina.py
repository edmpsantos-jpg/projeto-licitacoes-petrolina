# Databricks notebook source

spark.conf.set(
    "fs.azure.account.key.datalakepetrolinaatual.dfs.core.windows.net",
    "DkGHDAfDjf3CI9z/AmKDV4VgjVatAM9OT49MfgCW1mTUjHK1WovA+dtG3xHBifGfnLtX9rP+nrMc+ASth3VVjw=="
)


caminho_raw = "abfss://licitacoes-petrolina@datalakepetrolinaatual.dfs.core.windows.net/1-raw/"


dbutils.fs.ls(caminho_raw)

# COMMAND ----------


df_contratos = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .load(caminho_raw + "Detalhes dos ContratosTCM.csv")


display(df_contratos.limit(10))


# COMMAND ----------

from pyspark.sql.functions import col, trim, upper

df_limpo = df_contratos.select(
    trim(col("Unidade Gestora")).alias("Secretaria"),
    col("Modalidade"),
    col("Contrato").alias("Numero_Contrato"),
    upper(trim(col("Contratado"))).alias("Fornecedor"),
    col("Assinatura").alias("Data_Assinatura")
).filter(col("Município") == "Petrolina de Goiás")


display(df_limpo.limit(10))

# COMMAND ----------


caminho_processed = "abfss://licitacoes-petrolina@datalakepetrolinaatual.dfs.core.windows.net/2-processed/contratos_limpos"

df_limpo.write.mode("overwrite").parquet(caminho_processed)

print("Sucesso! Os dados de Petrolina foram salvos na camada 2-processed.")

# COMMAND ----------


caminho_processed = "abfss://licitacoes-petrolina@datalakepetrolinaatual.dfs.core.windows.net/2-processed/contratos_limpos"


df_limpo.write.mode("overwrite").parquet(caminho_processed)

print("Sucesso! Os dados limpos de Petrolina foram salvos na camada 2-processed.")
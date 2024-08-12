# Databricks notebook source
# DBTITLE 1,Import Class
# MAGIC %run ./class_SCDManager

# COMMAND ----------

# DBTITLE 1,Import Lib's
# Importa bibliotecas
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Select After
# MAGIC %sql
# MAGIC -- SELECT * FROM default.dim_teste1

# COMMAND ----------

# DBTITLE 1,Create Sample
# Definir schema para o DataFrame
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("DESCRICAO", StringType(), True),
    StructField("INDICE", IntegerType(), True),
])

# Criar lista de tuplas com os valores
data = [
    (5101, "VENDA", 1),
    (5102, "VENDA", 1),
    (5103, "VENDA", 1),
    (5104, "VENDA", 1)
]

# Criar DataFrame a partir da lista de tuplas e do schema
df_dimensao = spark.createDataFrame(data, schema)

# Exibir DataFrame
display(df_dimensao)

# COMMAND ----------

# DBTITLE 1,Execute Class
# Cria uma instância da classe SCD2Manager
scd2_manager = SCDManager()

# Defina os parâmetros necessários
columns_to_hash = ["ID", "DESCRICAO", "INDICE"]
partition_columns = columns_to_hash
matched_columns = ["ID", "DESCRICAO"]

# Use o método manage_scd2_table para criar ou atualizar a tabela
scd2_manager.manage_scd_table(
    scd_type=1,
    df=df_dimensao,
    schema_name='default',
    table_name='dim_teste1',
    sk_name='SK_DIM_TESTE',
    pk_name='ID',
    partition_columns=partition_columns,
    matched_columns=matched_columns
)


# COMMAND ----------

# DBTITLE 1,Select Before
# MAGIC %sql
# MAGIC SELECT * FROM default.dim_teste1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table default.dim_teste1

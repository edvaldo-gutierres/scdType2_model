# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import pytz
import logging

class SCDManager:
    def __init__(self):
        """
        Inicializa a classe SCDManager e configura o fuso horário da sessão Spark.
        """
        try:
            spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
        except Exception as e:
            logging.error(f"Erro ao configurar o fuso horário da sessão Spark: {e}")
            raise

    def generate_change_date(self, time_zone: str) -> datetime:
        """
        Retorna o timestamp atual ajustado para o fuso horário especificado.
        
        Parâmetros:
        time_zone (str): O fuso horário para ajustar o timestamp.
        
        Retorna:
        datetime: O timestamp atual ajustado para o fuso horário especificado.
        """
        try:
            tz = pytz.timezone(time_zone)
            return datetime.now(tz)
        except Exception as e:
            logging.error(f"Erro ao gerar a data de mudança: {e}")
            raise

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """
        Verifica se a tabela existe no esquema especificado.
        
        Parâmetros:
        schema_name (str): O nome do esquema.
        table_name (str): O nome da tabela.
        
        Retorna:
        bool: True se a tabela existir, False caso contrário.
        """
        try:
            result = spark.sql(f"SHOW TABLES IN {schema_name} LIKE '{table_name}'").collect()
            return len(result) > 0
        except Exception as e:
            logging.error(f"Erro ao verificar a existência da tabela {schema_name}.{table_name}: {e}")
            raise

    ################################### SCD-TYPE 2 #############################################
    def create_dimension_scd2(self, df: DataFrame, schema_name: str, table_name: str, sk_name: str, columns_to_hash: list) -> DataFrame:
        """
        Cria uma dimensão do tipo SCD-2 (Slowly Changing Dimension - Tipo 2).
        
        Parâmetros:
        df (DataFrame): DataFrame contendo os dados a serem inseridos na dimensão.
        schema_name (str): Nome do esquema onde a tabela será criada.
        table_name (str): Nome da tabela a ser criada.
        sk_name (str): Nome da coluna da chave substituta (SK).
        columns_to_hash (list): Lista de colunas utilizadas para gerar a chave substituta (SK).
        
        Retorna:
        DataFrame: DataFrame final com a dimensão criada.
        """
        try:
            # Define uma tupla com valores padrão para "Não informado"
            data = tuple(
                "Não informado" if isinstance(field.dataType, StringType) else
                -1 if isinstance(field.dataType, IntegerType) else
                None
                for field in df.schema.fields
            )
            
            # Cria um DataFrame com a tupla
            df_scd2 = spark.createDataFrame([data], schema=df.schema)

            # Faz o join com o DataFrame de entrada para a criação da dimensão
            df_scd2 = df_scd2.union(df)
            
            # Inseri as colunas default de dimensão
            df_scd2 = df_scd2.withColumn("ROW_INGESTION_TIMESTAMP", current_timestamp()) \
                .withColumn("ROW_VERSION", lit(1)) \
                .withColumn("ROW_CURRENT_INDICATOR", lit(True)) \
                .withColumn("ROW_EFFECTIVE_DATE", to_timestamp(lit('1900-01-01 00:00:00'), "yyyy-MM-dd HH:mm:ss")) \
                .withColumn("ROW_EXPIRATION_DATE", to_timestamp(lit(None), "yyyy-MM-dd HH:mm:ss"))
            
            # Inseri a coluna SK usando as colunas passadas como parâmetro
            df_scd2 = df_scd2.withColumn(
                sk_name,
                sha2(concat_ws("|", *columns_to_hash), 256)
            )
            
            # Ordena as colunas
            df_scd2 = df_scd2.select(
                sk_name,
                'ROW_INGESTION_TIMESTAMP',
                'ROW_VERSION',
                'ROW_CURRENT_INDICATOR',
                'ROW_EFFECTIVE_DATE',
                'ROW_EXPIRATION_DATE',
                *df.columns
            ).orderBy(columns_to_hash[0])  # Ordena pela primeira coluna da lista de colunas
            
            df_scd2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{schema_name}.{table_name}')

            print(f'Tabela {schema_name}.{table_name} (SCD-Type2) criada com sucesso ')

            # Retorna o DataFrame final
            return df_scd2
        except Exception as e:
            logging.error(f"Erro ao criar a dimensão SCD-Type2 {schema_name}.{table_name}: {e}")
            raise

    def upsert_scd_2(self, df_origem: DataFrame, schema_name: str, table_name: str, sk_name: str, pk_name: str, partition_columns: list, matched_columns: list) -> None:
        """
        Realiza um upsert (inserção/atualização) em uma dimensão do tipo SCD-2 (Slowly Changing Dimension - Tipo 2).

        Parâmetros:
        df_origem (DataFrame): DataFrame contendo os dados de origem a serem mesclados.
        schema_name (str): Nome do esquema onde a tabela está localizada.
        table_name (str): Nome da tabela onde o upsert será realizado.
        sk_name (str): Nome da coluna da chave substituta (SK).
        pk_name (str): Nome da coluna da chave primária (PK).
        partition_columns (list): Lista de colunas de partição.
        matched_columns (list): Lista de colunas usadas para correspondência no merge.
        """
        try:
            # Determina as colunas para a geração da chave substituta (SK)
            columns_to_hash = [col for col in df_origem.columns if col not in ['ROW_INGESTION_TIMESTAMP', 'ROW_VERSION', 'ROW_CURRENT_INDICATOR', 'ROW_EFFECTIVE_DATE', 'ROW_EXPIRATION_DATE', sk_name]]

            # Carrega o DataFrame de destino
            df_destino = spark.table(f"{schema_name}.{table_name}")

            # Cria uma janela para particionar pelas colunas de particionamento e ordenar por ROW_EFFECTIVE_DATE
            window_spec = Window.partitionBy(*partition_columns).orderBy(col("ROW_EFFECTIVE_DATE").desc())

            # Adiciona a coluna row_num para determinar a linha mais recente
            df_destino = df_destino.withColumn("row_num", row_number().over(window_spec)).filter((col("row_num") == 1) & (col("ROW_CURRENT_INDICATOR") == True)).drop("row_number").select(partition_columns)

            # Filtra para manter apenas as linhas mais recentes
            # df_destino = df_destino.filter(col("row_num") == 1).select(*partition_columns).orderBy(partition_columns[0])

            # Realiza o EXCEPT para retornar apenas os registros novos
            df_dados_novos = df_origem.exceptAll(df_destino)

            # Gera o timestamp atual para mudança
            change_date = self.generate_change_date("America/Sao_Paulo")

            # Adiciona a coluna de chave substituta (SK) e o timestamp de mudança
            df_temp_dados_novos = df_dados_novos.withColumn(
                sk_name,
                sha2(concat_ws("|", lit(change_date), *[col(c) for c in columns_to_hash]), 256)
            ).withColumn("CHANGE_DATE", to_timestamp(lit(change_date)))

            # Pega os registros novos com base nas colunas de matched_columns
            new_data = df_temp_dados_novos.select(matched_columns).distinct()

            # Renomeia a coluna ID no df_temp_dados_novos para evitar ambiguidade
            df_temp_dados_novos = df_temp_dados_novos.withColumnRenamed(pk_name, f'{pk_name}_novo')

            # Pega os dados atuais da dimensão e renomeia as colunas para evitar ambiguidade
            df_atual = (
                spark.table(f"{schema_name}.{table_name}")
                .filter(col(pk_name).isin([row[pk_name] for row in new_data.collect()]))
                .select(
                    col(sk_name).alias(f'{sk_name}_anterior'),
                    *[col(c).alias(f'{c}_anterior') for c in partition_columns],
                    col("ROW_VERSION").alias("ROW_VERSION_anterior"),
                    col(pk_name)
                )
            )

            # Junta com df_temp_dados_novos para atualização, usando as colunas renomeadas
            df_temp_dados_novos = df_temp_dados_novos.join(
                df_atual,
                on=[df_temp_dados_novos[f'{pk_name}_novo'] == df_atual[f'{pk_name}_anterior']],
                how="inner"
            )

            # Calcula o novo valor de ROW_VERSION
            df_temp_dados_novos = df_temp_dados_novos.withColumn(
                "ROW_VERSION", coalesce(col("ROW_VERSION_anterior") + 1, lit(1))
            )

            # Cria uma janela para particionar pelos IDs e ordenar por ROW_VERSION em ordem decrescente
            window_spec = Window.partitionBy(pk_name).orderBy(col("ROW_VERSION_anterior").desc())

            # Adiciona uma coluna de row_number para identificar a linha mais recente
            df_temp_dados_novos = df_temp_dados_novos.withColumn("row_num", row_number().over(window_spec))

            # Filtra para manter apenas as linhas mais recentes por ID (pk_name)
            df_temp_dados_novos = df_temp_dados_novos.filter(col("row_num") == 1).drop("row_num")

            # Seleciona as colunas finais sem ambiguidade
            df_final = df_temp_dados_novos.select(
                col(sk_name),
                col(f'{sk_name}_anterior'),
                *[col(c) for c in partition_columns],
                col("CHANGE_DATE"),
                col("ROW_VERSION")
            )
            
            # Cria uma view temporária
            df_final.createOrReplaceTempView("dados_novos")
            
            # Carrega a tabela Delta
            deltaTable = DeltaTable.forName(spark, f"{schema_name}.{table_name}")
            
            # Primeiro MERGE: Atualização dos registros existentes
            deltaTable.alias("destino").merge(
                df_final.alias("dados_novos"),
                f"destino.{sk_name} = dados_novos.{sk_name}_anterior"
            ).whenMatchedUpdate(
                set={
                    "ROW_EXPIRATION_DATE": to_timestamp(lit(change_date)),
                    "ROW_CURRENT_INDICATOR": lit(False)
                }
            ).execute()
            
            
            matched_columns.append('ROW_VERSION')

            # Segundo MERGE: Inserção de novos registros
            deltaTable.alias("destino").merge(
                df_final.alias("dados_novos"),
                " AND ".join([f"destino.{col_name} = dados_novos.{col_name}" for col_name in matched_columns])
            ).whenNotMatchedInsert(
                values={
                    sk_name: col(sk_name),
                    "ROW_INGESTION_TIMESTAMP": to_timestamp(lit(change_date)),
                    "ROW_VERSION": col("ROW_VERSION"),
                    "ROW_CURRENT_INDICATOR": lit(True),
                    "ROW_EFFECTIVE_DATE": to_timestamp(lit(change_date)),
                    "ROW_EXPIRATION_DATE": lit(None).cast(TimestampType()),
                    **{col_name: col(col_name) for col_name in partition_columns}
                }
            ).execute()
        except Exception as e:
            logging.error(f"Erro ao realizar o upsert SCD-Type2 na tabela {schema_name}.{table_name}: {e}")
            raise


    ################################### SCD-TYPE 1 #############################################
    def create_dimension_scd1(self, df: DataFrame, schema_name: str, table_name: str, sk_name: str, columns_to_hash: list) -> DataFrame:
        """
        Cria uma dimensão do tipo SCD-1 (Slowly Changing Dimension - Tipo 1).
        
        Parâmetros:
        df (DataFrame): DataFrame contendo os dados a serem inseridos na dimensão.
        schema_name (str): Nome do esquema onde a tabela será criada.
        table_name (str): Nome da tabela a ser criada.
        sk_name (str): Nome da coluna da chave substituta (SK).
        columns_to_hash (list): Lista de colunas utilizadas para gerar a chave substituta (SK).
        
        Retorna:
        DataFrame: DataFrame final com a dimensão criada.
        """

        try:
            # Define uma tupla com valores padrão para "Não informado"
            data = tuple(
                "Não informado" if isinstance(field.dataType, StringType) else
                -1 if isinstance(field.dataType, IntegerType) else
                None
                for field in df.schema.fields
            )
            
            # Cria um DataFrame com a tupla
            df_scd1 = spark.createDataFrame([data], schema=df.schema)

            # Faz o join com o DataFrame de entrada para a criação da dimensão
            df_scd1 = df_scd1.union(df)
            
            # Inseri as colunas default de dimensão
            df_scd1 = df_scd1.withColumn("ROW_INGESTION_TIMESTAMP", current_timestamp())
            
            # Inseri a coluna SK usando as colunas passadas como parâmetro
            df_scd1 = df_scd1.withColumn(
                sk_name,
                sha2(concat_ws("|", *columns_to_hash), 256)
            )
            
            # Ordena as colunas
            df_scd1 = df_scd1.select(
                sk_name,
                'ROW_INGESTION_TIMESTAMP',
                *df.columns
            ).orderBy(columns_to_hash[0])  # Ordena pela primeira coluna da lista de colunas
            
            df_scd1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{schema_name}.{table_name}')

            print(f'Tabela {schema_name}.{table_name} (SCD-Type1) criada com sucesso ')

            # Retorna o DataFrame final
            return df_scd1
        except Exception as e:
            logging.error(f"Erro ao criar a dimensão SCD-Type1 {schema_name}.{table_name}: {e}")
            raise


    def upsert_scd_1(self, df_origem: DataFrame, schema_name: str, table_name: str, sk_name: str, pk_name: str, partition_columns: list, matched_columns: list) -> None:
        """
        Realiza um upsert (inserção/atualização) em uma dimensão do tipo SCD-1 (Slowly Changing Dimension - Tipo 1).

        Parâmetros:
        df_origem (DataFrame): DataFrame contendo os dados de origem a serem mesclados.
        schema_name (str): Nome do esquema onde a tabela está localizada.
        table_name (str): Nome da tabela onde o upsert será realizado.
        sk_name (str): Nome da coluna da chave substituta (SK).
        pk_name (str): Nome da coluna da chave primária (PK).
        partition_columns (list): Lista de colunas de partição.
        matched_columns (list): Lista de colunas usadas para correspondência no merge.
        """

        try:

            # Determina automaticamente as colunas que devem ser usadas para a geração da chave substituta (SK)
            columns_to_hash = [col for col in df_origem.columns if col not in ['ROW_INGESTION_TIMESTAMP', sk_name]]
            
            # Carrega o DataFrame de destino
            df_destino = spark.table(f"{schema_name}.{table_name}")
            
            # Cria uma janela para particionar pelas colunas de particionamento e ordenar por ROW_INGESTION_TIMESTAMP
            window_spec = Window.partitionBy(*partition_columns).orderBy(col(pk_name).desc())
            
            # Adiciona a coluna row_num para determinar a linha mais recente
            df_destino = df_destino.withColumn("row_num", row_number().over(window_spec))
            
            # Filtra para manter apenas as linhas mais recentes
            df_destino = df_destino.filter(col("row_num") == 1).drop("row_num")
            
            # Realiza o LEFT JOIN para encontrar registros novos
            df_dados_novos = df_origem.alias("origem").join(
                df_destino.alias("destino"),
                on=[col(f"origem.{col_name}") == col(f"destino.{col_name}") for col_name in matched_columns],
                how="left_anti"  # Mantém apenas os registros da origem que não estão no destino
            )
            
            # Gera o timestamp atual para mudança
            change_date = self.generate_change_date("America/Sao_Paulo")
            
            # Adiciona a coluna de chave substituta (SK) e o timestamp de mudança
            df_dados_novos = df_dados_novos.withColumn(
                sk_name,
                sha2(concat_ws("|", lit(change_date), *[col(c) for c in columns_to_hash]), 256)
            ).withColumn("ROW_INGESTION_TIMESTAMP", to_timestamp(lit(change_date)))
            
            # Cria uma view temporária
            df_dados_novos.createOrReplaceTempView("dados_novos")
            
            # Carrega a tabela Delta
            deltaTable = DeltaTable.forName(spark, f"{schema_name}.{table_name}")
            
            # Primeiro MERGE: Atualização dos registros existentes
            deltaTable.alias("destino").merge(
                df_dados_novos.alias("dados_novos"),
                f"destino.{pk_name} = dados_novos.{pk_name}"
            ).whenMatchedUpdate(
                set={
                    "ROW_INGESTION_TIMESTAMP": col("dados_novos.ROW_INGESTION_TIMESTAMP"),
                    **{col_name: col(f"dados_novos.{col_name}") for col_name in matched_columns}
                }
            ).execute()
            
            # Segundo MERGE: Inserção de novos registros
            deltaTable.alias("destino").merge(
                df_dados_novos.alias("dados_novos"),
                " AND ".join([f"destino.{col_name} = dados_novos.{col_name}" for col_name in matched_columns])
            ).whenNotMatchedInsert(
                values={
                    sk_name: col(sk_name),
                    "ROW_INGESTION_TIMESTAMP": col("ROW_INGESTION_TIMESTAMP"),
                    **{col_name: col(col_name) for col_name in matched_columns}
                }
            ).execute()
        except Exception as e:
            logging.error(f"Erro ao realizar o upsert SCD-Type1 na tabela {schema_name}.{table_name}: {e}")
            raise


    def manage_scd_table(self, scd_type: int, df: DataFrame, schema_name: str, table_name: str, sk_name: str, pk_name: str, partition_columns: list, matched_columns: list) -> None:
        """
        Gerencia a criação e o upsert de uma tabela de dimensão SCD (Slowly Changing Dimension) do tipo 1 ou 2.

        Parâmetros:
        scd_type (int): Tipo de SCD (1 ou 2).
        df (DataFrame): DataFrame contendo os dados a serem inseridos ou atualizados.
        schema_name (str): Nome do esquema onde a tabela está localizada.
        table_name (str): Nome da tabela onde o upsert será realizado.
        sk_name (str): Nome da coluna da chave substituta (SK).
        pk_name (str): Nome da coluna da chave primária (PK).
        partition_columns (list): Lista de colunas de partição.
        matched_columns (list): Lista de colunas usadas para correspondência no merge.
        """
        try:
            if scd_type == 2:
                if self.table_exists(schema_name, table_name):
                    self.upsert_scd_2(df, schema_name, table_name, sk_name, pk_name, partition_columns, matched_columns)
                else:
                    self.create_dimension_scd2(df, schema_name, table_name, sk_name, partition_columns)
                    self.upsert_scd_2(df, schema_name, table_name, sk_name, pk_name, partition_columns, matched_columns)

            elif scd_type == 1:
                if self.table_exists(schema_name, table_name):
                    self.upsert_scd_1(df, schema_name, table_name, sk_name, pk_name, partition_columns, matched_columns)
                else:
                    self.create_dimension_scd1(df, schema_name, table_name, sk_name, partition_columns)
                    self.upsert_scd_1(df, schema_name, table_name, sk_name, pk_name, partition_columns, matched_columns)

            else:
                raise ValueError(f"Tipo SCD inválido: {scd_type}. Utilize 1 ou 2.")

        except Exception as e:
            raise Exception(f"Erro ao gerenciar a tabela SCD {schema_name}.{table_name}: {e}")

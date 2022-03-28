# Importando modulos
import time
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from Parquet import Parquet
from Conexao import ConexaoPostgres
from funcoes import *

# Criando sessao Spark
spark = SparkSession.builder.appName('job_3').getOrCreate()

# Limpando saida
os.system("clear")
print("\nIniciando Job 3\n---------")

# Criacao das tabelas temporarias/datamart
try:
    print("\n\tCriando Datamart...")
    inicio = time.time()
    df_temp_tables = spark.createDataFrame([("postgre", "Criação temp tables")], ["usuario", "descricao_log"])
    conexao = ConexaoPostgres("34.134.10.208","mineracao","postgres","senhabanco")
    conexao.inserir_dados(df_temp_tables, "registro_log")
except Exception as e:
    print(str(e))  
    gravar_log(f"job_3.py - Falha ao criar Datamart")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_3.py - Criacao do Datamart em {(float((fim - inicio)%60))}")

# Leitura das tabelas do Datamart
try:
    print("\n\tLendo Datamart...")
    inicio = time.time()
    conexao = ConexaoPostgres("34.134.10.208","mineracao","postgres","senhabanco")
    df_arrecadacao = conexao.ler_dados(spark, "dm_arrecadacao")
    df_autuacao = conexao.ler_dados(spark, "dm_autuacao")
    df_barragens = conexao.ler_dados(spark, "dm_barragens")
    df_beneficiada = conexao.ler_dados(spark, "dm_beneficiada")
    df_distribuicao = conexao.ler_dados(spark, "dm_distribuicao")
    df_municipio = conexao.ler_dados(spark, "dm_municipio")
    df_pib = conexao.ler_dados(spark, "dm_pib")
except Exception as e:
    print(str(e))
    gravar_log(f"job_3.py - Falha ao ler Datamart")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_3.py - Datamart lido em {(float((fim - inicio)%60))}")


# Gravando em Parquets
try:
    print("\n\tGravando parquets...")
    inicio = time.time()
    parquet = Parquet("gs://parquets/arrecadacao").grava_parquet(df_arrecadacao)
    parquet = Parquet("gs://parquets/autuacao").grava_parquet(df_autuacao)
    parquet = Parquet("gs://parquets/barragens").grava_parquet(df_barragens)
    parquet = Parquet("gs://parquets/beneficiada").grava_parquet(df_beneficiada)
    parquet = Parquet("gs://parquets/distribuicao").grava_parquet(df_distribuicao)
    parquet = Parquet("gs://parquets/municipio").grava_parquet(df_municipio)
    parquet = Parquet("gs://parquets/pib").grava_parquet(df_pib)   
except Exception as e:
    print(str(e))
    gravar_log(f"job_3.py - Falha ao salvar parquets")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_3.py - Parquets salvos em {(float((fim - inicio)%60))}")
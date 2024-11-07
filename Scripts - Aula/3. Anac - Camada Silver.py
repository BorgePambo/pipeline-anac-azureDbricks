# Databricks notebook source
# MAGIC %fs ls /mnt/Anac

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac/Bronze/

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/Anac/Bronze/V_OCORRENCIA_AMPLA.json')
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

# substituindo colunas de texto null para 'Sem Registro'

# COMMAND ----------

colunas = ['Aerodromo_de_Destino', 'Aerodromo_de_Origem', 'CLS', 'Categoria_da_Aeronave', 'Classificacao_da_Ocorrência', 'Danos_a_Aeronave', 'Data_da_Ocorrencia', 'Descricao_do_Tipo', 'Fase_da_Operacao', 'Historico', 'Hora_da_Ocorrência', 'ICAO', 'Ilesos_Passageiros', 'Ilesos_Tripulantes', 'Latitude', 'Longitude', 'Matricula', 'Modelo', 'Municipio', 'Nome_do_Fabricante', 'Numero_da_Ficha', 'Numero_da_Ocorrencia', 'Numero_de_Assentos', 'Operacao', 'Operador', 'Operador_Padronizado', 'PMD', 'PSSO', 'Regiao', 'Tipo_ICAO', 'Tipo_de_Aerodromo', 'Tipo_de_Ocorrencia', 'UF']

##percorrendo todas as colunas e fazer a mesma coisa para todas selecionadas na variavel
for ajuste in colunas:
    df = df.fillna('Sem Registro', subset=[ajuste])

display(df)

# COMMAND ----------

# mportar todas as funções do módulo pyspark.sql.functions
from pyspark.sql.functions import *

# COMMAND ----------

ajuste_int = [
    'Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes',
    'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes',
    'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes',
    'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes'
]

for coluna in ajuste_int:
    df = df\
        .withColumn(coluna, df[coluna].cast("int"))\
        .fillna(0, subset=[coluna])

display(df)

# COMMAND ----------

print( df.filter(col('Lesoes_Desconhecidas_Passageiros').isNull()).count() )

# COMMAND ----------

resultado = []
for coluna in df.columns:
    num_nulos = df.filter(col(coluna).isNull()).count()
    resultado.append((coluna, num_nulos))

df_novo = spark.createDataFrame(resultado, ["Coluna", "Valores_Nulos"])
display(df_novo)

# COMMAND ----------

# MAGIC %fs ls /mnt/Anac

# COMMAND ----------

# Salvando o DataFrame em formato Parquet no caminho especificado
df.write\
    .mode('overwrite')\
    .format("parquet")\
    .save('dbfs:/mnt/Anac/Silver/anac_silver.parquet')

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac/Silver/

# COMMAND ----------

# Ler esse arquivo em Parquet
display( spark.read.parquet('dbfs:/mnt/Anac/Silver/anac_silver.parquet/') )

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



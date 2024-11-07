# Databricks notebook source
"""
Descrição do Projeto Gold (base final para analistas de estados tratarem)

1° Selecionar somente as colunas que o cliente pediu
2° Criar uma coluna que faz a soma de todas as lesões 
3° Renomear as colunas oara ficar mais intituivo para o usuário final
4° Excluir dados que o esrado tenham a classificação [indeterminado, Sem Registro, Exterior]'
5° inserir colunas com nome de atualização para o usuário ver quando os dados formam atualizados
6° Salvar na camada Gold particionada por UF > 'Estado'
"""

# COMMAND ----------

# MAGIC %fs ls mnt/Anac/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac/Silver/

# COMMAND ----------

df = spark.read.parquet('dbfs:/mnt/Anac/Silver/anac_silver.parquet/')
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

#selecionar somente colunas que o cliente/sector pediu
colunas = ['Aerodromo_de_Destino', 'Aerodromo_de_Origem', 'Classificacao_da_Ocorrência', 'Danos_a_Aeronave', 'Data_da_Ocorrencia', 'Municipio', 'UF', 'Tipo_de_Ocorrencia', 'Tipo_de_Aerodromo', 'Lesoes_Desconhecidas_Passageiros',  'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']

df = df.select(colunas)
display(df)

# COMMAND ----------

#Criar uma coluna que faz a soma d todas as lesões
colunas_a_somar = [
    'Lesoes_Desconhecidas_Passageiros',
    'Lesoes_Desconhecidas_Terceiros',
    'Lesoes_Desconhecidas_Tripulantes',
    'Lesoes_Fatais_Passageiros',
    'Lesoes_Fatais_Terceiros',
    'Lesoes_Fatais_Tripulantes',
    'Lesoes_Graves_Passageiros',
    'Lesoes_Graves_Terceiros',
    'Lesoes_Graves_Tripulantes',
    'Lesoes_Leves_Passageiros',
    'Lesoes_Leves_Terceiros',
    'Lesoes_Leves_Tripulantes'
]

df = df.withColumn('Total_Lesoes', sum(df[somartudo] for somartudo in colunas_a_somar))
display(df)

# COMMAND ----------

#Renomear as colunas para ficar mais intituivo ao usurios

df = df\
      .withColumnRenamed('Aerodromo_de_Destino', 'Destino')\
      .withColumnRenamed('Aerodromo_de_Origem', 'Origem')\
      .withColumnRenamed('Classificacao_da_Ocorrência', 'Ocorrencia')\
      .withColumnRenamed('Danos_a_Aeronave', 'Danos')\
      .withColumnRenamed('Data_da_Ocorrencia', 'Data')\
      .withColumnRenamed('UF', 'Estado')

display(df)

# COMMAND ----------

# ~simbolo de negação
Filtro = ['Indeterminado', 'Sem Registro', 'Exterior']
display( df.filter(df['Estado'].isin(Filtro) ) )

# COMMAND ----------

#Excluir dados de Estado que tenha classificacao ['Indeterminado', 'Sem Registro', 'Exterior']
classificacao_a_excluir = ['Indeterminado', 'Sem Registro', 'Exterior']
df = df.filter(~df['Estado'].isin(classificacao_a_excluir) )
display(df)

# COMMAND ----------

#5° Inserir coluna com nome da atualização para o usuário ver quando os dados foram atualizados
from pyspark.sql.functions import current_timestamp, date_format, from_utc_timestamp

df = df.withColumn(
    'Atualização',
    date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss")
)
display(df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac/

# COMMAND ----------

#Salvando na camada Gold particionado por UF > 'Estado'
df.write\
    .mode('overwrite')\
    .format('parquet')\
    .partitionBy('Estado')\
    .save('dbfs:/mnt/Anac/Gold/anac_gold_particionado')

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac/Gold/

# COMMAND ----------



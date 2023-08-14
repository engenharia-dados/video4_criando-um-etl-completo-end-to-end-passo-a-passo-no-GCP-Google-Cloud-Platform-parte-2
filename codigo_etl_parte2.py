import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from datetime import datetime
from pytz import timezone

fuso = 'America/Sao_Paulo'
formato_data = '%Y-%m-%d'
data_corrente = datetime.now(timezone(fuso)).strftime(formato_data)
data_corrente

arquivo_bruto_entrada = spark.read.json('gs://base_voo/dados_brutos/'+data_corrente+'.json')
arquivo_bruto_entrada.createOrReplaceTempView('tb_voos')

query = """
select
*,
case
when distance between 0 and 1000 then 1
when distance between 1001 and 2000 then 2
when distance between 2001 and 3000 then 3
when distance between 3001 and 4000 then 4
when distance between 4001 and 5000 then 5
end categoria_distancia_voos
from tb_voos limit 10
"""

query_saida = spark.sql(query)

storage = 'gs://base_voo/dados_tratados/'+data_corrente+'_ETL_voos'

query_saida.coalesce(1).write.format('json').save(storage)

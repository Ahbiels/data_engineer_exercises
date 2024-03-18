from pyspark.sql import SparkSession, functions as Func
from pyspark.sql.types import *
from datetime import datetime
import random

def main(path_input,path_output, bucket_name, format_file_save, bg_table,name_file_output):
    now = datetime.now()
    spark = SparkSession.builder.appName("Challenger_two - ETL with pyspark")\
        .getOrCreate()

    current_hour = now.strftime("%H:%M:%S")
    current_hour = current_hour.replace(":","")
    name_complement = str(random.randint(100000,999999)) + "_" + current_hour
    
    data_input = spark.read.format(format_file_save).load(path_input)

    data_output = data_input.withColumn("Data da Coleta", Func.to_date("Data da Coleta","dd/MM/yyyy"))
    data_output = data_output.withColumn("Ano", Func.year(Func.col("Data da Coleta")))
    data_output = data_output.withColumn("Semestre", Func.when( Func.month(Func.col("Data da COleta")) < 7, "1 Semestre").otherwise("2 Semestre"))
    data_output = data_output.withColumn("Nome do arquivo", Func.input_file_name())

    #Write the file in the bucket
    data_output.write.format(format_file_save).save(f"{path_output}/{name_file_output}_{name_complement}")

     #Write the file in the bigquery
    data_output.write.format("bigquery") \
        .option("temporaryGcsBucket", bucket_name) \
        .option("table", bg_table) \
        .save()
    
    # spark.stop()

path_input = "gs://challenger-data-enginner-input/data.parquet"
path_output = "gs://challenger-data-enginner-output/"

bucket_name = path_output[5:]
bucket_name = bucket_name.split("/")[0]

format_file_save = "parquet"
bg_table = "projeto-estudos-415711.challengertwo.chalenger-data"

name_file_output = "data_parquet"

main(path_input,path_output, bucket_name, format_file_save, bg_table,name_file_output)


#path_input: Caminho dos dados no GCS gerado pela API do coletor. Ex: gs://bucket_name/file_name
#path_output: Caminho onde os dados processados ​​serão salvos. Ex: gs://nome_do_bucket_2/nome_do_arquivo
#format_file_save: Formato do arquivo a ser salvo em path_output. Ex: PARQUETE
#bq_table: tabela do BigQuery onde os dados serão salvos. Ex: dataset.table_example
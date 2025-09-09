from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("test")    
        # .config("hive.metastore.uris", "thrift://masternode:9083")
        # .enableHiveSupport()
        .getOrCreate()
)

spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES").show()
spark.table("gold.data_bureau").show()

# (
#     df.coalesce(1)  # força apenas 1 arquivo
#       .write
#       .option("header", True)        # inclui cabeçalho
#       .option("encoding", "UTF-8")   # garante UTF-8
#       .mode("overwrite")             # sobrescreve saída se já existir
#       .csv("/opt/scripts/gold.csv")  # caminho local (vai criar diretório)
# )


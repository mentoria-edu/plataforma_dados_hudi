from pyspark.sql import SparkSession

metastore_host = "masternode"
metastore_port = "9083"

spark = (
    SparkSession.builder
        .appName("SparkRemoteHiveMetastoreTest")
        .config("hive.metastore.uris", "thrift://masternode:9083")
        .enableHiveSupport()
        .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

dados = [(1, "Nome_1"), (2, "Nome_2"), (3, "Nome_3")]
df = spark.createDataFrame(dados, ["id", "nome"])

df.write.mode("overwrite").saveAsTable("bronze.teste_simples")

print("DataFrame salvo com sucesso no Hive Metastore remoto!")
spark.stop()
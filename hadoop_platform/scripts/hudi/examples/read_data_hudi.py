from pyspark.sql import SparkSession

tableName = "trips_table"
basePath = f"hdfs://masternode:9000/lakehouse/{tableName}"

spark = SparkSession.builder \
    .appName("read_hudi") \
    .config("hive.metastore.uris", "thrift://masternode:9083") \
    .getOrCreate()

df = spark.read.format("hudi").load(basePath)
df.show()

df.printSchema()

print(f"Total de registros: {df.count()}")

spark.stop()
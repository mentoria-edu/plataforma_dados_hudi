from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("read_hudi") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Ler os dados do Hudi
df = spark.read.format("hudi").load("file:///tmp/trips_table")
df.show()

# Ver o schema
df.printSchema()

# Contar registros
print(f"Total de registros: {df.count()}")

spark.stop()
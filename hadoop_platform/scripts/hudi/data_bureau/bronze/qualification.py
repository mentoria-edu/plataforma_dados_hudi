from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, LongType, DoubleType, TimestampType

PATH_CSV_FILE = "/csvs/qualificacao_socios.csv"
TABLE_NAME = "responsible_qualification"

spark = SparkSession.builder.appName(f"bronze_{TABLE_NAME}").getOrCreate()

df = spark.read.csv(
    PATH_CSV_FILE,
    sep=";",
    header=False,
    inferSchema=False
)

df = df.withColumnsRenamed({
    "_c0": "id_qualification",
    "_c1": "description",
})


df = df.withColumn("created_at", current_timestamp())

df = df \
    .withColumn("id_qualification", col("id_qualification").cast(StringType())) \
    .withColumn("description", col("description").cast(StringType())) \
    

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.recordkey.field": "id_qualification",
    "hoodie.datasource.write.operation": "insert",
    "hoodie.parquet.small.file.limit": "0",
    "hoodie.parquet.max.file.size": "125829120", 
    "hoodie.copyonwrite.record.size.estimate": "1024",
    "hoodie.clustering.inline": "true",
    "hoodie.clustering.inline.max.commits": "5",
    "hoodie.clustering.plan.strategy.target.file.max.bytes": "125829120",
    "hoodie.clustering.plan.strategy.small.file.limit": "123731968",
    "hoodie.clustering.plan.strategy.sort.columns": "id_qualification",
    "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}

df.write.format("hudi") \
    .mode("append") \
    .options(**hudi_options) \
    .saveAsTable(f"bronze.{TABLE_NAME}")
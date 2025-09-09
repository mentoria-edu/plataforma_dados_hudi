from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, when, isnan, isnull
from pyspark.sql.types import StringType, LongType, DoubleType, TimestampType

TABLE_NAME_BRONZE = "bronze.legal_nature"
TABLE_NAME_SILVER = "legal_nature"

spark = SparkSession.builder.appName(f"silver_{TABLE_NAME_SILVER}").getOrCreate()

df = spark.read.table(TABLE_NAME_BRONZE)

df = df \
    .withColumn("id_legal_nature", trim(col("id_legal_nature"))) \
    .withColumn("description", trim(col("description"))) \
    
df = df.filter(
    col("id_legal_nature").isNotNull() & 
    (col("id_legal_nature") != "") & 
    col("description").isNotNull() & 
    (col("description") != "")
)

df = df.dropDuplicates(["id_legal_nature"])

hudi_options = {
    "hoodie.table.name": TABLE_NAME_SILVER,
    "hoodie.datasource.write.recordkey.field": "id_legal_nature",
    "hoodie.datasource.write.operation": "insert",
    "hoodie.parquet.small.file.limit": "0",
    "hoodie.parquet.max.file.size": "125829120", 
    "hoodie.copyonwrite.record.size.estimate": "1024",
    "hoodie.clustering.inline": "true",
    "hoodie.clustering.inline.max.commits": "5",
    "hoodie.clustering.plan.strategy.target.file.max.bytes": "125829120",
    "hoodie.clustering.plan.strategy.small.file.limit": "123731968",
    "hoodie.clustering.plan.strategy.sort.columns": "id_legal_nature",
    "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

df.write.format("hudi") \
    .mode("overwrite") \
    .options(**hudi_options) \
    .saveAsTable(f"silver.{TABLE_NAME_SILVER}")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, when, isnan, isnull
from pyspark.sql.types import StringType, LongType, DoubleType, TimestampType

TABLE_NAME_BRONZE = "bronze.data_bureau"
TABLE_NAME_SILVER = "empresas_rf_silver"

spark = SparkSession.builder.appName("silver_data_bureau").getOrCreate()

df = spark.read.table(TABLE_NAME_BRONZE)

df = df \
    .withColumn("cnpj", trim(col("cnpj"))) \
    .withColumn("company_name", trim(col("company_name"))) \
    .withColumn("company_size", trim(col("company_size"))) 
    

hudi_options = {
    "hoodie.table.name": TABLE_NAME_SILVER,
    "hoodie.datasource.write.recordkey.field": "cnpj",
    "hoodie.datasource.write.operation": "insert",
    "hoodie.parquet.small.file.limit": "134217728",
    "hoodie.parquet.max.file.size": "1073741824",
    "hoodie.copyonwrite.record.size.estimate": "1024",
    "hoodie.clustering.inline": "true",
    "hoodie.clustering.inline.max.commits": "1",
    "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",
    "hoodie.clustering.plan.strategy.small.file.limit": "134217728",
    "hoodie.clustering.plan.strategy.sort.columns": "cnpj",
    "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

df.write.format("hudi") \
    .mode("overwrite") \
    .options(**hudi_options) \
    .saveAsTable("silver.data_bureau")
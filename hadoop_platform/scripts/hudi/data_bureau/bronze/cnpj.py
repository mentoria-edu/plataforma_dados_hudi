from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, LongType, DoubleType, TimestampType

PATH_CSV_FILE = "/csvs/empresas_tst.csv"
TABLE_NAME = "empresas_rf"

spark = SparkSession.builder.appName("bronze_data_bureau").getOrCreate()

df = spark.read.csv(
    PATH_CSV_FILE,
    sep=";",
    header=False,
    inferSchema=False
)

df = df.withColumnsRenamed({
    "_c0": "cnpj",
    "_c1": "company_name",
    "_c2": "legal_nature",
    "_c3": "responsible_qualification",
    "_c4": "share_capital",
    "_c5": "company_size",
    "_c6": "federative_entity"
})

df = df.na.fill({"company_size": "00"})
df = df.withColumn("created_at", current_timestamp())

df = df \
    .withColumn("cnpj", col("cnpj").cast(StringType())) \
    .withColumn("company_name", col("company_name").cast(StringType())) \
    .withColumn("legal_nature", col("legal_nature").cast(StringType())) \
    .withColumn("responsible_qualification", col("responsible_qualification").cast(StringType())) \
    .withColumn("share_capital", col("share_capital").cast(DoubleType())) \
    .withColumn("company_size", col("company_size").cast(StringType())) \
    .withColumn("federative_entity", col("federative_entity").cast(StringType())) \
    .withColumn("created_at", col("created_at").cast(TimestampType()))

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.recordkey.field": "cnpj",
    "hoodie.datasource.write.operation": "insert",
    "hoodie.parquet.small.file.limit": "0",
    "hoodie.parquet.max.file.size": "125829120", 
    "hoodie.copyonwrite.record.size.estimate": "1024",
    "hoodie.clustering.inline": "true",
    "hoodie.clustering.inline.max.commits": "5",
    "hoodie.clustering.plan.strategy.target.file.max.bytes": "125829120",
    "hoodie.clustering.plan.strategy.small.file.limit": "123731968",
    "hoodie.clustering.plan.strategy.sort.columns": "cnpj",
    "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}

df.write.format("hudi") \
    .mode("append") \
    .options(**hudi_options) \
    .saveAsTable("bronze.data_bureau")
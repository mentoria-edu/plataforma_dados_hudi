from pyspark.sql import SparkSession


TABLE_NAME = "data_bureau"
spark = SparkSession.builder.appName("gold_data_bureau").getOrCreate()

query = '''
SELECT 
   silver.data_bureau.cnpj,
   silver.data_bureau.company_name, 
   silver.data_bureau.legal_nature AS id_legal_nature,
   silver.legal_nature.description AS description_legal_nature,
   silver.data_bureau.responsible_qualification AS id_responsible_qualification,
   silver.responsible_qualification.description AS description_responsible_qualification,
   silver.data_bureau.share_capital,
   silver.data_bureau.company_size,
   silver.data_bureau.federative_entity
FROM
    silver.data_bureau
LEFT JOIN
    silver.legal_nature
ON
    silver.data_bureau.legal_nature = silver.legal_nature.id_legal_nature
LEFT JOIN
    silver.responsible_qualification
ON
    silver.data_bureau.responsible_qualification = silver.responsible_qualification.id_qualification;
'''

table = spark.sql(query)
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

spark.sql("CREATE DATABASE IF NOT EXISTS gold")

table.write.format("hudi") \
    .mode("append") \
    .options(**hudi_options) \
    .saveAsTable(f"gold.{TABLE_NAME}")
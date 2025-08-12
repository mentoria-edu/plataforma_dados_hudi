from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

query = '''
CREATE TABLE hudi_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI
PARTITIONED BY (city);
'''
spark.sql(query)
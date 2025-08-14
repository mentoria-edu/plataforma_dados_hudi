from pyspark.sql.functions import lit, col
from pyspark.sql import SparkSession

tableName = "trips_table"
basePath = "file:///tmp/trips_table"

spark = SparkSession.builder \
    .appName("first_hudi_script") \
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
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("test")    
        # .config("hive.metastore.uris", "thrift://masternode:9083")
        # .enableHiveSupport()
        .getOrCreate()
)

spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES").show()
spark.table("gold.data_bureau").show()




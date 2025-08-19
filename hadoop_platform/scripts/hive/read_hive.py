from pyspark.sql import SparkSession

spark = (
            SparkSession.builder.appName("test")    
                .config("hive.metastore.uris", "thrift://masternode:9083")
                .enableHiveSupport()
                .getOrCreate()
        )
spark.sql("SHOW DATABASES").show()
spark.sql("USE bronze")
spark.sql("SHOW TABLES").show()
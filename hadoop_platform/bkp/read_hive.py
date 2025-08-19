from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
spark.sql("SHOW DATABASES").show()
spark.sql("USE bronze")
spark.sql("SHOW TABLES").show()
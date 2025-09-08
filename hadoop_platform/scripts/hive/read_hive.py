from pyspark.sql import SparkSession

spark = (
            SparkSession.builder.appName("test")    
                .config("hive.metastore.uris", "thrift://masternode:9083")
                .enableHiveSupport()
                .getOrCreate()
        )
spark.sql("SHOW DATABASES").show()
# spark.sql("USE bronze")
# spark.sql("DROP TABLE IF EXISTS bronze.data_bureau;")
spark.sql("USE silver")
spark.sql("SHOW TABLES").show()

df = spark.table("silver.data_bureau").show()
# query = '''
#     SELECT cnpj, COUNT(cnpj) AS qtd
#     FROM bronze.teste_csv
#     GROUP BY cnpj
#     HAVING COUNT(cnpj) > 1
#     ORDER BY cnpj DESC;
# '''

# query_2 = '''
#     SELECT porte, COUNT(porte) AS qtd 
#     FROM bronze.teste_csv
#     GROUP BY porte;
# '''
# df = spark.table("bronze.teste_csv")

# df = df.select("porte").na.fill("00")
# df.groupBy("porte").count().show()


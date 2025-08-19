from pyspark.sql import SparkSession
from pyspark.sql.functions import col

tableName = "bisteca_frita_voadora_2"
# basePath = f"hdfs://masternode:9000/lakehouse/{tableName}"  # caminho da tabela Hudi
basePath = f"hdfs://masternode:9000/lakehouse/"  # caminho da tabela Hudi

try:
    spark = SparkSession.builder \
        .appName("update_script_hudi_2") \
        .config("hive.metastore.uris", "thrift://masternode:9083") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    print("SparkSession criada com sucesso!")
except Exception as e:
    print(f"Erro ao criar SparkSession: {e}")
    exit(1)


###

columns = ["ts","uuid","rider","driver","fare","city"]
data = [(1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
        (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70,"san_francisco"),
        (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90,"san_francisco"),
        (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
        (1695115999911,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai")]

inserts = spark.createDataFrame(data).toDF(*columns)

###





hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.datasource.write.partitionpath.field': 'city',
    # 'hoodie.datasource.write.operation': 'upsert',
    # 'hoodie.datasource.write.table.type': 'cow'
}

# # Ler registros a serem atualizados
updatesDf = spark.read.format("hudi").load(basePath) \
    .withColumn("fare", col("fare") * 100)

# Escrever atualizações
# updatesDf.write.format("hudi") \
#     .options(**hudi_options) \
#     .mode("append") \
#     .save(basePath)

updatesDf.write.format("hudi") \
    .mode("append") \
    .options(**hudi_options) \
    .saveAsTable("bronze.batata_frita_voadora")
    # .options(**hudi_options) \


spark.table("bronze.batata_frita_voadora").show()

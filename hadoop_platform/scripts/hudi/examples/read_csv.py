from pyspark.sql import SparkSession

PATH_CSV_FILE = "/csvs/empresas_tst.csv"

spark = SparkSession.builder.appName("tst_read_csv").getOrCreate()

df = spark.read.csv(
    PATH_CSV_FILE,
    sep=";"
)

df = df.withColumnsRenamed(
                            {
                            "_c0": "cnpj",
                            "_c1": "razao_social",
                            "_c2": "natureza_juridica",
                            "_c3": "qualificacao_responsavel",
                            "_c4": "capital_social",
                            "_c5": "porte",
                            "_c6": "ente_federativo"
                            }
                        )

df.show()
from pyspark.sql import SparkSession

# Versão do Spark (ajuste conforme necessário)
spark = SparkSession.builder \
    .appName("Leitura de JSON e Escrita em Delta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")
# Teste com um DataFrame simples
data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["name", "value"])

# Caminho para salvar o Delta Table
caminho_delta = "/home/leandro/Eng-de-dados/data/games/bronze"

df.write.format("delta").mode("overwrite").save(caminho_delta)

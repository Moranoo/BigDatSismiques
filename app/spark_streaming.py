from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# Création de la session Spark avec support pour Kafka
spark = SparkSession.builder \
    .appName("AnalyseSismiqueTemporelle") \
    .getOrCreate()

# Schéma des données entrantes
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("secousse", BooleanType()),
    StructField("magnitude", DoubleType()),
    StructField("tension_entre_plaque", DoubleType())
])

# Lecture des données de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic1") \
    .load()

# Extraction des données JSON
df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calculs et analyses en streaming
# Vous pouvez ajouter des calculs comme le calcul de l’amplitude et l’identification des motifs
df = df.withColumn("amplitude", col("magnitude") * col("tension_entre_plaque"))

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

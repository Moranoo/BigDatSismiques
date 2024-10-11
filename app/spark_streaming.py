# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType

# Créez une session Spark
spark = SparkSession.builder \
    .appName("Analyse des Données Sismiques Avancée") \
    .getOrCreate()

# Mute les logs inférieurs au niveau WARN
spark.sparkContext.setLogLevel("WARN")

# Configuration de Kafka
kafka_brokers = "kafka:9092"
topic_name = "seismic-data"

# Schéma des données sismiques
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("magnitude", DoubleType()) \
    .add("location", StringType())

# Consommez les données depuis Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", topic_name) \
    .load()

# Analyse des données
df = df.selectExpr("CAST(value AS STRING)") \
       .select(from_json(col("value"), schema).alias("data")) \
       .select("data.*")

# Convertir le timestamp en un format de temps utilisable
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Ajout d'un watermark pour gérer l'agrégation dans un flux de streaming
windowed_avg = df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(df.timestamp, "10 minutes", "5 minutes")) \
    .agg(avg("magnitude").alias("average_magnitude"))

# Détection de séquences prédictives
prediction = windowed_avg.withColumn("seismic_alert", col("average_magnitude") > 5.0)

# Affichage des résultats avec prédiction en temps réel en utilisant "complete" mode
query = prediction.writeStream \
                  .outputMode("complete") \
                  .format("console") \
                  .start()

query.awaitTermination()

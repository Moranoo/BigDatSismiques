from pyspark.sql import SparkSession

SparkSession.builder.appName("ScriptStreaming").getOrCreate()

# je select mon topic
kafka_topic = "topic1"

#Serveur Kafka
kafka_server = "localhost:9092"

#recuperation de mes data de mon stream kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_server).option("subscribe", kafka_topic).load()

#cast de mes data en string pour les rendres lisibles
df = df.selectExpr("CAST(value AS STRING)")

query = df.writeStream.outputMode("append").format("console").start()



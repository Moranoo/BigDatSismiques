# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyhive import hive
from pyspark.sql.functions import col, avg, date_format, lag
from pyspark.sql.window import Window

# Configuration de la connexion Hive
hive_host = 'hive-server'
hive_port = 10000
hive_database = 'data_sismique'

# Création de la session Spark avec support Hive
spark = SparkSession.builder.appName("AnalyseSismique").enableHiveSupport().getOrCreate()

# Lecture des datasets
df = spark.read.csv("./dataset_sismique.csv", header=True)
df_ville = spark.read.csv("./dataset_sismique_villes.csv", header=True)

# Renommer les colonnes pour éviter les espaces et pour plus de clarté
df = df.withColumnRenamed("date", "date_secousse") \
       .withColumnRenamed("tension entre plaque", "tension")

df_ville = df_ville.withColumnRenamed("date", "date_secousse") \
                   .withColumnRenamed("tension entre plaque", "tension")

# Affichage des datasets
df.show()
df_ville.show()

# Connexion à la base de données Hive
conn = hive.Connection(host=hive_host, port=hive_port, database=hive_database)
cursor = conn.cursor()

# Création des tables Hive
cursor.execute("""
CREATE TABLE IF NOT EXISTS earthquake (
    date_secousse STRING,
    secousse BOOLEAN,
    magnitude DOUBLE,
    tension DOUBLE
)
STORED AS PARQUET
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS earthquake_cities (
    date_secousse STRING,
    ville STRING,
    secousse BOOLEAN,
    magnitude DOUBLE,
    tension DOUBLE
)
STORED AS PARQUET
""")

# Fonction pour insérer les données si la table est vide
def insert_data_if_empty(cursor, table_name, dataframe, insert_query):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    if count == 0:
        for row in dataframe.collect():
            cursor.execute(insert_query, row)
        conn.commit()
        print(f"Données insérées avec succès dans la table Hive `{table_name}`.")
    else:
        print(f"Les données existent déjà dans la table Hive `{table_name}`, aucune insertion nécessaire.")

# Insertion des données dans les tables respectives
insert_data_if_empty(cursor, "earthquake", df, "INSERT INTO TABLE earthquake VALUES (%s, %s, %s, %s)")
insert_data_if_empty(cursor, "earthquake_cities", df_ville, "INSERT INTO TABLE earthquake_cities VALUES (%s, %s, %s, %s, %s)")

# Analyse sur les données Hive
def run_analysis(table_name, schema, with_ville=False):
    cursor.execute(f"SELECT * FROM {table_name}")
    data = spark.createDataFrame(cursor.fetchall(), schema=schema)

    # Calcul de l'amplitude des signaux
    data = data.withColumn("amplitude", col("magnitude") * col("tension"))
    data.show()

    # Identification des périodes d'activité sismique importante
    threshold = 5.0
    severe_activity = data.filter(col("amplitude") > threshold)
    print(f"Périodes d'activité sismique intense dans `{table_name}` :")
    severe_activity.select("date_secousse", "magnitude", "tension", "amplitude").show()

    # Corrélation entre magnitude et tension
    correlation_mag_tension = data.stat.corr("magnitude", "tension")
    print(f"Corrélation entre magnitude et tension dans `{table_name}`: {correlation_mag_tension}")

    # Identification des événements potentiellement précurseurs
    window_spec = Window.partitionBy("ville").orderBy("date_secousse") if with_ville else Window.orderBy("date_secousse")
    data = data.withColumn("next_magnitude", lag("magnitude", -1).over(window_spec))
    print(f"Événements potentiellement précurseurs dans `{table_name}` :")
    data.select("date_secousse", "magnitude", "tension", "next_magnitude").show()

    # Agrégation par jour, heure, et minute ensemble, avec ville si disponible
    data = data.withColumn("day_hour_minute", date_format("date_secousse", "yyyy-MM-dd HH:mm"))
    if with_ville:
        agg_df = data.groupBy("day_hour_minute", "ville").agg(avg("magnitude").alias("avg_magnitude"))
        print(f"Moyenne des magnitudes par jour, heure, minute et ville dans `{table_name}` :")
        agg_df.orderBy("day_hour_minute", "ville").show()
    else:
        agg_df = data.groupBy("day_hour_minute").agg(avg("magnitude").alias("avg_magnitude"))
        print(f"Moyenne des magnitudes par jour, heure et minute dans `{table_name}` :")
        agg_df.orderBy("day_hour_minute").show()

# Exécution de l'analyse pour `earthquake`
run_analysis("earthquake", ["date_secousse", "secousse", "magnitude", "tension"])

# Exécution de l'analyse pour `earthquake_cities` en incluant `ville`
run_analysis("earthquake_cities", ["date_secousse", "ville", "secousse", "magnitude", "tension"], with_ville=True)

# Fermeture du curseur, de la connexion et de la session Spark
cursor.close()
conn.close()
spark.stop()

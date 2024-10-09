from pyspark.sql import SparkSession
from pyhive import hive

hive_host = 'hive-server'
hive_port = 10000
hive_database = 'data_sismique'

# création de la session Spark
spark = SparkSession.builder.appName("Lecture des csv").enableHiveSupport().getOrCreate()

# lecture du dataset
df = spark.read.csv("./dataset_sismique.csv", header=True)

# changement de nom de colonne pour plus de clarté
df = df.withColumnRenamed("date", "date_secousse") \
       .withColumnRenamed("secousse", "secousse") \
       .withColumnRenamed("magnitude", "magnitude") \
       .withColumnRenamed("tension entre plaque", "tension")

# affichage du dataset
df.show()

# vérifier le nombre de lignes
print(f"Nombre de lignes dans le csv: {df.count()}")

# informations de connexion bdd hive
conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    database=hive_database
)

# création d'un curseur
cursor = conn.cursor()

# création de la table si elle n'existe pas
create_table_query = """
CREATE TABLE IF NOT EXISTS earthquake (
    date_secousse STRING,
    secousse BOOLEAN,
    magnitude DOUBLE,
    tension DOUBLE
)
STORED AS PARQUET
"""
cursor.execute(create_table_query)

# insertion des nouvelles données dans la table Hive
# insert_query = """
# INSERT INTO TABLE earthquake VALUES (%s, %s, %s, %s)
# """

# for row in df.collect():
#     cursor.execute(insert_query, (row.date_secousse, row.secousse, row.magnitude, row.tension))

# conn.commit()

# print("Données sismiques insérées avec succès dans la table Hive.")

# Récuperer les données insérées
cursor.execute("SELECT * FROM earthquake")

# lecture depuis Hive et affichage des données
print("Données sismiques dans la table Hive:")
df_hive = spark.createDataFrame(cursor.fetchall(), df.schema)
df_hive.show()

# vérification du nombre de lignes dans la table
cursor.execute("SELECT COUNT(*) FROM earthquake")
print(cursor.fetchone()[0])

# Vérifier les valeurs aberrantes
cursor.execute("SELECT * FROM earthquake WHERE magnitude > 10")
print(cursor.fetchall())

# Vérifier les valeurs manquantes
cursor.execute("SELECT * FROM earthquake WHERE magnitude IS NULL")
print(cursor.fetchall())

# Vérifier les doublons
cursor.execute("SELECT * FROM earthquake GROUP BY date_secousse, secousse, magnitude, tension HAVING COUNT(*) > 1")
print(cursor.fetchall())

# Vérifier les valeurs None
cursor.execute("SELECT * FROM earthquake WHERE secousse IS NULL OR magnitude IS NULL OR tension IS NULL")
print(cursor.fetchall())

#et NaN
cursor.execute("SELECT * FROM earthquake WHERE magnitude = 'NaN'")
print(cursor.fetchall())

# fermeture du curseur et de la connexion
cursor.close()
conn.close()

# fermeture de la session Spark
spark.stop()

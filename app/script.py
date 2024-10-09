#
from pyspark.sql import SparkSession
from pyhive import hive

hive_host = 'hive-server'
hive_port = 10000
hive_database = 'data_sismique'

# creation session Spark
spark = SparkSession.builder.appName("Lecture des csv").getOrCreate()

# lecture du dataset
df = spark.read.csv("./dataset_sismique.csv", header='true')

# changement de nom de colonne pour plus de clarté
df = df.withColumnRenamed("tension entre plaque", "tension")

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

# creation d'un curseur
cursor = conn.cursor()

# creation table si elle n'existe pas
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

# insertion des données dans la table
insert_query = """
INSERT INTO TABLE earthquake VALUES (%s, %s, %s, %s)
"""

for row in df.collect():
    cursor.execute(insert_query, (row.date, row.secousse, row.magnitude, row.tension))

conn.commit()

print("Données sismiques insérées avec succès dans la table Hive.")

# vérification des données insérées
cursor.execute("SELECT * FROM earthquake LIMIT 5")
print(cursor.fetchall())

# vérification du nombre de lignes dans la table
cursor.execute("SELECT COUNT(*) FROM earthquake")
print(cursor.fetchone()[0])

# fermerture du curseur et de la connexion
cursor.close()
conn.close()

# fermeture de la session Spark
spark.stop()
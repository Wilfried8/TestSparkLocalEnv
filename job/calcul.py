from pyspark.sql import SparkSession
import logging

# Désactiver les logs INFO de Spark
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

def main():

    # Création de la session Spark
    spark = SparkSession.builder \
        .appName("SimpleSparkJob") \
        .getOrCreate()

    # Définir le niveau de journalisation à WARN pour Spark
    spark.sparkContext.setLogLevel("WARN")

    # Création d'un DataFrame à partir d'une liste de tuples
    data = [("John", 25), ("Alice", 30), ("Bob", 35), ("Tom", 14), ("Alice", 23), ("Bob", 16)]
    df = spark.createDataFrame(data, ["Name", "Age"])

    # Affichage du DataFrame
    df.show()

    # Opération de transformation : ajout d'une colonne
    df_with_status = df.withColumn("Status", df["Age"] > 24)

    # Affichage du DataFrame transformé
    df_with_status.show()

    # Arrêt de la session Spark
    spark.stop()

if __name__=="__main__":
    main()

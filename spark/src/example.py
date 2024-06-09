from pyspark.sql import SparkSession
import requests
import json
#spark = SparkSession.builder \
        #.appName("MiAplicacionSpark") \
        #.master("spark://localhost:7077") \
        #.getOrCreate()

spark = SparkSession \
    .builder \
    .appName('MiAplicacionSpark') \
    .master('local[*]') \
    .getOrCreate()
url = "https://cms.smitegame.com/wp-json/smite-api/all-gods/3"


try:
    response = requests.request("GET", url)
    data = response.json()
    with open('/usr/local/airflow/output/api_output.json', 'w') as f:
        json.dump(data, f)
    print(response.text)
    # Ejemplo de cómo realizar una operación simple: crear un DataFrame y mostrarlo
    data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    columns = ["Language", "Users"]

    df = spark.createDataFrame(data, schema=columns)
    df.show()
except Exception as e:
    print(f"Error al procesar datos con Spark: {e}")
finally:
    spark.stop()
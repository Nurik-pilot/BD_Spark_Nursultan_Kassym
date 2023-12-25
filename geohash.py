from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from geohash import encode

# Initialize SparkSession for generating geohash
spark = SparkSession.builder.appName("GeohashGeneration").getOrCreate()

file_path = "C:/Users/PCHelper/Documents/EPAM/spark_practical_task/spark_practice/raw/restaurant_csv/restaurant_data.csv"
restaurant_data = spark.read.option("header", "true").csv(file_path)

# Show the DataFrame schema and a few rows
restaurant_data.printSchema()
restaurant_data.show(5)

# Define UDF to generate geohash
@udf
def generate_geohash(lat, lon):
    return encode(float(lat), float(lon), precision=4)  
# Add a new column 'geohash' using the UDF
restaurant_data = restaurant_data.withColumn("geohash", generate_geohash("lat", "lng"))

# Show the DataFrame with the geohash column
restaurant_data.show(5)
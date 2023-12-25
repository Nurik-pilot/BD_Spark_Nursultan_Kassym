import pyspark
from pyspark.sql import SparkSession


# Initialize session for ETL job to read data
spark=SparkSession.builder.appName('ReadData').getOrCreate()
spark

# Read Parquet files into a DataFrame
path = 'C:/Users/PCHelper/Documents/EPAM/spark_practical_task/spark_practice/raw/weather/datasets'
df = spark.read.parquet(path)

# read the data
df.printSchema()
df.show(5)





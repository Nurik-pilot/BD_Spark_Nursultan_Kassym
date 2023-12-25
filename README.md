the ETL job project made on Apache Spark application

1. the folder raw contains weather data and restaurant data which are used in the project
2. spark.py file is reading data from the raw folder
3. check_null.py file checks the restaurant data for incorrect values in the latitude and longitude columns 
4. geohash.py generates a geohash by latitude and longitude using a geohash library like geohash-java.
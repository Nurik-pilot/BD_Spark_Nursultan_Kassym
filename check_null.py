import pyspark
from pyspark.sql import SparkSession
import requests

# Initialize session to check restaurant data for null values
spark = SparkSession.builder.appName("CheckRestaurantData").getOrCreate()

# Read the restaurant data into a DataFrame
path_rest = 'C:\Users\PCHelper\Documents\EPAM\spark_practical_task\spark_practice\raw\restaurant_csv'
restaurant_data = spark.read.option("header", "true").csv(path_rest)

# Filter rows with missing or incorrect latitude and longitude values
invalid_locations = restaurant_data.filter(
    (restaurant_data.latitude.isNull()) | 
    (restaurant_data.longitude.isNull()) | 
    (restaurant_data.latitude < -90) | 
    (restaurant_data.latitude > 90) | 
    (restaurant_data.longitude < -180) | 
    (restaurant_data.longitude > 180)
)

# Show the rows with missing or incorrect values
invalid_locations.show()


# Show a row with missing latitude and longitude
sample_row = invalid_locations.first()

# Connect API key and construct the API request
api_key = "5cb63669057c44fbade178427c91ae85"
address = sample_row.lat  
api_url = f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={api_key}"

# GET request to the API
response = requests.get(api_url)

# Process the API response to extract latitude and longitude
if response.status_code == 200:
    result = response.json()
    if result['total_results'] > 0:
        lat = result['results'][0]['geometry']['lat']
        lng = result['results'][0]['geometry']['lng']
    


spark.stop()
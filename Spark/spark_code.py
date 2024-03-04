from pyspark.sql import SparkSession
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("FlightDelayAnalysis") \
    .getOrCreate()

# Load data from S3 into DataFrame
s3_path = "s3://bigdata-assignment-video-demonstration/DelayedFlights-updated.csv"
df = spark.read.csv(s3_path, header=True, inferSchema=True)

# Register DataFrame as a temporary table
df.createOrReplaceTempView("delay_flights")

# Define queries for each type of delay
queries = {
    "CarrierDelay": "SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS CarrierDelay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "NASDelay": "SELECT Year, AVG((NASDelay / ArrDelay) * 100) AS NASDelay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "WeatherDelay": "SELECT Year, AVG((WeatherDelay / ArrDelay) * 100) AS WeatherDelay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "LateAircraftDelay": "SELECT Year, AVG((LateAircraftDelay / ArrDelay) * 100) AS LateAircraftDelay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "SecurityDelay": "SELECT Year, AVG((SecurityDelay / ArrDelay) * 100) AS SecurityDelay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
}

# Execute queries and measure execution time
for delay_type, query in queries.items():
    start_time = time.time()
    result = spark.sql(query).show()
    execution_time = time.time() - start_time
    print(f"Query for {delay_type} Delay executed in {execution_time:.2f} seconds")

# Stop Spark session
spark.stop()

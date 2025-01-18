from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read CSV Example") \
    .getOrCreate()

# Read data from a CSV file
df = spark.read.csv("path/to/your/file.csv", header=True, inferSchema=True)

# Show the data
df.show()

# Print schema
df.printSchema()

# Stop the Spark session
spark.stop()

from pyspark.sql import SparkSession

# Step 1: Start Spark session
spark = SparkSession.builder.appName("Task 1 - Load & Explore").getOrCreate()

# Step 2: Load CSV
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Step 3: Create Temp View
df.createOrReplaceTempView("sensor_readings")

# Step 4: Show first 5 rows
print("First 5 rows:")
df.show(5)

# Step 5: Count records
print("Total number of records:", df.count())

# Step 6: Distinct locations and sensor types
print("Distinct locations:")
spark.sql("SELECT DISTINCT location FROM sensor_readings").show()

print("Distinct sensor types:")
spark.sql("SELECT DISTINCT sensor_type FROM sensor_readings").show()

# Step 7: Save full DataFrame
df.write.csv("task1_output.csv", header=True, mode="overwrite")

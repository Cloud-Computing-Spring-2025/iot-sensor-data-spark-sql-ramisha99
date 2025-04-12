from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour

spark = SparkSession.builder.appName("Task 3 - Time-Based").getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Extract hour
df_with_hour = df.withColumn("hour_of_day", hour("timestamp"))

# Group by hour
avg_by_hour = df_with_hour.groupBy("hour_of_day") \
    .avg("temperature") \
    .withColumnRenamed("avg(temperature)", "avg_temp")

avg_by_hour.orderBy("avg_temp", ascending=False).show()

avg_by_hour.write.csv("task3_output.csv", header=True, mode="overwrite")

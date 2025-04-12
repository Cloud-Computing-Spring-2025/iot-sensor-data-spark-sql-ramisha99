from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour

spark = SparkSession.builder.appName("Task 5 - Pivot Table").getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("hour_of_day", hour("timestamp"))

pivot_df = df.groupBy("location").pivot("hour_of_day").avg("temperature")
pivot_df.show()

pivot_df.write.csv("task5_output.csv", header=True, mode="overwrite")

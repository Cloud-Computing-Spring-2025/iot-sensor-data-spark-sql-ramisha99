from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Task 2 - Filtering & Aggregation").getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Filter ranges
in_range = df.filter((df.temperature >= 18) & (df.temperature <= 30))
out_of_range = df.filter((df.temperature < 18) | (df.temperature > 30))

print("In-range rows:", in_range.count())
print("Out-of-range rows:", out_of_range.count())

# Group by location
agg_df = df.groupBy("location").agg(
    {"temperature": "avg", "humidity": "avg"}
).withColumnRenamed("avg(temperature)", "avg_temperature") \
 .withColumnRenamed("avg(humidity)", "avg_humidity")

agg_df.orderBy("avg_temperature", ascending=False).show()

agg_df.write.csv("task2_output.csv", header=True, mode="overwrite")

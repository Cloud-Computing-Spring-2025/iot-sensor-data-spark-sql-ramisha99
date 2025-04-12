from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Task 4 - Window Function").getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

sensor_avg = df.groupBy("sensor_id").agg(avg("temperature").alias("avg_temp"))

# Ranking
window_spec = Window.orderBy(sensor_avg["avg_temp"].desc())
ranked = sensor_avg.withColumn("rank_temp", dense_rank().over(window_spec))

ranked.orderBy("rank_temp").show(5)

ranked.write.csv("task4_output.csv", header=True, mode="overwrite")

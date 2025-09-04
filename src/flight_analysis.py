
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, desc, rank
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Flight Data Analysis") \
    .getOrCreate()

# Load dataset
df = spark.read.csv("data/flights.csv", header=True, inferSchema=True)

# Preview
print("Initial Schema:")
df.printSchema()
print("Sample Data:")
df.show(5)

# Data Cleaning
df = df.dropDuplicates()
df = df.na.drop(subset=["ARRIVAL_DELAY", "DEPARTURE_DELAY", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"])

# Create delay category
df = df.withColumn(
    "DELAY_CATEGORY",
    when(col("ARRIVAL_DELAY") <= 0, "On Time")
    .when(col("ARRIVAL_DELAY") <= 15, "Slight Delay")
    .otherwise("Heavy Delay")
)

# Average delay per airline
avg_delay = df.groupBy("AIRLINE").agg(avg("ARRIVAL_DELAY").alias("AVG_ARRIVAL_DELAY"))
avg_delay.orderBy(desc("AVG_ARRIVAL_DELAY")).show(5)

# Top 5 airports with most delays
top_airports = df.groupBy("ORIGIN_AIRPORT").agg(count("*").alias("TOTAL_FLIGHTS"))
top_airports.orderBy(desc("TOTAL_FLIGHTS")).show(5)

# Window Function: Rank airlines by avg delay
window_spec = Window.orderBy(desc("AVG_ARRIVAL_DELAY"))
ranked_airlines = avg_delay.withColumn("RANK", rank().over(window_spec))
ranked_airlines.show(5)

# On-time vs Delayed percentage
on_time_stats = df.groupBy("DELAY_CATEGORY").agg(count("*").alias("COUNT"))
on_time_stats.show()

# Save results
avg_delay.write.mode("overwrite").csv("output/avg_delay")
ranked_airlines.write.mode("overwrite").parquet("output/ranked_airlines")

print("Analysis complete. Results saved in output/ folder.")
spark.stop()

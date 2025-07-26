import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, year, month, round as spark_round
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Internship Task 1 - Graph Output") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

file_path = r"C:\Users\Lavar\OneDrive\Desktop\internship\data.csv"
output_txt = r"C:\Users\Lavar\OneDrive\Desktop\internship\monthly_summary.txt"
output_img = r"C:\Users\Lavar\OneDrive\Desktop\internship\monthly_summary.png"

# Load and clean data
df = spark.read.csv(file_path, header=True, inferSchema=True)

df_clean = df.filter(
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (year(col("tpep_pickup_datetime")) >= 2009) &
    (year(col("tpep_pickup_datetime")) <= 2025)
)

df_clean = df_clean.withColumn("year", year(col("tpep_pickup_datetime"))) \
                   .withColumn("month", month(col("tpep_pickup_datetime")))

summary_df = df_clean.groupBy("year", "month") \
    .agg(
        spark_round(avg("fare_amount"), 2).alias("average_fare"),
        count("*").alias("trip_count")
    ) \
    .orderBy("year", "month")

# Save text output
results = summary_df.collect()
with open(output_txt, "w") as f:
    f.write("=== Monthly Fare Summary ===\n")
    f.write(f"{'Year':<6}{'Month':<6}{'Avg Fare':<10}{'Trip Count':<10}\n")
    f.write("-" * 32 + "\n")
    for row in results:
        f.write(f"{row['year']:<6}{row['month']:<6}{row['average_fare']:<10}{row['trip_count']:<10}\n")

# Convert to pandas for plotting
pandas_df = summary_df.toPandas()

# Create a combined year-month label for x-axis
pandas_df["label"] = pandas_df["year"].astype(str) + "-" + pandas_df["month"].astype(str).str.zfill(2)

# Plot graph
plt.figure(figsize=(12, 6))
plt.plot(pandas_df["label"], pandas_df["average_fare"], marker='o', label="Average Fare", color='blue')
plt.bar(pandas_df["label"], pandas_df["trip_count"], alpha=0.3, label="Trip Count", color='orange')
plt.xticks(rotation=45)
plt.title("Monthly Average Fare and Trip Count")
plt.xlabel("Year-Month")
plt.ylabel("Value")
plt.legend()
plt.tight_layout()
plt.grid(True)

# Save graph
plt.savefig(output_img)
print(f"\n Graph saved to: {output_img}")

spark.stop()

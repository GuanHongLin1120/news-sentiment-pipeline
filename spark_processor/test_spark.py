from pyspark.sql import SparkSession

# Create a local Spark session (uses all CPU cores on your Mac)
spark = SparkSession.builder \
    .appName("SparkTest") \
    .master("local[*]") \
    .getOrCreate()

# Reduce log noise so we can see our output clearly
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 50)
print(f"Spark version: {spark.version}")
print("=" * 50 + "\n")

# Create a tiny test DataFrame to prove Spark works
data = [("AAPL", 312), ("TSM", 180), ("NVDA", 950)]
df = spark.createDataFrame(data, ["ticker", "price"])

print("Test DataFrame:")
df.show()

spark.stop()
print("Spark works correctly!")
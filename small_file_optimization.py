from pyspark.sql import SparkSession

# -----------------------------
# 1. Initialize Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("SmallFileOptimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "256m") \
    .config("spark.sql.files.openCostInBytes", "4m") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# -----------------------------
# 2. Read Small Parquet Files
# -----------------------------
input_path = "s3://your-bucket/input-parquet/"  # Change to your source path
df = spark.read.parquet(input_path)

print(f"Initial partitions: {df.rdd.getNumPartitions()}")

# -----------------------------
# 3. Optimize Partitions
# -----------------------------
# Option 1: Coalesce (no shuffle, good for reducing partitions)
optimized_df = df.coalesce(10)

# Option 2: Repartition (shuffle, better for balancing data)
# optimized_df = df.repartition(10)

print(f"Optimized partitions: {optimized_df.rdd.getNumPartitions()}")

# -----------------------------
# 4. Write Optimized Output
# -----------------------------
output_path = "s3://your-bucket/output-parquet/"  # Change to your destination path
optimized_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("✅ Small files compacted successfully!")

# -----------------------------
# 5. Stop Spark Session
# -----------------------------
spark.stop()

# PySpark job fails only in production - investigation approach

# 1. Capture full environment differences

# Production vs Staging checklist:
# - Spark versions and configurations
# - Data volume and skew patterns
# - Resource allocation (memory/cores)
# - Network topology and S3/Kafka latencies
# - Security policies (encryption, auth)

# 2. Enable comprehensive logging
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "/logs/spark-events")
spark.conf.set("spark.sql.adaptive.enabled", "false")  # Disable AQE temporarily

# 3. Collect production diagnostics
def diagnostic_checkpoint(df):
    print(f"Partitions: {df.rdd.getNumPartitions()}")
    print(f"Size estimate: {df.count() * 0.001} KB per row")  # Approx
    sample = df.limit(1000).toPandas()
    print(f"Null counts:\n{sample.isnull().sum()}")
    return df

# 4. Binary search in pipeline stages
# Comment out stages until job succeeds, then uncomment gradually

# 5. Compare query plans
prod_plan = spark.sql("EXPLAIN EXTENDED SELECT ...").collect()
dev_plan = spark.sql("EXPLAIN EXTENDED SELECT ...").collect()

# 6. Check for hidden data differences
# Production may have:
# - Corrupted parquet footers
# - Schema evolution issues (nested structs)
# - Unicode/encoding problems
# - Extremely long strings (>2GB)

# 7. Use Delta's time travel to compare
df_prod_v1 = spark.read.format("delta").option("versionAsOf", 1).load("/prod/delta")
df_prod_v2 = spark.read.format("delta").option("versionAsOf", 2).load("/prod/delta")
df_prod_v2.subtract(df_prod_v1).show()

# 8. Simulate production load
# Copy a week of production data to staging and reproduce
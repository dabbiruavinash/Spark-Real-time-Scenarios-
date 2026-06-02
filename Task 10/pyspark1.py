# Optimizing PySpark window functions on billions of rows

# Core strategies for window functions at scale

# Strategy 1: Partition wisely - avoid global windows
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sum, avg, col

# BAD: Global window - single partition
window_all = Window.orderBy("timestamp")  # DO NOT DO THIS

# GOOD: Partition by natural grouping
window_partitioned = Window.partitionBy("device_id", "date") \
                           .orderBy("timestamp") \
                           .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Strategy 2: Use rangeBetween for time-based windows (more efficient)
window_time = Window.partitionBy("device_id") \
                    .orderBy("timestamp") \
                    .rangeBetween(-3600, 0)  # Last hour in seconds

# Strategy 3: Bucket timestamps to reduce sort complexity
from pyspark.sql.functions import date_format, hour

df_bucketed = df.withColumn("hour_bucket", hour(col("timestamp"))) \
                .repartition(200, "device_id", "hour_bucket")  # Pre-partition

# Strategy 4: Break complex windows into multiple stages
# Instead of one window with 10 functions:
# Stage 1: Compute running totals
df1 = df.withColumn("running_sum", sum("value").over(window))

# Stage 2: Compute rankings separately (uses different partition)
window_rank = Window.partitionBy("device_id").orderBy(col("value").desc())
df2 = df1.withColumn("rank", row_number().over(window_rank))

# Strategy 5: Use Delta Z-order before window operations
spark.sql("OPTIMIZE delta.`/path` ZORDER BY (device_id, timestamp)")

# Strategy 6: Enable sort-based shuffle join
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.window.execution.preferSortMerge", "true")

# Strategy 7: Pre-aggregate where possible
df_aggregated = df.groupBy("device_id", "date", "hour") \
                   .agg(sum("value").alias("hourly_total"))

# Then apply window on smaller aggregated data
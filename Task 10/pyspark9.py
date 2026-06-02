# Optimizing shuffle-heavy transformations

# Comprehensive shuffle optimization strategies

# Strategy 1: Reduce shuffle data volume
# Instead of shuffling all columns
df.select("key", "value")  # Select only needed columns before shuffle
df.filter("status = 'active'")  # Filter before shuffle
df.dropDuplicates(["key"])  # Deduplicate before shuffle

# Strategy 2: Use bucketing for repeated shuffles
df.write.bucketBy(500, "key") \
    .sortBy("timestamp") \
    .option("path", "/bucketed/data") \
    .saveAsTable("bucketed_table")

# Strategy 3: Replace groupByKey with reduceByKey
# BAD
df.groupBy("key").agg(collect_list("value"))

# GOOD - custom aggregation
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import ArrayType, StringType

def merge_values(values):
    return list(set(values))  # Deduplicate in-place

merge_udf = udf(merge_values, ArrayType(StringType()))
df.groupBy("key").agg(merge_udf(collect_list("value")))

# Strategy 4: Use appropriate partition counts
def calculate_optimal_partitions(data_size_gb, executor_cores, num_executors):
    # Target partition size: 200MB-1GB
    target_partition_size_mb = 256
    partitions_by_size = (data_size_gb * 1024) / target_partition_size_mb
    
    # Based on parallelism
    total_cores = executor_cores * num_executors
    partitions_by_cores = total_cores * 2  # 2x for work balancing
    
    return int(max(partitions_by_size, partitions_by_cores))

# Strategy 5: Use coalesce for partition reduction
# Shuffle stage 1: Process in 2000 partitions
df_intermediate = df_initial \
    .repartition(2000, "key") \
    .groupBy("key") \
    .agg(sum("value").alias("total"))

# Stage 2: Reduce to 100 for final output
df_final = df_intermediate.coalesce(100)  # No shuffle!

# Strategy 6: Enable pushdown for aggregations
spark.conf.set("spark.sql.optimizer.aggregatePushdown", "true")

# Strategy 7: Use Delta Z-order before shuffle-heavy operations
spark.sql("OPTIMIZE delta.`/data` ZORDER BY (shuffle_key)")

# Strategy 8: Implement custom partitioner for skewed data
from pyspark import HashPartitioner

class SaltingPartitioner(HashPartitioner):
    def __init__(self, partitions, salt_factor=10):
        super().__init__(partitions)
        self.salt_factor = salt_factor
    
    def __call__(self, key):
        if key in self.skewed_keys:
            salt = random.randint(0, self.salt_factor - 1)
            salted_key = f"{key}_{salt}"
            return hash(salted_key) % self.numPartitions
        return hash(key) % self.numPartitions

# Strategy 9: Minimize shuffle in window functions
# Instead of global window, use tumbling windows
df.withColumn("window_start", floor(unix_timestamp("timestamp") / 3600) * 3600) \
  .groupBy("key", "window_start") \
  .agg(sum("value").alias("hourly_sum"))

# Strategy 10: Tune shuffle parameters
spark.conf.set("spark.sql.shuffle.partitions", "2000")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "200")
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")  # Increase from 48m
spark.conf.set("spark.shuffle.file.buffer", "128k")  # Increase from 32k
spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "400")  # For small shuffles
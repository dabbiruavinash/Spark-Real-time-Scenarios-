# How Adaptive Query Execution works internally and where it fails

# AQE components and workflow
"""
1. STAGE SUBMISSION
   - Initial query plan with logical optimizations
   - Physical plan with placeholders for unknown sizes

2. SHUFFLE QUERY STAGE (Runtime statistics collection)
   - Executes map stages, collects shuffle statistics
   - Records: partition sizes, data distribution

3. REOPTIMIZATION (After each shuffle)
   - Coalescing small partitions: merges partitions < minPartitionSize
   - Handling skew: splits skewed partitions (> skewFactor * median)
   - Switching join strategies: SortMerge -> Broadcast if small enough
   - Optimizing sort orders: removes unnecessary sorts

4. OPTIMIZED EXECUTION
   - Replaces placeholders with optimized plans
   - Continues execution with better parallelism
"""

# When AQE fails:

# Case 1: Partition size estimation wrong
"""
Problem: Data is highly compressible in memory but not on disk
- AQE estimates based on serialized sizes
- In-memory size can be 10x larger, causing OOM
Fix: spark.sql.adaptive.coalescePartitions.parallelismFirst=false
"""

# Case 2: Skew detection fails with key cardinality explosion
"""
Problem: 2 billion unique keys, but one key repeated 1M times
- AQE detects skew based on partition size, not key frequency
- Large number of small partitions masks the skew
Fix: Manual salting + spark.sql.adaptive.skewJoin.skewedPartitionFactor=10
"""

# Case 3: Broadcast threshold misjudgment
"""
Problem: Table fits in memory but broadcast takes too long
- AQE decides to broadcast after map stage
- Serialization/deserialization overhead > shuffle cost
- Network bandwidth between executors is bottleneck
Fix: spark.sql.adaptive.autoBroadcastJoinThreshold=10485760 (10MB)
"""

# Case 4: Dynamic partition pruning fails with non-deterministic filters
"""
Problem: Filter based on rand() or current_timestamp()
- DPP cannot prune at planning time
- Results in full table scan
Fix: Materialize deterministic filters first
"""

## 5. Handling late-arriving shipment events

```python
from pyspark.sql.functions import from_json, col, when, current_timestamp, expr
from pyspark.sql.types import StructType, StringType, TimestampType

# Strategy 1: Watermark with allowed lateness
df_stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "shipments") \
    .load() \
    .select(from_json(col("value"), shipment_schema).alias("data")) \
    .select("data.*")

# Add event time
df_with_time = df_stream \
    .withColumn("event_time", col("shipment_timestamp")) \
    .withColumn("processing_time", current_timestamp())

# Define watermark (allow 2 hours late)
watermarked = df_with_time \
    .withWatermark("event_time", "2 hours")

# Sink with late-arrival handling
def handle_late_arrivals(df, epoch_id):
    # Split into on-time vs late
    on_time = df.filter(col("event_time") >= col("processing_time") - expr("INTERVAL 2 HOURS"))
    late = df.filter(col("event_time") < col("processing_time") - expr("INTERVAL 2 HOURS"))
    
    # Append on-time to main table
    on_time.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save("/delta/shipments")
    
    # Handle late arrivals: update or log to separate table
    if late.count() > 0:
        late.write.format("delta") \
            .mode("append") \
            .save("/delta/shipments_late")
        
        # Merge late shipments with conflict resolution
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, "/delta/shipments")
        
        delta_table.alias("target") \
            .merge(
                late.alias("source"),
                "target.shipment_id = source.shipment_id"
            ) \
            .whenMatchedUpdate(set={
                "status": "source.status",  # Latest status wins
                "delivery_date": "source.delivery_date",
                "is_late_arrival": lit(True)
            }) \
            .whenNotMatchedInsertAll() \
            .execute()

# Apply windowed aggregations handling late data
from pyspark.sql.functions import window

aggregated = watermarked \
    .groupBy(
        window("event_time", "1 hour", "15 minutes"),
        "shipment_origin"
    ) \
    .agg(
        count("shipment_id").alias("shipment_count"),
        sum("quantity").alias("total_quantity")
    )
#  How broadcast joins create performance degradation

"""
Broadcast join failures and when to avoid them:

Case 1: Too large for broadcast but still triggers
"""
# Problem: Table is 5GB, broadcast threshold is 10GB
# Result: Each executor receives 5GB, OOM on small executor
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10GB")
df_bad = large_fact.join(broadcast(5gb_dim), "key")  # OOM

# Fix: Lower threshold to 200MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")

"""
Case 2: Broadcast join on skewed key distribution
"""
# Problem: One key appears 10M times
# Driver builds map for that key, executor iterates 10M times
# Worse than SortMergeJoin which distributes the work

df_skewed = large_fact.join(broadcast(small_but_skewed), "skewed_key")
# Better to use SortMerge with salting

"""
Case 3: Network bottleneck
"""
# 100 executors each receiving 500MB broadcast
# Network switch saturated, all tasks blocked
# Shuffle would distribute load across time

"""
Case 4: Serialization overhead
"""
# Complex nested structs in broadcast variable
# Each task deserializes independently → CPU spike
# Better to broadcast only necessary columns

"""
Case 5: Memory fragmentation in executor
"""
# 10GB executor memory, 4GB broadcast
# Remaining 6GB fragmented, unable to allocate for shuffle
# Leads to OOM even though total memory seems sufficient

# Diagnostic: When to avoid broadcast
def should_avoid_broadcast(df_dim):
    size_mb = df_dim.select(sum(length(to_json(struct("*"))))).collect()[0][0] / 1024 / 1024
    distinct_keys = df_dim.select("join_key").distinct().count()
    
    avoid_reasons = []
    if size_mb > 500:
        avoid_reasons.append("Size > 500MB")
    if distinct_keys < 100:
        avoid_reasons.append("Low cardinality join key")
    if size_mb / distinct_keys > 10:  # >10MB per key
        avoid_reasons.append("Large per-key data")
    
    return avoid_reasons
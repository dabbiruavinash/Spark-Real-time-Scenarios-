# Duplicate surrogate keys - root causes

"""
Root causes analysis for duplicate surrogate keys:

1. MONOTONICALLY_INCREASING_ID issues in partitions
   - Each partition starts from 0, leading to duplicates across partitions
   - Fix: Use row_number() with proper ordering
"""
# Problematic code
df = df.withColumn("surrogate_key", monotonically_increasing_id())

# Fixed approach
from pyspark.sql import Window
w = Window.orderBy("natural_key")
df = df.withColumn("surrogate_key", row_number().over(w))

"""
2. Checkpoint recovery without idempotency
   - Failed batch retries restart sequence generators
   - Duplicates occur when partial writes succeed
   - Fix: Store max key per partition in checkpoint
"""

"""
3. Parallel stream processing without coordination
   - Two streams write to same table with independent key generators
   - Fix: Use Delta's identity column (Databricks)
"""
# Delta identity column
spark.sql("""
    CREATE TABLE target (
        id BIGINT GENERATED ALWAYS AS IDENTITY,
        data STRING
    ) USING DELTA
""")

"""
4. Multiple downstream jobs reading same source
   - Each job generates keys independently
   - Overlapping key ranges
   - Fix: Single source of truth for key generation (Redis sequence)
"""

"""
5. Shuffle partition misconfiguration
   - Too few partitions cause key collisions
   - Fix: spark.sql.shuffle.partitions >= 2*executor_count
"""

# Comprehensive fix: Idempotent key generation
class IdempotentKeyGenerator:
    def __init__(self, key_table_path):
        self.key_table_path = key_table_path
        self.spark = SparkSession.builder.getOrCreate()
        
    def get_next_keys(self, n):
        """Atomically reserve n keys using Delta operations"""
        # Use Delta's atomic operations
        from delta.tables import DeltaTable
        
        key_table = DeltaTable.forPath(self.spark, self.key_table_path)
        
        # Atomic update with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                current_max = self.spark.sql(f"SELECT max(key) FROM delta.`{self.key_table_path}`").collect()[0][0] or 0
                next_keys = list(range(current_max + 1, current_max + n + 1))
                
                # Insert reserved keys atomically
                new_keys_df = self.spark.range(current_max + 1, current_max + n + 1) \
                    .toDF("key")
                
                key_table.alias("target") \
                    .merge(new_keys_df.alias("source"), "target.key = source.key") \
                    .whenNotMatchedInsertAll() \
                    .execute()
                
                return next_keys
            except Exception as e:
                if "ConcurrentAppendException" in str(e) and attempt < max_retries - 1:
                    continue
                raise
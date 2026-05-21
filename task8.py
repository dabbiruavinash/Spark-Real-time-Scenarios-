If data is adls and you want to sink in delta format how will you do it if you only 1 node and there 5k files 

# Optimized Delta Sink with Single Node
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp
import time

spark = SparkSession.builder \
    .appName("SingleNodeDeltaSink") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

def sink_5000_files_to_delta_single_node(
    source_path, 
    delta_path,
    batch_size=500  # Process 500 files at a time
):
    """
    Process 5000 files efficiently on a single node
    """
    # Get all file paths
    file_paths = spark.sparkContext.wholeTextFiles(source_path).keys().collect()
    total_files = len(file_paths)
    print(f"Total files to process: {total_files}")
    
    # Process in batches to avoid memory issues
    for i in range(0, total_files, batch_size):
        batch_files = file_paths[i:i+batch_size]
        
        print(f"Processing batch {i//batch_size + 1}, files: {len(batch_files)}")
        
        # Read batch of files
        df = spark.read.format("parquet").load(batch_files)
        
        # Add source file column for tracking
        df = df.withColumn("source_file", input_file_name())
        df = df.withColumn("processed_time", current_timestamp())
        
        # Write to Delta (merge if needed)
        if i == 0:
            df.write.format("delta").mode("overwrite").save(delta_path)
        else:
            df.write.format("delta").mode("append").save(delta_path)
        
        # Optimize after each batch
        spark.sql(f"OPTIMIZE delta.`{delta_path}`")
        
        # Clear cache to free memory
        spark.catalog.clearCache()

# Usage
sink_5000_files_to_delta_single_node(
    "abfss://container@storage.dfs.core.windows.net/source/",
    "abfss://container@storage.dfs.core.windows.net/delta/"
)

# Most Efficient: Direct ADLS to Delta with Partitioning
def efficient_delta_sink_5000_files(source_path, delta_path, num_partitions=10):
    """
    Most efficient approach for single node with 5000 files
    """
    # Step 1: Read all files with coalesce to control partitions
    df = spark.read.format("parquet").load(source_path)
    
    # Step 2: Repartition to optimal number for single node
    # Use coalesce to avoid shuffle (fast) or repartition for even distribution
    optimized_df = df.coalesce(num_partitions)
    
    # Step 3: Add useful metadata
    optimized_df = optimized_df \
        .withColumn("source_file", input_file_name()) \
        .withColumn("load_timestamp", current_timestamp())
    
    # Step 4: Write to Delta with performance optimizations
    optimized_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "true") \
        .save(delta_path)
    
    # Step 5: Post-write optimizations
    spark.sql(f"""
        OPTIMIZE delta.`{delta_path}`
        WHERE load_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    """)
    
    # Step 6: Z-order by frequently filtered columns
    # Get column names (excluding metadata columns)
    cols = [c for c in optimized_df.columns if c not in ['source_file', 'load_timestamp']]
    if cols:
        spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY ({','.join(cols[:3])})")
    
    print(f"Successfully processed {optimized_df.count()} rows from {len(df.inputFiles())} files")

# Configuration for ADLS access
spark.conf.set("fs.azure.account.auth.type.<storage>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage>.dfs.core.windows.net", "<client_id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage>.dfs.core.windows.net", "<client_secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage>.dfs.core.windows.net", "<endpoint>")

# Enable Delta optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.numPartitions", "10")

# Run the efficient sink
efficient_delta_sink_5000_files(
    "abfss://container@storage.dfs.core.windows.net/source/",
    "abfss://container@storage.dfs.core.windows.net/delta/",
    num_partitions=10  # Adjust based on your node's cores
)

# Chunked Processing with Checkpointing

def chunked_processing_with_checkpointing(source_path, delta_path, checkpoint_path):
    """
    Process files in chunks with checkpointing to resume from failures
    """
    from pyspark.sql import Row
    
    # Get list of all files
    all_files = spark.read.format("parquet").load(source_path).inputFiles()
    
    # Get already processed files from checkpoint
    try:
        processed_files_df = spark.read.parquet(checkpoint_path)
        processed_files = set([row.file_path for row in processed_files_df.collect()])
    except:
        processed_files = set()
    
    remaining_files = [f for f in all_files if f not in processed_files]
    
    print(f"Total files: {len(all_files)}, Already processed: {len(processed_files)}, Remaining: {len(remaining_files)}")
    
    # Process in chunks
    chunk_size = 200  # Files per chunk
    for chunk_idx in range(0, len(remaining_files), chunk_size):
        chunk_files = remaining_files[chunk_idx:chunk_idx+chunk_size]
        
        print(f"Processing chunk {chunk_idx//chunk_size + 1}, files: {len(chunk_files)}")
        
        try:
            # Read chunk
            df = spark.read.format("parquet").load(chunk_files)
            
            # Write to Delta
            df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(delta_path)
            
            # Update checkpoint
            checkpoint_data = [Row(file_path=f) for f in chunk_files]
            checkpoint_df = spark.createDataFrame(checkpoint_data)
            
            checkpoint_df.write \
                .mode("append") \
                .parquet(checkpoint_path)
            
            # Optimize every 5 chunks
            if (chunk_idx // chunk_size + 1) % 5 == 0:
                spark.sql(f"OPTIMIZE delta.`{delta_path}`")
            
            # Clear cache
            spark.catalog.clearCache()
            
        except Exception as e:
            print(f"Error processing chunk: {e}")
            # Save state and break - can resume from checkpoint
            break
    
    # Final optimization
    spark.sql(f"OPTIMIZE delta.`{delta_path}`")
    print("Processing completed")

# Usage
chunked_processing_with_checkpointing(
    "abfss://container@storage.dfs.core.windows.net/source/",
    "abfss://container@storage.dfs.core.windows.net/delta/",
    "abfss://container@storage.dfs.core.windows.net/checkpoint/"
)
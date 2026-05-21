When there's no timestamp column for incremental loading, you need alternative strategies. Here are practical approaches for both batch and streaming scenarios:

Strategy 1: File/Partition Based Incremental Load (Most Common)

# PySpark - Track processed files instead of timestamps

# Method 1A: Maintain processed file manifest
processed_files_path = "/mnt/processed_files_manifest/"

def get_new_files(source_path, processed_files_path, file_pattern="*.csv"):
    """Identify new files by comparing with previously processed files"""
    
    # Get all current files
    current_files = dbutils.fs.ls(source_path)
    current_file_names = set([f.name for f in current_files if f.name.endswith(file_pattern)])
    
    # Get already processed files
    if dbutils.fs.exists(processed_files_path):
        processed_df = spark.read.parquet(processed_files_path)
        processed_files = set([row.file_name for row in processed_df.collect()])
    else:
        processed_files = set()
    
    # Find new files
    new_files = current_file_names - processed_files
    
    return list(new_files)

# Process only new files
new_csv_files = get_new_files("/mnt/raw/csv/", "/mnt/processed_files_manifest/", "*.csv")

if new_csv_files:
    # Read only new files
    df_new = spark.read.option("header", "true").csv([f"/mnt/raw/csv/{f}" for f in new_csv_files])
    
    # Process the data
    df_transformed = transform_data(df_new)
    df_transformed.write.mode("append").format("delta").save("/mnt/processed/delta/")
    
    # Update manifest
    manifest_df = spark.createDataFrame([(f, current_timestamp()) for f in new_csv_files], 
                                        ["file_name", "processed_timestamp"])
    manifest_df.write.mode("append").parquet(processed_files_path)


Strategy 2: Use File Metadata (Modified Time)

# PySpark - Use file modification timestamp

from pyspark.sql.functions import input_file_name, col, to_timestamp

def read_new_files_by_modified_time(source_path, last_run_time):
    """Read files modified after last run using file metadata"""
    
    # Get files with their modification time
    files_with_metadata = []
    for file in dbutils.fs.ls(source_path):
        if file.modificationTime > last_run_time:
            files_with_metadata.append(file.path)
    
    if files_with_metadata:
        # Read selected files
        df = spark.read.option("header", "true").csv(files_with_metadata)
        
        # Add file metadata columns for audit
        df = df.withColumn("source_file", input_file_name())
        df = df.withColumn("file_modified_time", 
                          to_timestamp(col("source_file").substr(1, 19)))  # Extract from path
        
        return df
    else:
        return None

# Track last run time (store in Delta table or config)
last_run = spark.sql("SELECT MAX(processing_time) FROM control_table").collect()[0][0]

new_data = read_new_files_by_modified_time("/mnt/raw/csv/", last_run)
if new_data:
    new_data.write.mode("append").format("delta").save("/mnt/processed/delta/")
    # Update control table with current timestamp


Strategy 3: Sequence Number or Monotonically Increasing ID


# PySpark - Using monotically increasing ID for batch incremental

def incremental_load_by_id(source_table, last_processed_id, id_column="id", batch_size=10000):
    """Incremental load using ID column as sequence"""
    
    # Read records with ID greater than last processed
    df_new = spark.sql(f"""
        SELECT * FROM {source_table}
        WHERE {id_column} > {last_processed_id}
        ORDER BY {id_column}
        LIMIT {batch_size}
    """)
    
    if df_new.count() > 0:
        # Process the batch
        current_max_id = df_new.agg({id_column: "max"}).collect()[0][0]
        
        # Write to processed container
        df_new.write.mode("append").format("delta").save("/mnt/processed/delta/")
        
        return current_max_id
    return last_processed_id

# Track last processed ID in control table
last_id = spark.sql("SELECT MAX(last_processed_id) FROM control_table").collect()[0][0]
new_id = incremental_load_by_id("raw_orders", last_id, "order_id")

Strategy 4: Hash-Based Incremental Load (Deduplication)

# PySpark - Use hash of all columns to identify new/changed records

from pyspark.sql.functions import sha2, concat_ws, col

def incremental_load_by_hash(source_path, target_table):
    """Use hash of all columns to detect changes without timestamp"""
    
    # Read source data
    df_source = spark.read.option("header", "true").csv(source_path)
    
    # Create hash of all columns
    columns_to_hash = df_source.columns
    df_source = df_source.withColumn("row_hash", 
                                    sha2(concat_ws("||", *columns_to_hash), 256))
    
    # Get existing hashes from target
    df_target = spark.read.format("delta").load(target_table)
    existing_hashes = set([row.row_hash for row in df_target.select("row_hash").distinct().collect()])
    
    # Filter new/changed records
    df_new_records = df_source.filter(~col("row_hash").isin(existing_hashes))
    
    # Also need to handle updates (if any) - requires comparison logic
    
    return df_new_records

df_incremental = incremental_load_by_hash("/mnt/raw/csv/", "/mnt/processed/delta/")
df_incremental.write.mode("append").format("delta").save("/mnt/processed/delta/")


Strategy 5: Structured Streaming with File Monitoring

# PySpark Structured Streaming - Auto-detects new files

def streaming_without_timestamp():
    """Structured Streaming automatically processes new files as they arrive"""
    
    # Read stream - automatically picks up new files
    df_stream = spark.readStream \
        .option("header", "true") \
        .option("maxFilesPerTrigger", 1)  # Control batch size
        .schema(expected_schema) \
        .csv("/mnt/raw/csv/")
    
    # Add processing metadata
    df_stream = df_stream.withColumn("processing_time", current_timestamp())
    df_stream = df_stream.withColumn("source_file", input_file_name())
    
    # Write stream to Delta
    query = df_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/mnt/checkpoints/") \
        .trigger(processingTime="1 minute") \
        .start("/mnt/processed/delta/")
    
    return query


Strategy 6: Control Table Approach (Recommended for Production)

# Complete production-ready solution using control table

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

class IncrementalLoader:
    def __init__(self, control_table_path="/mnt/control/incremental_control"):
        self.control_table_path = control_table_path
        self.init_control_table()
    
    def init_control_table(self):
        """Initialize control table if not exists"""
        if not DeltaTable.isDeltaTable(spark, self.control_table_path):
            spark.createDataFrame([], 
                "source_system string, last_processed_file string, last_modified_time timestamp, records_processed long")\
                .write.format("delta").save(self.control_table_path)
    
    def get_last_checkpoint(self, source_system):
        """Get last processed checkpoint info"""
        try:
            checkpoint = spark.sql(f"""
                SELECT last_processed_file, last_modified_time 
                FROM delta.`{self.control_table_path}`
                WHERE source_system = '{source_system}'
                ORDER BY last_modified_time DESC
                LIMIT 1
            """).collect()[0]
            return checkpoint
        except:
            return None
    
    def incremental_load_no_timestamp(self, source_path, source_system, file_pattern="*.csv"):
        """Main method for incremental load without timestamp"""
        
        # Strategy combination: File modification time + filename tracking
        checkpoint = self.get_last_checkpoint(source_system)
        last_processed_file = checkpoint.last_processed_file if checkpoint else None
        
        # Get all files with metadata
        files_metadata = []
        for file in dbutils.fs.ls(source_path):
            if file.name.endswith(file_pattern.replace("*", "")):
                files_metadata.append({
                    "file_path": file.path,
                    "file_name": file.name,
                    "modified_time": file.modificationTime
                })
        
        # Determine which files to process
        files_to_process = []
        if last_processed_file:
            # Find files after last processed file (based on name or time)
            files_to_process = [f for f in files_metadata 
                              if f["file_name"] > last_processed_file]
        else:
            files_to_process = files_metadata
        
        if not files_to_process:
            print(f"No new files found for {source_system}")
            return None
        
        # Read and process new files
        all_new_data = []
        for file_info in files_to_process:
            df_file = spark.read.option("header", "true").csv(file_info["file_path"])
            df_file = df_file.withColumn("source_file", lit(file_info["file_name"]))
            df_file = df_file.withColumn("ingestion_time", current_timestamp())
            all_new_data.append(df_file)
        
        if all_new_data:
            df_combined = all_new_data[0]
            for df in all_new_data[1:]:
                df_combined = df_combined.union(df)
            
            # Apply transformations and write
            df_transformed = self.transform_data(df_combined)
            df_transformed.write.mode("append").format("delta").save("/mnt/processed/delta/")
            
            # Update control table
            last_file = max(files_to_process, key=lambda x: x["file_name"])
            spark.createDataFrame([(source_system, last_file["file_name"], 
                                   current_timestamp(), df_combined.count())],
                                  "source_system string, last_processed_file string, last_modified_time timestamp, records_processed long")\
                .write.mode("append").format("delta").save(self.control_table_path)
            
            print(f"Processed {len(files_to_process)} files, {df_combined.count()} records")
            return df_transformed
        
        return None
    
    def transform_data(self, df):
        """Apply business transformations"""
        # Your transformation logic here
        return df

# Usage
loader = IncrementalLoader()
df_new = loader.incremental_load_no_timestamp("/mnt/raw/csv/", "sales_system")


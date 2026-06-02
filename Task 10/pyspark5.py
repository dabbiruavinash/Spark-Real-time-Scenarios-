# Handling corrupted Avro files without failing pipeline

# Strategy 1: Try-catch with bad records isolation
from pyspark.sql.functions import udf, struct
from avro.datafile import DataFileReader
from avro.io import DatumReader
import io

def safe_read_avro(content_bytes):
    """Read Avro and return tuple (is_valid, parsed_data, error_message)"""
    try:
        reader = DataFileReader(io.BytesIO(content_bytes), DatumReader())
        records = [record for record in reader]
        reader.close()
        return (True, records, None)
    except Exception as e:
        return (False, None, str(e))

# Register UDF
safe_avro_udf = udf(safe_read_avro, 
                     StructType([...]))  # Define return schema

# Process with fault tolerance
df_raw = spark.read.format("binaryFile") \
    .option("pathGlobFilter", "*.avro") \
    .load("/vendors/")

# Attempt to parse with error handling
df_parsed = df_raw \
    .withColumn("parse_result", safe_avro_udf(col("content"))) \
    .withColumn("is_valid", col("parse_result._1")) \
    .withColumn("parsed_data", col("parse_result._2")) \
    .withColumn("error_msg", col("parse_result._3"))

# Split into good and bad
df_good = df_parsed.filter(col("is_valid") == True) \
    .select("path", explode("parsed_data").alias("record")) \
    .select("record.*")

df_bad = df_parsed.filter(col("is_valid") == False) \
    .select("path", "error_msg")

# Write bad records for investigation
df_bad.write.mode("append").parquet("/corruption_quarantine/")

# Continue pipeline with good data
df_good.write.format("delta").mode("append").save("/delta/clean_avro")

# Strategy 2: Use Delta's schema enforcement with permissive mode
spark.read.format("avro") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("/path/") \
    .write.format("delta") \
    .option("mergeSchema", "true") \
    .save("/delta/data")

# Strategy 3: Implement dead letter queue
def write_with_dlq(df, epoch_id):
    try:
        df.write.format("delta").save("/delta/good")
    except Exception as e:
        # Send to DLQ
        df.write.format("delta").save("/delta/dead-letter-queue")
        # Alert monitoring
        send_alert(f"Batch {epoch_id} failed: {str(e)}")
# CDC ingestion from SAP into Delta Lake

# SAP CDC architecture using ODP framework

# Step 1: Extract from SAP via ODP extractor
class SAPCDCReader:
    def __init__(self, sap_connection_params):
        self.conn = self._create_sap_connection(sap_connection_params)
        
    def read_cdc(self, odata_service, subscription_id):
        """Read incremental changes from SAP OData"""
        # Use SAP ODP delta queue
        query = f"""
        /sap/opu/odata/sap/{odata_service}/DeltaSet?
        $filter=ChangedAt gt datetime'{self.last_offset}'
        &$orderby=ChangedAt
        """
        
        # Handle SAP-specific change types
        changes = self.conn.get(query)
        return self._parse_sap_cdc(changes)
    
    def _parse_sap_cdc(self, changes):
        """Convert SAP CDC format to standard CDC"""
        return [{
            "operation": change["ChangeType"],  # I/U/D
            "record": change["Record"],
            "timestamp": change["ChangedAt"],
            "sequence_id": change["SequenceNumber"]
        } for change in changes]

# Step 2: Apply to Delta with SCD Type 2
from delta.tables import DeltaTable

def apply_sap_cdc(df_cdc, target_path):
    """Apply SAP CDC changes to Delta table"""
    target_table = DeltaTable.forPath(spark, target_path)
    
    # Prepare source dataframe with CDC metadata
    updates = df_cdc \
        .withColumnRenamed("record", "new_data") \
        .withColumn("is_deleted", when(col("operation") == "D", True).otherwise(False)) \
        .withColumn("effective_date", col("timestamp")) \
        .withColumn("row_hash", sha2(to_json(struct("new_data")), 256))
    
    # Merge with SCD logic
    target_table.alias("target") \
        .merge(
            updates.alias("source"),
            "target.business_key = source.business_key AND target.is_current = true"
        ) \
        .whenMatchedUpdate(
            condition="source.row_hash != target.row_hash OR source.is_deleted = true",
            set={
                "is_current": lit(False),
                "end_date": "source.effective_date"
            }
        ) \
        .whenNotMatchedInsert(
            condition="source.is_deleted = false",
            values={
                "business_key": "source.business_key",
                "current_data": "source.new_data",
                "is_current": lit(True),
                "start_date": "source.effective_date",
                "sap_sequence": "source.sequence_id"
            }
        ) \
        .execute()

# Step 3: Streaming CDC pipeline
streaming_cdc = spark.readStream \
    .format("sap-odp") \
    .option("subscription", "CRUDE_OIL_SHIPMENTS") \
    .option("checkpointLocation", "/checkpoints/sap_cdc") \
    .load()

streaming_cdc.writeStream \
    .foreachBatch(lambda df, id: apply_sap_cdc(df, "/delta/sap_crude_oil")) \
    .trigger(processingTime="1 minute") \
    .start()
# Reusable metadata-driven ETL framework in Databricks

# Framework architecture
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Any
from enum import Enum
import json

class ETLFramework:
    def __init__(self, metadata_table: str):
        self.metadata_table = metadata_table
        self.spark = SparkSession.builder.getOrCreate()
        
    def load_pipeline_config(self, pipeline_id: str) -> Dict:
        """Load configuration from metadata tables"""
        config_df = self.spark.sql(f"""
            SELECT 
                p.pipeline_name,
                p.source_type,
                p.source_path,
                p.target_path,
                t.table_name,
                t.partition_columns,
                t.zorder_columns,
                c.column_name,
                c.source_column,
                c.transform_expr,
                c.target_column,
                c.data_type,
                v.validation_rule,
                v.severity
            FROM pipeline_metadata.pipelines p
            JOIN pipeline_metadata.tables t ON p.pipeline_id = t.pipeline_id
            LEFT JOIN pipeline_metadata.columns c ON t.table_id = c.table_id
            LEFT JOIN pipeline_metadata.validations v ON c.column_id = v.column_id
            WHERE p.pipeline_id = '{pipeline_id}'
            AND p.active = true
        """)
        
        return self._config_to_dict(config_df)
    
    def dynamic_reader(self, config: Dict) -> DataFrame:
        """Dynamically read based on source type"""
        source_type = config['source_type']
        source_path = config['source_path']
        
        readers = {
            'delta': self.spark.read.format('delta'),
            'parquet': self.spark.read.parquet,
            'json': lambda p: self.spark.read.option('multiline', 'true').json(p),
            'kafka': lambda p: self.spark.readStream.format('kafka')
                              .option('subscribe', p)
                              .load(),
            'sap': self._read_from_sap
        }
        
        return readers.get(source_type, self.spark.read)(source_path)
    
    def apply_transformations(self, df: DataFrame, config: Dict) -> DataFrame:
        """Apply column transformations from metadata"""
        from pyspark.sql.functions import expr
        
        transformations = []
        for col_config in config['columns']:
            if col_config['transform_expr']:
                transformations.append(
                    expr(col_config['transform_expr']).alias(col_config['target_column'])
                )
            else:
                transformations.append(
                    df[col_config['source_column']].alias(col_config['target_column'])
                )
        
        return df.select(*transformations)
    
    def validate_data(self, df: DataFrame, config: Dict) -> DataFrame:
        """Apply data quality validations"""
        from pyspark.sql.functions import when, col, lit
        
        validation_df = df
        for validation in config['validations']:
            rule = validation['validation_rule']
            severity = validation['severity']
            
            if severity == 'error':
                # Filter out invalid records
                validation_df = validation_df.filter(expr(rule))
                # Log bad records
                bad_records = df.filter(~expr(rule))
                bad_records.write.mode('append').parquet('/datalake/errors/')
            else:  # warning
                # Count and log but don't filter
                invalid_count = df.filter(~expr(rule)).count()
                if invalid_count > 0:
                    self._log_warning(f"Validation {rule} failed {invalid_count} times")
        
        return validation_df
    
    def write_with_optimization(self, df: DataFrame, config: Dict):
        """Optimized write with partitioning and Z-order"""
        writer = df.write.format('delta')
        
        if config.get('partition_columns'):
            writer = writer.partitionBy(*config['partition_columns'])
        
        writer.mode('append') \
              .option('mergeSchema', 'true') \
              .save(config['target_path'])
        
        # Post-write optimizations
        if config.get('zorder_columns'):
            zorder_cols = ','.join(config['zorder_columns'])
            self.spark.sql(f"""
                OPTIMIZE delta.`{config['target_path']}` 
                ZORDER BY ({zorder_cols})
            """)
    
    def execute_pipeline(self, pipeline_id: str):
        """Main orchestration method"""
        config = self.load_pipeline_config(pipeline_id)
        
        # Read
        df = self.dynamic_reader(config)
        
        # Transform
        df = self.apply_transformations(df, config)
        
        # Validate
        df = self.validate_data(df, config)
        
        # Write
        self.write_with_optimization(df, config)
        
        # Log metrics
        self._log_metrics(pipeline_id, df.count())

# Usage
framework = ETLFramework("etl_metadata_db")
framework.execute_pipeline("crude_oil_processing")
"""
Apache Spark Streaming Processing Module
Handles real-time streaming data processing using PySpark Structured Streaming
"""

from typing import Dict, List, Optional, Any
import logging


class SparkStreamingProcessor:
    """
    Streaming Processing using Apache Spark Structured Streaming
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self.spark = None
        self.streaming_df = None
    
    def initialize_spark_streaming(self, app_name: str = "AgricultureYieldStreaming") -> Any:
        """Initialize Spark session for streaming"""
        try:
            from pyspark.sql import SparkSession
            
            builder = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.streaming.checkpointLocation", 
                       self.config.get("checkpoint_dir", "/tmp/checkpoint"))
            
            if self.config.get("master"):
                builder = builder.master(self.config["master"])
            
            self.spark = builder.getOrCreate()
            self.logger.info(f"Spark Streaming session initialized: {app_name}")
            
            return self.spark
            
        except ImportError:
            self.logger.warning("PySpark not installed")
            return None
    
    def create_stream_from_kafka(self, 
                                 bootstrap_servers: str, 
                                 topic: str,
                                 starting_offset: str = "earliest") -> Any:
        """Create streaming DataFrame from Kafka"""
        if not self.spark:
            self.initialize_spark_streaming()
        
        self.streaming_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offset) \
            .load()
        
        self.logger.info(f"Streaming from Kafka topic: {topic}")
        return self.streaming_df
    
    def create_stream_from_socket(self, host: str = "localhost", port: int = 9999) -> Any:
        """Create streaming DataFrame from socket"""
        if not self.spark:
            self.initialize_spark_streaming()
        
        self.streaming_df = self.spark \
            .readStream \
            .format("socket") \
            .option("host", host) \
            .option("port", port) \
            .load()
        
        self.logger.info(f"Streaming from socket: {host}:{port}")
        return self.streaming_df
    
    def create_stream_from_files(self, 
                                 path: str, 
                                 format: str = "csv",
                                 schema: Optional[Any] = None) -> Any:
        """Create streaming DataFrame from file source"""
        if not self.spark:
            self.initialize_spark_streaming()
        
        stream = self.spark.readStream.format("filesystem") \
            .option("path", path) \
            .schema(schema) if schema else None
        
        if format == "csv":
            self.streaming_df = stream.option("header", "true").csv(path)
        elif format == "json":
            self.streaming_df = stream.json(path)
        elif format == "parquet":
            self.streaming_df = stream.parquet(path)
        
        self.logger.info(f"Streaming from files: {path}")
        return self.streaming_df
    
    def process_stream(self, 
                       transform_func: callable,
                       output_mode: str = "append",
                       trigger_interval: Optional[str] = None) -> Any:
        """Process streaming data with transformation"""
        if not self.streaming_df:
            self.logger.error("No streaming data source defined")
            return None
        
        # Apply transformation
        processed_df = transform_func(self.streaming_df)
        
        # Configure streaming query
        query = processed_df.writeStream \
            .format("console") \
            .outputMode(output_mode)
        
        if trigger_interval:
            query = query.trigger(processingTime=trigger_interval)
        
        return query
    
    def write_to_kafka(self, 
                       topic: str, 
                       bootstrap_servers: str,
                       output_mode: str = "complete") -> Any:
        """Write streaming data to Kafka"""
        if not self.streaming_df:
            self.logger.error("No streaming data to write")
            return None
        
        return self.streaming_df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic) \
            .outputMode(output_mode) \
            .start()
    
    def write_to_files(self, 
                       path: str, 
                       format: str = "parquet",
                       output_mode: str = "append",
                       trigger_interval: Optional[str] = None) -> Any:
        """Write streaming data to files"""
        if not self.streaming_df:
            self.logger.error("No streaming data to write")
            return None
        
        writer = self.streaming_df \
            .writeStream \
            .format("filesystem") \
            .option("path", path) \
            .outputMode(output_mode)
        
        if format == "parquet":
            writer = writer.option("checkpointLocation", f"{path}/_checkpoint")
        
        if trigger_interval:
            writer = writer.trigger(processingTime=trigger_interval)
        
        return writer.start()
    
    def stop_streaming(self):
        """Stop all active streaming queries"""
        if self.spark:
            for query in self.spark.streams.active:
                query.stop()
            self.logger.info("All streaming queries stopped")


class StreamTransformer:
    """
    Stream transformation utilities
    """
    
    @staticmethod
    def parse_json_stream(df: Any) -> Any:
        """Parse JSON data from stream"""
        from pyspark.sql import functions as F
        
        return df.select(F.from_json(df.value.cast("string"), "schema").alias("data")) \
            .select("data.*")
    
    @staticmethod
    def aggregate_window(df: Any, 
                        group_by: List[str], 
                        window_col: str,
                        aggregations: Dict[str, str]) -> Any:
        """Apply window aggregation"""
        from pyspark.sql import functions as F
        
        agg_exprs = []
        for col, agg_func in aggregations.items():
            if agg_func == "count":
                agg_exprs.append(F.count(col).alias(f"{col}_count"))
            elif agg_func == "avg":
                agg_exprs.append(F.avg(col).alias(f"{col}_avg"))
            elif agg_func == "sum":
                agg_exprs.append(F.sum(col).alias(f"{col}_sum"))
        
        return df.groupBy(
            *group_by,
            F.window(df[window_col], "1 hour")
        ).agg(*agg_exprs)
    
    @staticmethod
    def filter_stream(df: Any, condition: str) -> Any:
        """Filter streaming data"""
        return df.filter(condition)
    
    @staticmethod
    def join_streams(df1: Any, df2: Any, join_condition: str, join_type: str = "inner") -> Any:
        """Join two streaming DataFrames"""
        return df1.join(df2, join_condition, join_type)

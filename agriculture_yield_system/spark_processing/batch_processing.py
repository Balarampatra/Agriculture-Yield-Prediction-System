"""
Apache Spark Batch Processing Module
Handles batch processing of agricultural data using PySpark
"""

from typing import Dict, List, Optional, Any
import logging


class SparkBatchProcessor:
    """
    Batch Processing using Apache Spark for large-scale data processing
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self.spark = None
        self.df = None
    
    def initialize_spark(self, app_name: str = "AgricultureYieldProcessing") -> Any:
        """
        Initialize Spark session
        """
        try:
            from pyspark.sql import SparkSession
            
            builder = SparkSession.builder.appName(app_name)
            
            # Add configurations
            if self.config.get("master"):
                builder = builder.master(self.config["master"])
            
            if self.config.get("memory"):
                builder = builder.config("spark.driver.memory", self.config["memory"])
            
            self.spark = builder.getOrCreate()
            self.logger.info(f"Spark session initialized: {app_name}")
            
            return self.spark
        except ImportError:
            self.logger.warning("PySpark not installed. Install with: pip install pyspark")
            return None
    
    def load_data(self, 
                  source: str, 
                  path: str, 
                  format: str = "csv",
                  **options) -> Any:
        """
        Load data into Spark DataFrame
        """
        if not self.spark:
            self.initialize_spark()
        
        self.logger.info(f"Loading data from {source}: {path}")
        
        try:
            if source == "file":
                if format == "csv":
                    self.df = self.spark.read.csv(path, header=True, inferSchema=True, **options)
                elif format == "parquet":
                    self.df = self.spark.read.parquet(path, **options)
                elif format == "json":
                    self.df = self.spark.read.json(path, **options)
                elif format == "excel":
                    self.logger.warning("Excel format not directly supported in Spark")
                    return None
            elif source == "database":
                self.df = self.spark.read.format("jdbc").options(**options).load()
            
            self.logger.info(f"Loaded {self.df.count()} rows")
            return self.df
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            return None
    
    def transform_data(self, transformations: List[Dict]) -> Any:
        """
        Apply transformations to the data
        """
        if not self.df:
            self.logger.error("No data loaded. Load data first.")
            return None
        
        result_df = self.df
        
        for transform in transformations:
            transform_type = transform.get("type")
            
            if transform_type == "select":
                cols = transform.get("columns")
                result_df = result_df.select(cols)
                
            elif transform_type == "filter":
                condition = transform.get("condition")
                result_df = result_df.filter(condition)
                
            elif transform_type == "with_column":
                col_name = transform.get("name")
                col_expr = transform.get("expression")
                from pyspark.sql import functions as F
                result_df = result_df.withColumn(col_name, F.expr(col_expr))
                
            elif transform_type == "group_by":
                group_cols = transform.get("columns")
                agg_funcs = transform.get("aggregations")
                result_df = result_df.groupBy(group_cols).agg(*agg_funcs)
                
            elif transform_type == "join":
                other_df = transform.get("dataframe")
                join_cond = transform.get("condition")
                join_type = transform.get("type", "inner")
                result_df = result_df.join(other_df, join_cond, join_type)
        
        self.df = result_df
        return result_df
    
    def aggregate_data(self, 
                       group_by: List[str], 
                       aggregations: Dict[str, str]) -> Any:
        """
        Perform aggregations on the data
        """
        if not self.df:
            return None
        
        from pyspark.sql import functions as F
        
        agg_exprs = []
        for col, agg_func in aggregations.items():
            if agg_func == "sum":
                agg_exprs.append(F.sum(col).alias(f"{col}_sum"))
            elif agg_func == "avg":
                agg_exprs.append(F.avg(col).alias(f"{col}_avg"))
            elif agg_func == "count":
                agg_exprs.append(F.count(col).alias(f"{col}_count"))
            elif agg_func == "min":
                agg_exprs.append(F.min(col).alias(f"{col}_min"))
            elif agg_func == "max":
                agg_exprs.append(F.max(col).alias(f"{col}_max"))
        
        return self.df.groupBy(*group_by).agg(*agg_exprs)
    
    def write_data(self, 
                   destination: str, 
                   path: str, 
                   format: str = "csv",
                   mode: str = "overwrite",
                   **options) -> bool:
        """
        Write data to destination
        """
        if not self.df:
            self.logger.error("No data to write")
            return False
        
        self.logger.info(f"Writing data to {destination}: {path}")
        
        try:
            if destination == "file":
                if format == "csv":
                    self.df.write.csv(path, header=True, mode=mode, **options)
                elif format == "parquet":
                    self.df.write.parquet(path, mode=mode, **options)
                elif format == "json":
                    self.df.write.json(path, mode=mode, **options)
            elif destination == "database":
                self.df.write.format("jdbc").options(**options).save()
            
            self.logger.info("Data written successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing data: {str(e)}")
            return False
    
    def run_batch_job(self, 
                      source_config: Dict,
                      transform_config: List[Dict],
                      destination_config: Dict) -> bool:
        """
        Run complete batch processing job
        """
        # Load
        df = self.load_data(
            source_config["source"],
            source_config["path"],
            source_config.get("format", "csv")
        )
        
        if df is None:
            return False
        
        # Transform
        df = self.transform_data(transform_config)
        
        # Write
        success = self.write_data(
            destination_config["destination"],
            destination_config["path"],
            destination_config.get("format", "csv"),
            destination_config.get("mode", "overwrite")
        )
        
        return success
    
    def stop_spark(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")


class DataProcessor:
    """
    Data Processing utilities for Spark
    """
    
    @staticmethod
    def clean_nulls(df: Any) -> Any:
        """Clean null values"""
        from pyspark.sql import functions as F
        
        # Fill numeric with median
        numeric_cols = [field.name for field in df.schema.fields 
                       if str(field.dataType) in ['IntegerType', 'DoubleType', 'FloatType']]
        
        for col in numeric_cols:
            median_val = df.approxQuantile(col, [0.5], 0.01)[0]
            df = df.fillna({col: median_val})
        
        # Fill string with empty
        string_cols = [field.name for field in df.schema.fields 
                      if str(field.dataType) == 'StringType']
        df = df.fillna({col: "Unknown" for col in string_cols})
        
        return df
    
    @staticmethod
    def add_features(df: Any) -> Any:
        """Add derived features"""
        from pyspark.sql import functions as F
        
        # Yield per hectare
        if "production" in df.columns and "area" in df.columns:
            df = df.withColumn("yield_per_hectare", 
                             F.col("production") / F.col("area"))
        
        # Temperature category
        if "temperature" in df.columns:
            df = df.withColumn("temp_category",
                             F.when(F.col("temperature") < 15, "Cold")
                              .when(F.col("temperature") < 25, "Moderate")
                              .when(F.col("temperature") < 35, "Warm")
                              .otherwise("Hot"))
        
        return df
    
    @staticmethod
    def cache_data(df: Any, level: str = "MEMORY_AND_DISK") -> Any:
        """Cache DataFrame"""
        if level == "MEMORY_ONLY":
            return df.cache()
        elif level == "DISK_ONLY":
            return df.persist(storagelevel=level)
        else:
            return df.persist(storagelevel=level)

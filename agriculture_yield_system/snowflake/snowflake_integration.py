"""
Snowflake Integration Module
Contains functionality for Snowflake: Clustering, Time Travel, Semi-Structured Data
"""

from typing import Dict, List, Optional, Any
import logging


class SnowflakeConnector:
    """
    Snowflake database connector and utilities
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.cursor = None
    
    def connect(self, 
               account: str, 
               user: str, 
               password: str, 
               warehouse: str, 
               database: str,
               schema: str = "PUBLIC") -> bool:
        """
        Connect to Snowflake
        """
        try:
            import snowflake.connector
            
            self.connection = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                warehouse=warehouse,
                database=database,
                schema=schema
            )
            self.cursor = self.connection.cursor()
            self.logger.info("Connected to Snowflake successfully")
            return True
            
        except ImportError:
            self.logger.warning("snowflake-connector-python not installed")
            return False
        except Exception as e:
            self.logger.error(f"Error connecting to Snowflake: {str(e)}")
            return False
    
    def execute_query(self, query: str) -> Optional[List]:
        """Execute a query and return results"""
        if not self.cursor:
            self.logger.error("No connection established")
            return None
        
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            return None
    
    def close(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("Snowflake connection closed")


class ClusteringManager:
    """
    Snowflake Clustering management
    """
    
    @staticmethod
    def create_clustered_table(cluster_by: List[str]) -> str:
        """
        Generate SQL to create a clustered table
        """
        cluster_cols = ", ".join(cluster_by)
        return f"""
        CREATE TABLE fact_crop_yield_clustered (
            yield_id INTEGER,
            crop_key INTEGER,
            region_key INTEGER,
            production_tons DECIMAL(10,2),
            area_hectares DECIMAL(10,2),
            yield_per_hectare DECIMAL(10,2),
            created_at TIMESTAMP
        )
        CLUSTER BY ({cluster_cols});
        """
    
    @staticmethod
    def create_clustering_key(key: str) -> str:
        """Generate ALTER TABLE to add clustering key"""
        return f"ALTER TABLE fact_crop_yield CLUSTER BY ({key});"
    
    @staticmethod
    def reorganize_clusters() -> str:
        """Generate SQL to reorganize clusters"""
        return "ALTER TABLE fact_crop_yield REORGANIZE;"


class TimeTravelManager:
    """
    Snowflake Time Travel functionality
    """
    
    @staticmethod
    def query_at_timestamp(timestamp: str) -> str:
        """Query data at specific timestamp"""
        return f"SELECT * FROM fact_crop_yield AT (TIMESTAMP => '{timestamp}');"
    
    @staticmethod
    def query_at_offset(offset_minutes: int) -> str:
        """Query data at offset (minutes ago)"""
        return f"SELECT * FROM fact_crop_yield AT (OFFSET => {offset_minutes * 60});"
    
    @staticmethod
    def query_before_change(change_date: str) -> str:
        """Query data before a specific change"""
        return f"SELECT * FROM fact_crop_yield BEFORE (CHANGE => '{change_date}');"
    
    @staticmethod
    def undrop_table(table_name: str) -> str:
        """Undrop a table"""
        return f"UNDROP TABLE {table_name};"
    
    @staticmethod
    def create_clone(clone_name: str, source_table: str) -> str:
        """Create a clone of table at current time"""
        return f"CREATE OR REPLACE {clone_name} CLONE {source_table};"
    
    @staticmethod
    def get_table_history(table_name: str) -> str:
        """Get table history"""
        return f"SELECT * FROM TABLE({table_name}_HISTORY);"


class SemiStructuredDataHandler:
    """
    Handle semi-structured data in Snowflake (JSON, VARIANT, ARRAY)
    """
    
    @staticmethod
    def create_table_with_variant() -> str:
        """Create table with VARIANT column for semi-structured data"""
        return """
        CREATE TABLE sensor_data (
            sensor_id VARCHAR(50),
            reading_time TIMESTAMP,
            readings VARIANT,
            metadata VARIANT
        );
        """
    
    @staticmethod
    def insert_json_data(json_string: str) -> str:
        """Insert JSON data into VARIANT column"""
        return f"INSERT INTO sensor_data VALUES ('SENSOR_001', CURRENT_TIMESTAMP(), PARSE_JSON('{json_string}'));"
    
    @staticmethod
    def query_json_path(path: str) -> str:
        """Query specific path from JSON"""
        return f"SELECT readings:{path} FROM sensor_data;"
    
    @staticmethod
    def flatten_array() -> str:
        """Flatten array data"""
        return """
        SELECT sensor_id, f.value as array_element
        FROM sensor_data,
        LATERAL FLATTEN(input => readings:tags) f;
        """
    
    @staticmethod
    def parse_json_column() -> str:
        """Parse JSON column to structured format"""
        return """
        SELECT 
            sensor_id,
            readings:temperature::NUMBER as temperature,
            readings:humidity::NUMBER as humidity,
            readings:pressure::NUMBER as pressure
        FROM sensor_data;
        """
    
    @staticmethod
    def create_view_on_json() -> str:
        """Create view on JSON data"""
        return """
        CREATE OR REPLACE VIEW sensor_readings_view AS
        SELECT 
            sensor_id,
            reading_time,
            readings: temperature::NUMBER as temperature,
            readings:humidity::NUMBER as humidity,
            METADATA$FILE_NAME as source_file
        FROM sensor_data;
        """


class SnowflakePerformance:
    """
    Snowflake performance optimization utilities
    """
    
    @staticmethod
    def create_materialized_view() -> str:
        """Create materialized view for aggregation"""
        return """
        CREATE MATERIALIZED VIEW yield_by_region_mv AS
        SELECT 
            r.state_name,
            c.crop_name,
            t.year,
            AVG(f.yield_per_hectare) AS avg_yield,
            SUM(f.production_tons) AS total_production
        FROM fact_crop_yield f
        JOIN dim_region r ON f.region_key = r.region_key
        JOIN dim_crop c ON f.crop_key = c.crop_key
        JOIN dim_time t ON f.time_key = t.time_key
        GROUP BY r.state_name, c.crop_name, t.year;
        """
    
    @staticmethod
    def create_search_optimization() -> str:
        """Enable search optimization"""
        return "ALTER TABLE fact_crop_yield SET SEARCH_OPTIMIZATION = ON;"
    
    @staticmethod
    def analyze_table() -> str:
        """Analyze table for query optimization"""
        return "ANALYZE TABLE fact_crop_yield COMPUTE STATISTICS;"

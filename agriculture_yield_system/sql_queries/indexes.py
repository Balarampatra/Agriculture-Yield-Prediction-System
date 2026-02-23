"""
Database Indexes Module
Contains SQL index definitions for optimizing query performance
"""

from typing import List, Dict


class DatabaseIndexes:
    """
    Database indexes for Agriculture Yield Prediction Data Warehouse
    """
    
    def __init__(self):
        self.table_name = "fact_crop_yield"
    
    def get_composite_indexes(self) -> List[str]:
        """Generate composite indexes for fact table"""
        return [
            """CREATE INDEX idx_fact_region_time 
               ON fact_crop_yield (region_key, time_key)
               INCLUDE (production_tons, area_hectares);""",
               
            """CREATE INDEX idx_fact_crop_time 
               ON fact_crop_yield (crop_key, time_key)
               INCLUDE (production_tons);""",
               
            """CREATE INDEX idx_fact_weather_soil 
               ON fact_crop_yield (weather_key, soil_key)
               INCLUDE (yield_per_hectare);"""
        ]
    
    def get_partitioned_indexes(self) -> List[str]:
        """Generate partitioned indexes"""
        return [
            """CREATE LOCAL INDEX idx_partition_crop 
               ON fact_crop_yield (crop_key) 
               PARTITION BY (time_key);""",
               
            """CREATE LOCAL INDEX idx_partition_production 
               ON fact_crop_yield (production_tons DESC) 
               PARTITION BY (time_key);"""
        ]
    
    def get_covering_indexes(self) -> List[str]:
        """Generate covering indexes for common queries"""
        return [
            """CREATE INDEX idx_covering_yield_report 
               ON dim_region (state_name, district)
               INCLUDE (latitude, longitude);"""
        ]
    
    def get_all_indexes(self) -> Dict[str, List[str]]:
        """Return all index definitions"""
        return {
            "composite": self.get_composite_indexes(),
            "partitioned": self.get_partitioned_indexes(),
            "covering": self.get_covering_indexes()
        }

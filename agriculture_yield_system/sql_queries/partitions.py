"""
Table Partitioning Module
Contains SQL partitioning strategies for large-scale data management
"""

from typing import List, Dict


class TablePartitions:
    """
    Table partitioning strategies for Agriculture Yield Data Warehouse
    """
    
    def __init__(self):
        self.table_name = "fact_crop_yield"
    
    def get_range_partitioning(self) -> str:
        """Generate range partitioning by time"""
        return """
        CREATE TABLE fact_crop_yield (
            yield_id INTEGER,
            crop_key INTEGER,
            region_key INTEGER,
            weather_key INTEGER,
            time_key INTEGER,
            soil_key INTEGER,
            production_tons DECIMAL(10,2),
            area_hectares DECIMAL(10,2),
            yield_per_hectare DECIMAL(10,2),
            created_at TIMESTAMP
        )
        PARTITION BY RANGE (time_key);
        """
    
    def get_partition_definitions(self) -> List[str]:
        """Generate partition definitions by year"""
        return [
            """CREATE TABLE fact_crop_yield_2020 
               PARTITION OF fact_crop_yield 
               FOR VALUES FROM (20200101) TO (20210101);""",
            
            """CREATE TABLE fact_crop_yield_2021 
               PARTITION OF fact_crop_yield 
               FOR VALUES FROM (20210101) TO (20220101);""",
               
            """CREATE TABLE fact_crop_yield_2022 
               PARTITION OF fact_crop_yield 
               FOR VALUES FROM (20220101) TO (20230101);""",
               
            """CREATE TABLE fact_crop_yield_2023 
               PARTITION OF fact_crop_yield 
               FOR VALUES FROM (20230101) TO (20240101);"""
        ]
    
    def get_list_partitioning(self) -> str:
        """Generate list partitioning by region"""
        return """
        CREATE TABLE fact_crop_yield_region (
            yield_id INTEGER,
            crop_key INTEGER,
            region_key INTEGER,
            production_tons DECIMAL(10,2)
        )
        PARTITION BY LIST (region_key);
        """
    
    def get_hash_partitioning(self) -> str:
        """Generate hash partitioning"""
        return """
        CREATE TABLE fact_crop_yield_hash (
            yield_id INTEGER,
            crop_key INTEGER,
            production_tons DECIMAL(10,2)
        )
        PARTITION BY HASH (yield_id);
        """
    
    def get_all_partition_strategies(self) -> Dict[str, str]:
        """Return all partition strategies"""
        return {
            "range_partitioning": self.get_range_partitioning(),
            "list_partitioning": self.get_list_partitioning(),
            "hash_partitioning": self.get_hash_partitioning()
        }

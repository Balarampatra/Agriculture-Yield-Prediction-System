"""
Star Schema Design for Agriculture Yield Prediction Data Warehouse

This module implements a star schema design with:
- Fact Table: fact_crop_yield
- Dimension Tables: dim_crop, dim_region, dim_weather, dim_time, dim_soil
"""

import pandas as pd
from typing import Dict, List, Optional


class StarSchema:
    """
    Star Schema implementation for Agriculture Yield Prediction
    """
    
    def __init__(self):
        self.fact_table = "fact_crop_yield"
        self.dimension_tables = [
            "dim_crop",
            "dim_region", 
            "dim_weather",
            "dim_time",
            "dim_soil"
        ]
    
    def create_fact_table(self) -> str:
        """Generate SQL to create fact table"""
        sql = """
        CREATE TABLE fact_crop_yield (
            yield_id INTEGER PRIMARY KEY,
            crop_key INTEGER REFERENCES dim_crop(crop_key),
            region_key INTEGER REFERENCES dim_region(region_key),
            weather_key INTEGER REFERENCES dim_weather(weather_key),
            time_key INTEGER REFERENCES dim_time(time_key),
            soil_key INTEGER REFERENCES dim_soil(soil_key),
            production_tons DECIMAL(10,2),
            area_hectares DECIMAL(10,2),
            yield_per_hectare DECIMAL(10,2),
            n_content DECIMAL(5,2),
            p_content DECIMAL(5,2),
            k_content DECIMAL(5,2),
            ph_level DECIMAL(4,2),
            rainfall_mm DECIMAL(8,2),
            temperature_celsius DECIMAL(5,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        return sql
    
    def create_dim_crop(self) -> str:
        """Generate SQL to create crop dimension table"""
        sql = """
        CREATE TABLE dim_crop (
            crop_key INTEGER PRIMARY KEY,
            crop_id VARCHAR(50) UNIQUE,
            crop_name VARCHAR(100),
            crop_category VARCHAR(50),
            growing_season VARCHAR(50),
            expected_yield_per_hectare DECIMAL(10,2)
        );
        """
        return sql
    
    def create_dim_region(self) -> str:
        """Generate SQL to create region dimension table"""
        sql = """
        CREATE TABLE dim_region (
            region_key INTEGER PRIMARY KEY,
            region_id VARCHAR(50) UNIQUE,
            state_name VARCHAR(100),
            district VARCHAR(100),
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            climate_zone VARCHAR(50)
        );
        """
        return sql
    
    def create_dim_weather(self) -> str:
        """Generate SQL to create weather dimension table"""
        sql = """
        CREATE TABLE dim_weather (
            weather_key INTEGER PRIMARY KEY,
            weather_id VARCHAR(50) UNIQUE,
            temperature_celsius DECIMAL(5,2),
            humidity_percent DECIMAL(5,2),
            rainfall_mm DECIMAL(8,2),
            wind_speed_kmh DECIMAL(5,2),
            weather_condition VARCHAR(50)
        );
        """
        return sql
    
    def create_dim_time(self) -> str:
        """Generate SQL to create time dimension table"""
        sql = """
        CREATE TABLE dim_time (
            time_key INTEGER PRIMARY KEY,
            full_date DATE UNIQUE,
            day_of_month INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER,
            season VARCHAR(50),
            is_peak_season BOOLEAN
        );
        """
        return sql
    
    def create_dim_soil(self) -> str:
        """Generate SQL to create soil dimension table"""
        sql = """
        CREATE TABLE dim_soil (
            soil_key INTEGER PRIMARY KEY,
            soil_id VARCHAR(50) UNIQUE,
            soil_type VARCHAR(50),
            ph_level DECIMAL(4,2),
            nitrogen_content DECIMAL(5,2),
            phosphorus_content DECIMAL(5,2),
            potassium_content DECIMAL(5,2),
            organic_matter_percent DECIMAL(5,2)
        );
        """
        return sql
    
    def create_all_tables(self) -> List[str]:
        """Generate SQL for all star schema tables"""
        return [
            self.create_dim_crop(),
            self.create_dim_region(),
            self.create_dim_weather(),
            self.create_dim_time(),
            self.create_dim_soil(),
            self.create_fact_table()
        ]
    
    def get_schema_diagram(self) -> Dict:
        """Return schema diagram information"""
        return {
            "schema_type": "Star Schema",
            "fact_table": self.fact_table,
            "dimension_tables": self.dimension_tables,
            "description": "Central fact table with denormalized dimension tables for optimized query performance"
        }

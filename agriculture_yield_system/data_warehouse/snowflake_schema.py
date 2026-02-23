"""
Snowflake Schema Design for Agriculture Yield Prediction Data Warehouse

This module implements a snowflake schema design with normalized dimension tables:
- Fact Table: fact_crop_yield
- Dimension Tables with hierarchies: dim_crop, dim_region, dim_time
- Sub-dimension Tables: dim_climate_zone, dim_soil_type
"""

import pandas as pd
from typing import Dict, List


class SnowflakeSchema:
    """
    Snowflake Schema implementation for Agriculture Yield Prediction
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
        self.sub_dimension_tables = [
            "dim_climate_zone",
            "dim_soil_type"
        ]
    
    def create_dim_climate_zone(self) -> str:
        """Generate SQL to create climate zone sub-dimension table"""
        sql = """
        CREATE TABLE dim_climate_zone (
            climate_zone_key INTEGER PRIMARY KEY,
            climate_zone_id VARCHAR(50) UNIQUE,
            climate_zone_name VARCHAR(100),
            temperature_range VARCHAR(50),
            rainfall_pattern VARCHAR(50),
            suitable_crops TEXT
        );
        """
        return sql
    
    def create_dim_soil_type(self) -> str:
        """Generate SQL to create soil type sub-dimension table"""
        sql = """
        CREATE TABLE dim_soil_type (
            soil_type_key INTEGER PRIMARY KEY,
            soil_type_id VARCHAR(50) UNIQUE,
            soil_type_name VARCHAR(100),
            drainage_quality VARCHAR(50),
            nutrient_retention VARCHAR(50)
        );
        """
        return sql
    
    def create_fact_table(self) -> str:
        """Generate SQL to create fact table with foreign keys to normalized dimensions"""
        sql = """
        CREATE TABLE fact_crop_yield (
            yield_id INTEGER PRIMARY KEY,
            crop_sub_category_key INTEGER REFERENCES dim_crop_sub_category(sub_category_key),
            region_district_key INTEGER REFERENCES dim_region_district(district_key),
            weather_key INTEGER REFERENCES dim_weather(weather_key),
            time_key INTEGER REFERENCES dim_time(time_key),
            soil_subtype_key INTEGER REFERENCES dim_soil_subtype(subtype_key),
            production_tons DECIMAL(10,2),
            area_hectares DECIMAL(10,2),
            yield_per_hectare DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        return sql
    
    def create_dim_crop_category(self) -> str:
        """Generate SQL to create crop category dimension"""
        sql = """
        CREATE TABLE dim_crop_category (
            category_key INTEGER PRIMARY KEY,
            category_id VARCHAR(50) UNIQUE,
            category_name VARCHAR(100),
            description TEXT
        );
        """
        return sql
    
    def create_dim_crop_sub_category(self) -> str:
        """Generate SQL to create crop sub-category dimension (normalizes dim_crop)"""
        sql = """
        CREATE TABLE dim_crop_sub_category (
            sub_category_key INTEGER PRIMARY KEY,
            category_key INTEGER REFERENCES dim_crop_category(category_key),
            sub_category_id VARCHAR(50) UNIQUE,
            sub_category_name VARCHAR(100),
            growing_season VARCHAR(50),
            expected_yield_per_hectare DECIMAL(10,2)
        );
        """
        return sql
    
    def create_dim_crop(self) -> str:
        """Generate SQL to create crop dimension (normalized through sub-category)"""
        sql = """
        CREATE TABLE dim_crop (
            crop_key INTEGER PRIMARY KEY,
            sub_category_key INTEGER REFERENCES dim_crop_sub_category(sub_category_key),
            crop_id VARCHAR(50) UNIQUE,
            crop_name VARCHAR(100),
            variety VARCHAR(100),
            maturity_days INTEGER
        );
        """
        return sql
    
    def create_dim_region_country(self) -> str:
        """Generate SQL to create country dimension"""
        sql = """
        CREATE TABLE dim_region_country (
            country_key INTEGER PRIMARY KEY,
            country_id VARCHAR(50) UNIQUE,
            country_name VARCHAR(100),
            region_code VARCHAR(10)
        );
        """
        return sql
    
    def create_dim_region_state(self) -> str:
        """Generate SQL to create state dimension"""
        sql = """
        CREATE TABLE dim_region_state (
            state_key INTEGER PRIMARY KEY,
            country_key INTEGER REFERENCES dim_region_country(country_key),
            state_id VARCHAR(50) UNIQUE,
            state_name VARCHAR(100),
            climate_zone_key INTEGER REFERENCES dim_climate_zone(climate_zone_key)
        );
        """
        return sql
    
    def create_dim_region_district(self) -> str:
        """Generate SQL to create district dimension (normalizes dim_region)"""
        sql = """
        CREATE TABLE dim_region_district (
            district_key INTEGER PRIMARY KEY,
            state_key INTEGER REFERENCES dim_region_state(state_key),
            district_id VARCHAR(50) UNIQUE,
            district_name VARCHAR(100),
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6)
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
        """Generate SQL to create soil base dimension"""
        sql = """
        CREATE TABLE dim_soil (
            soil_key INTEGER PRIMARY KEY,
            soil_id VARCHAR(50) UNIQUE,
            soil_type_key INTEGER REFERENCES dim_soil_type(soil_type_key),
            ph_level DECIMAL(4,2),
            nitrogen_content DECIMAL(5,2),
            phosphorus_content DECIMAL(5,2),
            potassium_content DECIMAL(5,2),
            organic_matter_percent DECIMAL(5,2)
        );
        """
        return sql
    
    def create_dim_soil_subtype(self) -> str:
        """Generate SQL to create soil subtype dimension (normalizes dim_soil)"""
        sql = """
        CREATE TABLE dim_soil_subtype (
            subtype_key INTEGER PRIMARY KEY,
            soil_type_key INTEGER REFERENCES dim_soil_type(soil_type_key),
            subtype_id VARCHAR(50) UNIQUE,
            subtype_name VARCHAR(100),
            description TEXT
        );
        """
        return sql
    
    def create_all_tables(self) -> List[str]:
        """Generate SQL for all snowflake schema tables"""
        return [
            self.create_dim_climate_zone(),
            self.create_dim_soil_type(),
            self.create_dim_crop_category(),
            self.create_dim_crop_sub_category(),
            self.create_dim_crop(),
            self.create_dim_region_country(),
            self.create_dim_region_state(),
            self.create_dim_region_district(),
            self.create_dim_weather(),
            self.create_dim_time(),
            self.create_dim_soil_subtype(),
            self.create_dim_soil(),
            self.create_fact_table()
        ]
    
    def get_schema_diagram(self) -> Dict:
        """Return schema diagram information"""
        return {
            "schema_type": "Snowflake Schema",
            "fact_table": self.fact_table,
            "dimension_tables": self.dimension_tables,
            "sub_dimension_tables": self.sub_dimension_tables,
            "description": "Normalized dimension tables with hierarchical structures reducing data redundancy"
        }

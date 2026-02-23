"""
Advanced SQL Queries Module
Contains CTE (Common Table Expressions), Window Functions, and complex queries
for Agriculture Yield Prediction Data Warehouse
"""

from typing import List, Dict, Any
import pandas as pd


class AdvancedSQLQueries:
    """
    Advanced SQL queries using CTE and Window Functions
    for agricultural data analysis
    """
    
    def __init__(self):
        self.table_name = "fact_crop_yield"
    
    def cte_yield_by_crop(self) -> str:
        """
        CTE to calculate average yield by crop type
        """
        sql = """
        WITH CropYieldCTE AS (
            SELECT 
                c.crop_name,
                c.crop_category,
                AVG(f.yield_per_hectare) AS avg_yield,
                COUNT(*) AS total_records,
                MAX(f.yield_per_hectare) AS max_yield,
                MIN(f.yield_per_hectare) AS min_yield
            FROM fact_crop_yield f
            JOIN dim_crop c ON f.crop_key = c.crop_key
            GROUP BY c.crop_name, c.crop_category
        )
        SELECT 
            crop_name,
            crop_category,
            avg_yield,
            total_records,
            max_yield,
            min_yield
        FROM CropYieldCTE
        ORDER BY avg_yield DESC;
        """
        return sql
    
    def cte_yield_by_region(self) -> str:
        """
        CTE to analyze yield performance by region
        """
        sql = """
        WITH RegionYieldCTE AS (
            SELECT 
                r.state_name,
                r.district,
                r.climate_zone,
                SUM(f.production_tons) AS total_production,
                SUM(f.area_hectares) AS total_area,
                AVG(f.yield_per_hectare) AS avg_yield
            FROM fact_crop_yield f
            JOIN dim_region r ON f.region_key = r.region_key
            GROUP BY r.state_name, r.district, r.climate_zone
        ),
        RegionalStatsCTE AS (
            SELECT 
                state_name,
                climate_zone,
                SUM(total_production) AS state_total_production,
                AVG(avg_yield) AS state_avg_yield
            FROM RegionYieldCTE
            GROUP BY state_name, climate_zone
        )
        SELECT * FROM RegionalStatsCTE
        ORDER BY state_total_production DESC;
        """
        return sql
    
    def cte_yield_trend_analysis(self) -> str:
        """
        CTE with Window Functions to analyze yield trends over time
        """
        sql = """
        WITH YieldTrendCTE AS (
            SELECT 
                t.year,
                t.season,
                c.crop_name,
                f.yield_per_hectare,
                AVG(f.yield_per_hectare) OVER (
                    PARTITION BY c.crop_name, t.season 
                    ORDER BY t.year 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS moving_avg_3yr,
                LAG(f.yield_per_hectare) OVER (
                    PARTITION BY c.crop_name, t.season 
                    ORDER BY t.year
                ) AS prev_year_yield,
                f.yield_per_hectare - LAG(f.yield_per_hectare) OVER (
                    PARTITION BY c.crop_name, t.season 
                    ORDER BY t.year
                ) AS year_over_year_change
            FROM fact_crop_yield f
            JOIN dim_time t ON f.time_key = t.time_key
            JOIN dim_crop c ON f.crop_key = c.crop_key
        )
        SELECT 
            year,
            season,
            crop_name,
            yield_per_hectare,
            moving_avg_3yr,
            prev_year_yield,
            year_over_year_change
        FROM YieldTrendCTE
        WHERE prev_year_yield IS NOT NULL
        ORDER BY crop_name, season, year;
        """
        return sql
    
    def window_function_ranking(self) -> str:
        """
        Window Functions to rank crops by yield within each region
        """
        sql = """
        SELECT 
            r.state_name,
            c.crop_name,
            AVG(f.yield_per_hectare) AS avg_yield,
            RANK() OVER (
                PARTITION BY r.state_name 
                ORDER BY AVG(f.yield_per_hectare) DESC
            ) AS yield_rank,
            DENSE_RANK() OVER (
                PARTITION BY r.state_name 
                ORDER BY AVG(f.yield_per_hectare) DESC
            ) AS dense_yield_rank,
            PERCENT_RANK() OVER (
                PARTITION BY r.state_name 
                ORDER BY AVG(f.yield_per_hectare) DESC
            ) AS percent_rank,
            NTILE(4) OVER (
                PARTITION BY r.state_name 
                ORDER BY AVG(f.yield_per_hectare) DESC
            ) AS quartile
        FROM fact_crop_yield f
        JOIN dim_region r ON f.region_key = r.region_key
        JOIN dim_crop c ON f.crop_key = c.crop_key
        GROUP BY r.state_name, c.crop_name
        ORDER BY r.state_name, yield_rank;
        """
        return sql
    
    def window_function_running_totals(self) -> str:
        """
        Window Functions to calculate running totals and cumulative percentages
        """
        sql = """
        SELECT 
            t.year,
            c.crop_name,
            SUM(f.production_tons) AS yearly_production,
            SUM(SUM(f.production_tons)) OVER (
                PARTITION BY c.crop_name 
                ORDER BY t.year 
                ROWS UNBOUNDED PRECEDING
            ) AS running_total_production,
            SUM(f.production_tons) * 1.0 / 
                SUM(SUM(f.production_tons)) OVER (PARTITION BY c.crop_name) * 100
            AS percent_of_total
        FROM fact_crop_yield f
        JOIN dim_time t ON f.time_key = t.time_key
        JOIN dim_crop c ON f.crop_key = c.crop_key
        GROUP BY t.year, c.crop_name
        ORDER BY c.crop_name, t.year;
        """
        return sql
    
    def cte_weather_impact_analysis(self) -> str:
        """
        CTE to analyze weather impact on crop yields
        """
        sql = """
        WITH WeatherYieldCTE AS (
            SELECT 
                w.weather_condition,
                w.temperature_celsius,
                w.rainfall_mm,
                c.crop_name,
                AVG(f.yield_per_hectare) AS avg_yield,
                COUNT(*) AS observation_count
            FROM fact_crop_yield f
            JOIN dim_weather w ON f.weather_key = w.weather_key
            JOIN dim_crop c ON f.crop_key = c.crop_key
            GROUP BY w.weather_condition, w.temperature_celsius, w.rainfall_mm, c.crop_name
        ),
        WeatherStatsCTE AS (
            SELECT 
                weather_condition,
                AVG(avg_yield) AS condition_avg_yield,
                MIN(avg_yield) AS min_yield_observed,
                MAX(avg_yield) AS max_yield_observed
            FROM WeatherYieldCTE
            GROUP BY weather_condition
        )
        SELECT 
            w.weather_condition,
            w.temperature_celsius,
            w.rainfall_mm,
            w.crop_name,
            w.avg_yield,
            ws.condition_avg_yield,
            w.avg_yield - ws.condition_avg_yield AS deviation_from_avg
        FROM WeatherYieldCTE w
        JOIN WeatherStatsCTE ws ON w.weather_condition = ws.weather_condition
        ORDER BY ws.condition_avg_yield DESC;
        """
        return sql
    
    def cte_soil_fertility_analysis(self) -> str:
        """
        CTE to analyze soil fertility impact on yields
        """
        sql = """
        WITH SoilFertilityCTE AS (
            SELECT 
                s.soil_type,
                s.ph_level,
                s.nitrogen_content,
                s.phosphorus_content,
                s.potassium_content,
                c.crop_name,
                AVG(f.yield_per_hectare) AS avg_yield,
                COUNT(*) AS sample_count
            FROM fact_crop_yield f
            JOIN dim_soil s ON f.soil_key = s.soil_key
            JOIN dim_crop c ON f.crop_key = c.crop_key
            GROUP BY s.soil_type, s.ph_level, s.nitrogen_content, 
                     s.phosphorus_content, s.potassium_content, c.crop_name
        ),
        NutrientRangeCTE AS (
            SELECT 
                crop_name,
                MIN(nitrogen_content) AS min_n,
                MAX(nitrogen_content) AS max_n,
                MIN(phosphorus_content) AS min_p,
                MAX(phosphorus_content) AS max_p,
                MIN(potassium_content) AS min_k,
                MAX(potassium_content) AS max_k
            FROM SoilFertilityCTE
            GROUP BY crop_name
        )
        SELECT 
            sf.soil_type,
            sf.crop_name,
            sf.avg_yield,
            sf.sample_count,
            nr.min_n, nr.max_n,
            nr.min_p, nr.max_p,
            nr.min_k, nr.max_k
        FROM SoilFertilityCTE sf
        JOIN NutrientRangeCTE nr ON sf.crop_name = nr.crop_name
        ORDER BY sf.crop_name, sf.avg_yield DESC;
        """
        return sql
    
    def complex_analytics_query(self) -> str:
        """
        Complex query combining CTE and Window Functions for comprehensive analysis
        """
        sql = """
        WITH YieldByRegionCTE AS (
            SELECT 
                r.state_name,
                r.district,
                c.crop_name,
                t.year,
                t.season,
                SUM(f.production_tons) AS production,
                SUM(f.area_hectares) AS area,
                AVG(f.yield_per_hectare) AS yield
            FROM fact_crop_yield f
            JOIN dim_region r ON f.region_key = r.region_key
            JOIN dim_crop c ON f.crop_key = c.crop_key
            JOIN dim_time t ON f.time_key = t.time_key
            GROUP BY r.state_name, r.district, c.crop_name, t.year, t.season
        ),
        YearlyStatsCTE AS (
            SELECT 
                crop_name,
                year,
                SUM(production) AS yearly_production,
                SUM(area) AS yearly_area,
                AVG(yield) AS yearly_avg_yield,
                MAX(yield) AS yearly_max_yield,
                MIN(yield) AS yearly_min_yield
            FROM YieldByRegionCTE
            GROUP BY crop_name, year
        ),
        RankedCropsCTE AS (
            SELECT 
                crop_name,
                year,
                yearly_production,
                yearly_avg_yield,
                RANK() OVER (PARTITION BY year ORDER BY yearly_production DESC) AS production_rank,
                RANK() OVER (PARTITION BY year ORDER BY yearly_avg_yield DESC) AS yield_rank
            FROM YearlyStatsCTE
        )
        SELECT 
            rc.year,
            rc.crop_name,
            rc.yearly_production,
            rc.yearly_avg_yield,
            rc.production_rank,
            rc.yield_rank,
            yrc.district AS top_producing_district,
            yrc.yield AS top_district_yield
        FROM RankedCropsCTE rc
        JOIN YieldByRegionCTE yrc 
            ON rc.crop_name = yrc.crop_name 
            AND rc.year = yrc.year
            AND yrc.yield = (
                SELECT MAX(yield) 
                FROM YieldByRegionCTE yrc2 
                WHERE yrc2.crop_name = rc.crop_name 
                AND yrc2.year = rc.year
            )
        WHERE rc.production_rank <= 10
        ORDER BY rc.year, rc.production_rank;
        """
        return sql
    
    def get_all_queries(self) -> Dict[str, str]:
        """Return all advanced SQL queries"""
        return {
            "cte_yield_by_crop": self.cte_yield_by_crop(),
            "cte_yield_by_region": self.cte_yield_by_region(),
            "cte_yield_trend_analysis": self.cte_yield_trend_analysis(),
            "window_function_ranking": self.window_function_ranking(),
            "window_function_running_totals": self.window_function_running_totals(),
            "cte_weather_impact_analysis": self.cte_weather_impact_analysis(),
            "cte_soil_fertility_analysis": self.cte_soil_fertility_analysis(),
            "complex_analytics_query": self.complex_analytics_query()
        }

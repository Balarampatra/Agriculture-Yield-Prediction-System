"""
ETL Pipeline Module
Python-based ETL/ELT pipelines for Agriculture Yield Data Processing
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging


class ETLPipeline:
    """
    ETL Pipeline for Agriculture Yield Prediction Data
    Handles Extract, Transform, Load operations
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self.extracted_data = {}
        self.transformed_data = {}
        self.load_status = {}
    
    def extract(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from various sources
        """
        self.logger.info(f"Extracting data from {source}")
        
        if source == "csv":
            return self._extract_from_csv(kwargs.get("path"))
        elif source == "database":
            return self._extract_from_database(kwargs.get("query"))
        elif source == "api":
            return self._extract_from_api(kwargs.get("endpoint"))
        elif source == "excel":
            return self._extract_from_excel(kwargs.get("path"))
        else:
            raise ValueError(f"Unsupported source: {source}")
    
    def _extract_from_csv(self, path: str) -> pd.DataFrame:
        """Extract data from CSV file"""
        return pd.read_csv(path)
    
    def _extract_from_database(self, query: str) -> pd.DataFrame:
        """Extract data from database"""
        # Placeholder for database connection
        self.logger.info(f"Executing query: {query}")
        return pd.DataFrame()
    
    def _extract_from_api(self, endpoint: str) -> pd.DataFrame:
        """Extract data from API"""
        self.logger.info(f"Fetching from API: {endpoint}")
        return pd.DataFrame()
    
    def _extract_from_excel(self, path: str) -> pd.DataFrame:
        """Extract data from Excel file"""
        return pd.read_excel(path)
    
    def transform(self, df: pd.DataFrame, transformations: List[str]) -> pd.DataFrame:
        """
        Apply transformations to the data
        """
        self.logger.info(f"Applying transformations: {transformations}")
        
        result_df = df.copy()
        
        for transform in transformations:
            if transform == "clean_missing":
                result_df = self._clean_missing_values(result_df)
            elif transform == "normalize":
                result_df = self._normalize_data(result_df)
            elif transform == "feature_engineering":
                result_df = self._create_features(result_df)
            elif transform == "encode_categorical":
                result_df = self._encode_categorical(result_df)
            elif transform == "outlier_detection":
                result_df = self._handle_outliers(result_df)
        
        self.transformed_data = result_df
        return result_df
    
    def _clean_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values"""
        # Fill numeric columns with median
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            df[col].fillna(df[col].median(), inplace=True)
        
        # Fill categorical columns with mode
        cat_cols = df.select_dtypes(include=['object']).columns
        for col in cat_cols:
            df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else "Unknown", inplace=True)
        
        return df
    
    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize numeric columns"""
        from sklearn.preprocessing import MinMaxScaler
        
        scaler = MinMaxScaler()
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            df[col] = scaler.fit_transform(df[[col]])
        
        return df
    
    def _create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create new features for the model"""
        # Create yield per hectare if not exists
        if 'production' in df.columns and 'area' in df.columns:
            df['yield_per_hectare'] = df['production'] / df['area'].replace(0, np.nan)
        
        # Create temperature categories
        if 'temperature' in df.columns:
            df['temp_category'] = pd.cut(df['temperature'], 
                                         bins=[0, 15, 25, 35, 50],
                                         labels=['Cold', 'Moderate', 'Warm', 'Hot'])
        
        # Create rainfall categories
        if 'rainfall' in df.columns:
            df['rainfall_category'] = pd.cut(df['rainfall'],
                                             bins=[0, 50, 100, 200, 500],
                                             labels=['Low', 'Medium', 'High', 'Very High'])
        
        return df
    
    def _encode_categorical(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode categorical variables"""
        from sklearn.preprocessing import LabelEncoder
        
        cat_cols = df.select_dtypes(include=['object']).columns
        encoders = {}
        
        for col in cat_cols:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
            encoders[col] = le
        
        return df
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect and handle outliers using IQR method"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Cap outliers
            df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
        
        return df
    
    def load(self, df: pd.DataFrame, destination: str, **kwargs) -> bool:
        """
        Load data to destination
        """
        self.logger.info(f"Loading data to {destination}")
        
        if destination == "csv":
            return self._load_to_csv(df, kwargs.get("path"))
        elif destination == "database":
            return self._load_to_database(df, kwargs.get("table"))
        elif destination == "snowflake":
            return self._load_to_snowflake(df, kwargs.get("table"))
        else:
            raise ValueError(f"Unsupported destination: {destination}")
    
    def _load_to_csv(self, df: pd.DataFrame, path: str) -> bool:
        """Load data to CSV"""
        df.to_csv(path, index=False)
        return True
    
    def _load_to_database(self, df: pd.DataFrame, table: str) -> bool:
        """Load data to database"""
        self.logger.info(f"Loading to table: {table}")
        return True
    
    def _load_to_snowflake(self, df: pd.DataFrame, table: str) -> bool:
        """Load data to Snowflake"""
        self.logger.info(f"Loading to Snowflake table: {table}")
        return True
    
    def run_pipeline(self, 
                     source: str, 
                     destination: str,
                     transformations: List[str],
                     **kwargs) -> bool:
        """
        Run the complete ETL pipeline
        """
        # Extract
        df = self.extract(source, **kwargs)
        self.extracted_data = df
        
        # Transform
        df = self.transform(df, transformations)
        
        # Load
        success = self.load(df, destination, **kwargs)
        
        return success


class DataQualityChecker:
    """
    Data Quality Checker for ETL Pipeline
    """
    
    def __init__(self):
        self.quality_report = {}
    
    def check_completeness(self, df: pd.DataFrame) -> Dict:
        """Check data completeness"""
        total_cells = df.size
        missing_cells = df.isnull().sum().sum()
        completeness = (total_cells - missing_cells) / total_cells * 100
        
        return {
            "total_cells": total_cells,
            "missing_cells": missing_cells,
            "completeness_percent": round(completeness, 2)
        }
    
    def check_validity(self, df: pd.DataFrame) -> Dict:
        """Check data validity"""
        issues = []
        
        # Check for negative values in production/area
        if 'production' in df.columns:
            neg_prod = (df['production'] < 0).sum()
            if neg_prod > 0:
                issues.append(f"Found {neg_prod} negative production values")
        
        if 'area' in df.columns:
            neg_area = (df['area'] < 0).sum()
            if neg_area > 0:
                issues.append(f"Found {neg_area} negative area values")
        
        return {"issues": issues, "is_valid": len(issues) == 0}
    
    def check_uniqueness(self, df: pd.DataFrame, column: str) -> Dict:
        """Check uniqueness of a column"""
        if column not in df.columns:
            return {"error": f"Column {column} not found"}
        
        total = len(df)
        unique = df[column].nunique()
        
        return {
            "total": total,
            "unique": unique,
            "duplicate_count": total - unique,
            "uniqueness_percent": round(unique / total * 100, 2)
        }
    
    def generate_quality_report(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive data quality report"""
        return {
            "completeness": self.check_completeness(df),
            "validity": self.check_validity(df),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns)
        }

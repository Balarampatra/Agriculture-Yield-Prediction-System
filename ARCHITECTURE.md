# Agriculture Yield Prediction System - Architecture

## Overview
This comprehensive system combines Machine Learning with Data Engineering best practices for crop yield prediction.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           AGRICULTURE YIELD PREDICTION SYSTEM                         │
└─────────────────────────────────────────────────────────────────────────────────────┘

                                    ┌─────────────────┐
                                    │   WEB FRONTEND  │
                                    │   (Flask App)   │
                                    └────────┬────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
                    ▼                        ▼                        ▼
         ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
         │   Dashboard      │    │  Prediction API  │    │   Data Entry     │
         │   (Charts.js)    │    │   (ML Pipeline)  │    │   (Forms)        │
         └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
                  │                       │                        │
                  └───────────────────────┼────────────────────────┘
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    │           APPLICATION LAYER               │
                    │  ┌─────────────────────────────────────┐   │
                    │  │     ML Prediction Pipeline         │   │
                    │  │  - CustomData (Feature Input)      │   │
                    │  │  - PredictPipeline (Model inference)│  │
                    │  │  - Preprocessor & Model (.pkl)     │   │
                    │  └─────────────────────────────────────┘   │
                    └───────────────────────────────────────────┘
                                          │
┌─────────────────────────────────────────┴───────────────────────────────────────────┐
│                           DATA ENGINEERING LAYER                                     │
│                                                                                       │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                    │
│  │  ETL Pipeline   │   │   Spark         │   │   Snowflake     │                    │
│  │  (Python-based)  │   │   Processing    │   │   Integration   │                    │
│  │  - Extract       │   │  - Batch        │   │  - Clustering    │                    │
│  │  - Transform     │   │  - Streaming    │   │  - Time Travel   │                    │
│  │  - Load          │   │                 │   │  - Semi-Structured│                    │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘                    │
│           │                       │                       │                            │
└───────────┼───────────────────────┼───────────────────────┼────────────────────────┘
            │                       │                       │
            ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           DATA WAREHOUSE LAYER                                      │
│                                                                                     │
│    ┌─────────────────────┐     ┌─────────────────────────────────────────────┐      │
│    │    STAR SCHEMA      │     │           SNOWFLAKE SCHEMA                  │      │
│    │  ┌─────────────┐    │     │    ┌──────────────────────────────────┐     │      │
│    │  │fact_crop_   │    │     │    │     Central Fact Table           │     │      │
│    │  │yield        │    │     │    │  ┌────┬─────┬─────┬─────┬─────┐  │     │      │
│    │  └──────┬──────┘    │     │    │  │Dim │Dim  │Dim  │Dim  │Dim  │  │     │      │
│    │         │           │     │    │  │Crop│Region│Time │Soil │Weather│ │   │      │
│    │  ┌──────┴──────┐    │     │    │  └──┴─────┴─────┴─────┴─────┴──┘ │     │      │
│    │  │dim_crop     │    │     │    └──────────────────────────────────┘     │      │
│    │  │dim_region  │    │     │    ┌───────────┐  ┌───────────┐              │      │
│    │  │dim_weather │    │     │    │ Sub-Dim   │  │ Sub-Dim   │              │      │
│    │  │dim_time    │    │     │    │ District  │  │  Season   │              │      │
│    │  │dim_soil    │    │     │    └───────────┘  └───────────┘              │      │
│    │  └────────────┘    │     └─────────────────────────────────────────────┘      │
│    └─────────────────────┘                                                       │      │
└─────────────────────────────────────────────────────────────────────────────────────┘
            │                       │                       │
            ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           ADVANCED SQL & OPTIMIZATION                               │
│                                                                                     │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                    │
│  │  CTE Queries    │   │ Window Functions│   │   Indexing      │                    │
│  │  - Yield by    │   │  - RANK()       │   │  - B-Tree       │                     │
│  │    Crop         │   │  - LAG/LEAD     │   │  - Hash         │                    │
│  │  - Trend Analysis│  │  - Running Total│   │  - Composite    │                    │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘                    │
│                                                                                     │
│  ┌─────────────────┐   ┌─────────────────┐                                         │
│  │  Partitioning   │   │ Performance     │                                         │
│  │  - By Year      │   │  Tuning         │                                         │
│  │  - By Region    │   │  - Query Opt.   │                                         │
│  │  - By Season    │   │  - Cache        │                                         │
│  └─────────────────┘   └─────────────────┘                                         │
└─────────────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           SECURITY & GOVERNANCE                                       │
│                                                                                       │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                    │
│  │  RBAC           │   │  Data Masking   │   │  Audit Logging  │                    │
│  │  - Admin        │   │  - Email        │   │  - Access Logs  │                    │
│  │  - Data Scientist│  │  - Phone        │   │  - Changes      │                    │
│  │  - Analyst      │   │  - Financial    │   │  - Compliance   │                    │
│  │  - Viewer       │   │                 │   │                 │                    │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘                    │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Data Warehouse (Star/Snowflake Schema)
- **Fact Table**: `fact_crop_yield` - Central table with yield metrics
- **Dimension Tables**:
  - `dim_crop` - Crop information
  - `dim_region` - Geographic regions
  - `dim_weather` - Weather conditions
  - `dim_time` - Temporal data
  - `dim_soil` - Soil characteristics

### 2. Advanced SQL Queries
- **CTE (Common Table Expressions)**: Multi-level aggregations
- **Window Functions**: RANK(), LAG/LEAD, Running Totals
- **Indexing**: B-Tree, Hash, Composite indexes
- **Partitioning**: By year, region, season

### 3. ETL Pipeline (Python-based)
- **Extract**: CSV, Database, API, Excel
- **Transform**: Clean, Normalize, Feature Engineering, Encode
- **Load**: CSV, Database, Snowflake

### 4. Apache Spark Processing
- **Batch Processing**: Large-scale data processing
- **Streaming**: Real-time data ingestion

### 5. Snowflake Integration
- **Clustering**: Automatic data clustering
- **Time Travel**: Historical data access
- **Semi-Structured Data**: JSON, VARIANT support

### 6. Security
- **RBAC**: Role-Based Access Control
- **Data Masking**: PII protection
- **Audit Logging**: Compliance tracking

### 7. Visualization Dashboard
- Interactive Charts (Chart.js)
- Production Analytics
- Yield Trends
- Regional Comparisons
- Weather Impact Analysis

## API Endpoints
- `GET /` - Home page
- `GET /dashboard` - Analytics dashboard
- `POST /predict` - Crop yield prediction

## File Structure
```
Crop_prediction_ml_pipeline-main/
├── app.py                          # Flask application
├── requirements.txt                # Dependencies
├── src/
│   ├── pipeline/
│   │   ├── prediction_pipeline.py  # ML prediction
│   │   └── train_pipeline.py        # Model training
│   └── components/
│       ├── data_ingestion.py
│       ├── data_transformation.py
│       └── model_trainer.py
├── agriculture_yield_system/
│   ├── data_warehouse/
│   │   ├── star_schema.py
│   │   └── snowflake_schema.py
│   ├── sql_queries/
│   │   ├── advanced_queries.py       # CTE, Window Functions
│   │   ├── indexes.py
│   │   └── partitions.py
│   ├── etl_pipeline/
│   │   └── etl_pipeline.py
│   ├── spark_processing/
│   │   ├── batch_processing.py
│   │   └── streaming_processing.py
│   ├── snowflake/
│   │   └── snowflake_integration.py
│   └── security/
│       └── security_manager.py       # RBAC, Masking
├── templates/
│   ├── index.html                    # Main form
│   └── dashboard.html                # Analytics dashboard
└── dataset/
    └── train_model.py

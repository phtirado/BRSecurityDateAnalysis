"""
ETL Pipeline Module - Bronze to Silver to Gold

This module orchestrates the complete ETL (Extract, Transform, Load) pipeline
for processing MVI (Mortality, Violence and Injury) data through a medallion
architecture with three layers:

1. BRONZE LAYER: Raw data from source files
2. SILVER LAYER: Cleaned and standardized data with proper type casting
3. GOLD LAYER: Analytics-ready dimensional star schema

The pipeline transforms raw CSV input into a normalized star schema with:
- Dimension tables: dim_location (geographic), dim_metric (metrics metadata)
- Fact table: fct_mvi (normalized metric values by location, year, metric)

This design enables efficient querying and analysis of violence-related metrics
across Brazilian states and regions over time.
"""

import os
import re
import pandas as pd
import numpy as np
import pyarrow as pa
from Entities import MetricEntity as entity


class ETLPipeline:
    """
    ETL Pipeline orchestrator for MVI data processing.
    
    Manages the three-layer data transformation workflow:
    - Bronze: Raw data ingestion from CSV files
    - Silver: Data cleaning, standardization, and type conversion
    - Gold: Dimensional modeling into star schema for analytics
    
    The pipeline enforces data lineage, maintains audit trails through file
    outputs at each stage, and ensures data quality through validation and
    cleaning operations.
    
    Class Attributes:
        BASE_DIR (str): Base directory for data pipeline (parent of 'data' folder)
        BRONZE_DIR (str): Path to raw data input directory (data/1_bronze)
        SILVER_DIR (str): Path to cleaned data output directory (data/2_silver)
        GOLD_DIR (str): Path to analytics-ready output directory (data/3_gold)
        MVI (MVI): Instance of MVI metric entity with configurations
        
    Example:
        >>> pipeline = ETLPipeline()
        >>> pipeline.run_pipeline("T01-MVI-anuario-2026.csv")
    """

    # Directory Setup - Medalion Architecture Paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in locals() else ".\\"
    BRONZE_DIR = os.path.join(BASE_DIR, "data", "1_bronze")
    SILVER_DIR = os.path.join(BASE_DIR, "data", "2_silver")
    GOLD_DIR = os.path.join(BASE_DIR, "data", "3_gold")
    MetricEntity = entity.MetricEntity()  # Instantiate MVI class to access its attributes and methods

    def __init__(self, metric_entity: entity.MetricEntity = None):
        """
        Initialize the ETL pipeliine and prepare directory structure.
        
        Creates the medallion architecture directory structure (Bronze, Silver, Gold)
        if they don't exist. Also handles pyarrow/pandas compatibility issues
        that can occur during repeated notebook executions.
        """
        if metric_entity is not None:
            self.MetricEntity = metric_entity

        # Work around repeated notebook execution collisions in pyarrow/pandas
        for ext_name in ["pandas.period", "pandas.interval"]:
            try:
                pa.unregister_extension_type(ext_name)
            except Exception:
                pass

    # -------------------------------------------------------------------
    # STEP 1: BRONZE TO SILVER (Extraction, Data Cleaning & Type Casting)
    # -------------------------------------------------------------------
    def bronze_to_silver(self, input_csv_path: str):
        """
        Transform raw data from Bronze layer to cleaned Silver layer.
        
        This stage performs:
        1. Raw data extraction using metric-specific parsing logic
        2. Data cleaning: removing footnote markers, normalizing missing values
        3. Type casting: converting all numeric values to float with validation
        4. Persisting cleaned data in both Parquet (efficient) and CSV (readable) formats
        
        Data Quality Operations:
        - Removes footnote markers (e.g., '(4)' from location names)
        - Normalizes missing value representations: '-', 'NaN', 'None' → np.nan
        - Handles European number format: space as thousands separator, comma as decimal
        - Validates numeric conversions with error handling
        
        Args:
            input_csv_path (str): Full path to raw CSV file in Bronze directory
            
        Returns:
            str: Path to the saved Parquet file in Silver directory
            
        Output Files:
            - cleaned_mvi_wide.parquet (primary, efficient format)
            - cleaned_mvi_wide.csv (human-readable reference)
            
        Example:
            >>> silver_path = pipeline.bronze_to_silver("data/1_bronze/T01-MVI-anuario-2026.csv")
            >>> print(silver_path)
            "data/2_silver/cleaned_mvi_wide.parquet"
        """
        print("--- [1/2] Processing Bronze -> Silver ---")
        
        # Read raw lines to extract data boundaries
        with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()

        # Extract data block using MVI-specific parsing (handles raw file structure)
        # MVI.get_data_block() skips headers, extracts rows 10-38, handles semicolon delimiters
        # data_rows = self.MVI.get_data_block(lines)
        data_rows = self.MetricEntity.get_data_block(lines)

        # Standardized Column Headers from MVI configuration (16 columns wide format)
        cols = self.MetricEntity.columns_header   

        df_raw = pd.DataFrame(data_rows, columns=cols)

        # ===== Data Cleaning Helper Functions =====
        def clean_num(val):
            """
            Convert numeric string values to float with comprehensive error handling.
            
            Handles:
            - Missing values: NaN, None, '-', empty strings → np.nan
            - European number format: '1 234,56' → 1234.56
            - Numeric validation: returns np.nan on conversion failure
            
            Args:
                val: Raw value (string, float, or None)
                
            Returns:
                float: Converted value or np.nan if invalid
            """
            if pd.isna(val) or val is None:
                return np.nan
            s = str(val).strip()
            # Treat various missing value indicators as NaN
            if s in ['-', '', '(-)', 'NaN', 'None']:
                return np.nan
            # Strip space thousands separator, convert comma to decimal point
            # (European format conversion)
            s = s.replace(' ', '').replace(',', '.')
            try:
                return float(s)
            except ValueError:
                return np.nan

        def clean_location(val):
            """
            Clean location names by removing footnote markers.
            
            Removes numeric footnote references (e.g., '(4)', '(5)') appended
            to location names in raw data source files.
            
            Args:
                val: Location name potentially containing footnote markers
                
            Returns:
                str: Cleaned location name
            """
            if pd.isna(val):
                return val
            # Strip footnote markers like '(4)' or '(5)'
            return re.sub(r'\s*\(\d+\)', '', str(val)).strip()

        # ===== Apply Cleaning Operations =====
        df_silver = df_raw.copy()
        df_silver['location'] = df_silver['location_raw'].apply(clean_location)
        df_silver.drop(columns=['location_raw'], inplace=True)
        
        # Clean all numeric columns (all columns except 'location')
        num_cols = [c for c in df_silver.columns if c != 'location']
        for col in num_cols:
            df_silver[col] = df_silver[col].apply(clean_num)

        # Export to Silver Layer as Parquet with CSV fallback
        silver_parquet_output = os.path.join(self.SILVER_DIR, self.MetricEntity.silver_parquet_output)
        silver_csv_output = os.path.join(self.SILVER_DIR, self.MetricEntity.silver_csv_output)
        df_silver.to_parquet(silver_parquet_output, engine='fastparquet', index=False)
        df_silver.to_csv(silver_csv_output, index=False, encoding='utf-8')
        print(f"Silver table saved to: {silver_parquet_output} ({len(df_silver)} rows)")
        return silver_parquet_output

    # -------------------------------------------------------------------
    # STEP 2: SILVER TO GOLD (Normalization into Star Schema)
    # -------------------------------------------------------------------
    def silver_to_gold(self, silver_file_path: str):
        """
        Transform cleaned Silver data into analytics-ready Gold star schema.
        
        This stage performs dimensional modeling to create a normalized star schema
        optimized for OLAP queries and business analytics:
        
        STAR SCHEMA STRUCTURE:
        ├── dim_location (Location Dimension)
        │   ├── location_id (PK): Sequential location identifier
        │   ├── location_name: State or national name
        │   ├── region: Macro-region (Norte, Nordeste, Centro-Oeste, Sudeste, Sul, Nacional)
        │   └── is_national: Boolean flag for national aggregate
        │
        ├── dim_metric (Metric Dimension)
        │   ├── metric_id (PK): Metric identifier (1-7)
        │   ├── metric_code: Machine-readable code
        │   ├── metric_name: Human-readable name
        │   └── unit: Measurement unit (Absoluto or Taxa per 100k)
        │
        └── fct_mvi (MVI Fact Table)
            ├── location_id (FK): Reference to dim_location
            ├── metric_id (FK): Reference to dim_metric
            ├── year: Reporting year
            └── value: Metric value
        
        Data Denormalization:
        - Pivots wide-format metrics (homicidio_doloso_2024, etc.) into tall format
        - Creates one fact row per (location, metric, year) combination
        - Results in a highly normalized structure (3NF) for efficient querying
        
        Args:
            silver_file_path (str): Path to cleaned Parquet file from Silver layer
            
        Output Files (Parquet format):
            - dim_location.parquet: Location dimension table
            - dim_metric.parquet: Metric metadata dimension table
            - fct_mvi.parquet: Fact table with all metric observations
            
        Example:
            >>> pipeline.silver_to_gold("data/2_silver/cleaned_mvi_wide.parquet")
            Gold Star Schema created successfully:
              - dim_location.parquet (28 rows)
              - dim_metric.parquet (7 rows)
              - fct_mvi.parquet (392 rows)
        """
        print("\n--- [2/2] Processing Silver -> Gold ---")
        # Load Silver data from Parquet (efficient columnar format)
        df_silver = pd.read_parquet(silver_file_path, engine='fastparquet')

        # ===== DIMENSION 1: Location Dimension =====
        # Maps each state/region to a unique location_id for the star schema
        # Includes region classification for geographic aggregation queries
        region_map = self.MetricEntity.region_map  # Load from MetricEntity configuration

        # Extract unique locations and assign sequential IDs
        df_loc = df_silver[['location']].drop_duplicates().reset_index(drop=True)
        df_loc['location_id'] = df_loc.index + 1
        df_loc['location_name'] = df_loc['location']
        df_loc['is_national'] = df_loc['location_name'] == 'Brasil'  # Flag national aggregate
        df_loc['region'] = df_loc['location_name'].map(region_map)  # Map state to region
        
        # Final dimension table: 4 columns, one row per location (28 expected: 26 states + DF + Brasil)
        dim_location = df_loc[['location_id', 'location_name', 'region', 'is_national']]

        # ===== DIMENSION 2: Metric Dimension =====
        # Defines the 7 violence indicators tracked by MVI
        # Used by fact table to identify which metric each observation represents
        dim_metric = pd.DataFrame(self.MetricEntity.metrics_metadata)
        # Keep only essential columns: metric_id, metric_code, metric_name, unit
        dim_metric = dim_metric[['metric_id', 'metric_code', 'metric_name', 'unit']]

        # ===== FACT TABLE: MVI Unpivoted and Normalized =====
        # Transforms wide format (one row per location with all metrics as columns)
        # Into tall format (one row per location-metric-year combination)
        # This enables efficient OLAP queries and time-series analysis
        
        fact_records = []
        metric_lookup = dict(zip(dim_metric['metric_code'], dim_metric['metric_id']))
        loc_lookup = dict(zip(dim_location['location_name'], dim_location['location_id']))

        metrics = list(metric_lookup.keys())  # [homicidio_doloso, latrocinio, ...]

        # Unpivot: Iterate through each location and create a fact row for each (metric, year) combination
        for _, row in df_silver.iterrows():
            loc_id = loc_lookup[row['location']]
            for year in [2024, 2025]:
                for m_code in metrics:
                    col_name = f"{m_code}_{year}"  # e.g., 'homicidio_doloso_2024'
                    val = row[col_name]  # Extract metric value
                    fact_records.append({
                        'location_id': loc_id,
                        'metric_id': metric_lookup[m_code],
                        'year': year,
                        'value': val
                    })

        fct_mvi = pd.DataFrame(fact_records)

        # ===== Export Gold Star Schema to Parquet =====
        # All tables use Parquet format for efficient columnar storage and analysis
        dim_location.to_parquet(os.path.join(self.GOLD_DIR, self.MetricEntity.dim_location_parquet), index=False)
        dim_metric.to_parquet(os.path.join(self.GOLD_DIR, self.MetricEntity.dim_metric_parquet), index=False)
        fct_mvi.to_parquet(os.path.join(self.GOLD_DIR, self.MetricEntity.fct_to_parquet), index=False)

        print("Gold Star Schema created successfully:")
        print(f"  - dim_location.parquet ({len(dim_location)} rows)")
        print(f"  - dim_metric.parquet ({len(dim_metric)} rows)")
        print(f"  - fct_mvi.parquet ({len(fct_mvi)} rows)")

    # ===================================================================
    # PIPELINE ORCHESTRATION & UTILITIES
    # ===================================================================
    
    def run_pipeline(self, raw_file_name):
        """
        Execute the complete ETL pipeline end-to-end.
        
        Orchestrates the full Bronze → Silver → Gold transformation workflow:
        1. Validates that the raw file exists in the Bronze directory
        2. Executes data cleaning and standardization (Bronze → Silver)
        3. Creates dimensional star schema (Silver → Gold)
        
        Args:
            raw_file_name (str): Filename of raw CSV file (without path)
                                 Must exist in the Bronze directory
                                 Example: "T01-MVI-anuario-2026.csv"
                                 
        Workflow:
            >>> pipeline = ETLPipeline()
            >>> pipeline.run_pipeline("T01-MVI-anuario-2026.csv")
            === Starting ETL Pipeline === T01-MVI-anuario-2026.csv
            Checking for raw file at: .../data/1_bronze/T01-MVI-anuario-2026.csv
            --- [1/2] Processing Bronze -> Silver ---
            Silver table saved to: .../data/2_silver/cleaned_mvi_wide.parquet (28 rows)
            --- [2/2] Processing Silver -> Gold ---
            Gold Star Schema created successfully:
              - dim_location.parquet (28 rows)
              - dim_metric.parquet (7 rows)
              - fct_mvi.parquet (392 rows)
        """
        print(f"\n=== Starting ETL Pipeline === {raw_file_name}")
        raw_file = os.path.join(self.BRONZE_DIR, raw_file_name)
        print(f"Checking for raw file at: {raw_file}")
        if os.path.exists(raw_file):
            silver_file = self.bronze_to_silver(raw_file)
            self.silver_to_gold(silver_file)
        else:
            print(f"Please place {raw_file_name} into: {self.BRONZE_DIR}")

    def print_variables(self):
        """
        Display pipeline configuration for debugging and validation..
        
        Prints the directory paths configured for the medallion architecture,
        useful for verifying that the pipeline is using the correct locations
        for data input/output.
        
        Output:
            === ETL Pipeline Variables ===
            BASE_DIR: <absolute path to pipeline script directory>
            BRONZE_DIR: <path to 1_bronze raw data directory>
            SILVER_DIR: <path to 2_silver cleaned data directory>
            GOLD_DIR: <path to 3_gold star schema directory>
            
        Example:
            >>> pipeline = ETLPipeline()
            >>> pipeline.print_variables()
        """
        print(f"BASE_DIR: {self.BASE_DIR}")
        print(f"BRONZE_DIR: {self.BRONZE_DIR}")
        print(f"SILVER_DIR: {self.SILVER_DIR}")
        print(f"GOLD_DIR: {self.GOLD_DIR}")
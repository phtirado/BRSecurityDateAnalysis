"""
Metric Entity Interface Module

This module defines the abstract base class for metric entities used in the ETL pipeline.
All concrete metric implementations (e.g., MVI - Mortality, Violence and Injury Index) 
must inherit from IMetricEntity and implement all abstract properties and methods.

The interface ensures consistent data handling across different metrics by enforcing:
- Output format specifications (Parquet and CSV)
- Data structure standards (columns and regions mapping)
- Metadata tracking for data lineage
- Data extraction logic for raw input processing
"""

from typing import Dict, List


class MetricEntity():
    """
    Abstract base class defining the interface for metric entities.
    
    This class establishes a contract that all metric implementations must follow.
    It ensures consistent handling of metric data through the ETL pipeline stages:
    - Bronze (raw): Input data extraction
    - Silver (cleaned): Data transformation and standardization
    - Gold (analytics): Dimensional modeling for analytics
    
    Attributes:
        All attributes are defined as abstract properties that subclasses must implement.
    """
    @property
    def silver_parquet_output(self) -> str:
        """
        Output filename for the cleaned metric data in Parquet format (Silver layer).
        
        Returns:
            str: Filename (e.g., 'cleaned_mvi_wide.parquet') for the standardized,
                 wide-format metric data. Parquet format is used for efficient
                 storage and columnar analysis.
        """
        pass

    @property
    def silver_csv_output(self) -> str:
        """
        Output filename for the cleaned metric data in CSV format (Silver layer).
        
        Returns:
            str: Filename (e.g., 'cleaned_mvi_wide.csv') for the standardized,
                 wide-format metric data. CSV format provides human-readable
                 output for accessibility and data verification.
        """
        pass

    @property
    def dim_location_parquet(self) -> str:
        pass

    @property
    def dim_metric_parquet(self) -> str:
        pass

    @property
    def fct_to_parquet(self) -> str:
        pass

    @property
    def columns_header(self) -> List[str]:
        """
        Standard column headers for wide-format output.
        
        Returns:
            List[str]: Ordered list of column names in wide format.
                      Typically includes location identifiers, temporal dimensions,
                      and metric-specific value columns. This standardized structure
                      ensures consistency across the data warehouse.
                      
        Example:
            ['location_id', 'year', 'metric_value', 'confidence_interval', ...]
        """
        pass

    @property
    def region_map(self) -> Dict[str, str]:
        """
        Mapping from Brazilian states/locations to macro-regions.
        
        Returns:
            Dict[str, str]: Dictionary mapping state codes or names to their
                           corresponding geographic macro-regions (e.g., Northeast,
                           Southeast, South, North, Center-West). Used for
                           geographic aggregation and analysis.
                           
        Example:
            {'SP': 'Southeast', 'RJ': 'Southeast', 'BA': 'Northeast', ...}
        """
        pass

    @property
    def metrics_metadata(self) -> List[Dict[str, str | int]]:
        """
        Metadata descriptions for all tracked metrics.
        
        Returns:
            List[Dict[str, str | int]]: List of dictionaries containing metadata
                                         for each metric, including:
                                         - name: Metric identifier
                                         - description: Human-readable description
                                         - year: Year of data collection
                                         - source: Data source reference
                                         - version: Data version/release number
                                         
        Example:
            [
                {
                    'name': 'homicide_rate',
                    'description': 'Annual homicide rate per 100k',
                    'year': 2024,
                    'source': 'Brazilian Police Database',
                    'version': 1
                },
                ...
            ]
        """
        pass

    def get_data_block(self, lignes: List[str]) -> List[str]:
        """
        Extracts the relevant data block from raw input.
        
        This method handles metric-specific parsing logic to isolate the actual
        data content from the raw input file (bronze layer). Different metrics
        may have different raw file formats, so each implementation defines
        its own extraction logic.
        
        Args:
            lignes (List[str]): List of lines from the raw input file, typically
                               read from a CSV or text file in the Bronze layer.
                               
        Returns:
            List[str]: Filtered list containing only the relevant data lines,
                      with headers, footers, and any non-data content removed.
                      This output is ready for parsing and transformation.
                      
        Note:
            Implementations should handle:
            - Skipping file headers/metadata
            - Removing footer information
            - Filtering out blank lines or comments
            - Normalizing line formatting for downstream processing
        """
        pass
"""
MVI Entity Module

MVI (Mortality, Violence and Injury Index) is a comprehensive metric that tracks
violence-related deaths and injuries across Brazil at state and national levels.

This module provides a concrete implementation of the IMetricEntity interface,
specializing in processing and managing MVI data through the ETL pipeline.
"""

from Entities.IMetricEntity import IMetricEntity
from typing import Dict, List


class MVI(IMetricEntity):
    """
    Concrete implementation of the Mortality, Violence and Injury (MVI) metric.
    
    The MVI metric aggregates five key violence indicators:
    1. Homicídio Doloso (Intentional Homicide)
    2. Latrocínio (Robbery Homicide)
    3. Lesão Corporal Seguida de Morte (Bodily Injury Followed by Death)
    4. Policiais Vítimas de CVLI (Police Officers Victims)
    5. Morte por Intervenção Policial (Police Intervention Deaths)
    
    For each indicator, the class tracks:
    - Absolute counts for 2024 and 2025
    - Aggregate MVI values (absolute counts and rates per 100k inhabitants)
    - Percentage variation between years
    
    The data is organized geographically across Brazil's 27 units (26 states + DF)
    and grouped into five macro-regions: Norte, Nordeste, Centro-Oeste, Sudeste, Sul,
    plus a Nacional (National) aggregate.
    
    Attributes:
        _silver_parquet_output (str): Output filename for Parquet format (Silver layer)
        _silver_csv_output (str): Output filename for CSV format (Silver layer)
        _columns_header (List[str]): Ordered column names for wide-format data
        _region_map (Dict[str, str]): State-to-region geographical mapping
        _metrics_metadata (List[Dict]): Detailed metadata for each tracked metric
    """

    def __init__(self):
        """
        Initialize the MVI metric entity with standard configurations.
        
        Sets up output filenames, column structure, regional mapping, and
        metric metadata for the entire ETL pipeline.
        """
        self._silver_parquet_output = "cleaned_mvi_wide.parquet"
        self._silver_csv_output = "cleaned_mvi_wide.csv"

        # Define column structure for wide-format output (Silver layer)
        # Columns include location identifier and metrics for 2024-2025
        self._columns_header = [
            'location_raw',  # State or regional location name
            'homicidio_doloso_2024', 'homicidio_doloso_2025',  # Intentional homicides
            'latrocinio_2024', 'latrocinio_2025',  # Robbery homicides
            'lesao_corporal_morte_2024', 'lesao_corporal_morte_2025',  # Bodily injury deaths
            'policiais_vitimas_2024', 'policiais_vitimas_2025',  # Police officer deaths
            'intervencao_policial_2024', 'intervencao_policial_2025',  # Police intervention deaths
            'mvi_absoluto_2024', 'mvi_absoluto_2025',  # Total MVI (absolute counts)
            'mvi_taxa_2024', 'mvi_taxa_2025',  # MVI rates per 100k inhabitants
            'mvi_variacao_pct'  # Year-over-year percentage change
        ]    

        # Geographic mapping: Brazilian states and national level to macro-regions
        # Used for spatial aggregation and regional analysis
        self._region_map = {
            # Norte region (7 states)
            'Acre': 'Norte', 'Amapá': 'Norte', 'Amazonas': 'Norte', 'Pará': 'Norte', 
            'Rondônia': 'Norte', 'Roraima': 'Norte', 'Tocantins': 'Norte',
            # Nordeste region (9 states)
            'Alagoas': 'Nordeste', 'Bahia': 'Nordeste', 'Ceará': 'Nordeste', 
            'Maranhão': 'Nordeste', 'Paraíba': 'Nordeste', 'Pernambuco': 'Nordeste', 
            'Piauí': 'Nordeste', 'Rio Grande do Norte': 'Nordeste', 'Sergipe': 'Nordeste',
            # Centro-Oeste region (4 states + DF)
            'Distrito Federal': 'Centro-Oeste', 'Goiás': 'Centro-Oeste', 
            'Mato Grosso': 'Centro-Oeste', 'Mato Grosso do Sul': 'Centro-Oeste',
            # Sudeste region (4 states)
            'Espírito Santo': 'Sudeste', 'Minas Gerais': 'Sudeste', 
            'Rio de Janeiro': 'Sudeste', 'São Paulo': 'Sudeste',
            # Sul region (3 states)
            'Paraná': 'Sul', 'Rio Grande do Sul': 'Sul', 'Santa Catarina': 'Sul',
            # National aggregate
            'Brasil': 'Nacional'
        }

        # Metadata for each tracked metric (for data warehouse dimensionality)
        self._metrics_metadata = [
            {
                'metric_id': 1,
                'metric_code': 'homicidio_doloso',
                'metric_name': 'Homicídio Doloso',
                'description': 'Intentional homicide/murder',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 2,
                'metric_code': 'latrocinio',
                'metric_name': 'Latrocínio',
                'description': 'Robbery-related homicide',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 3,
                'metric_code': 'lesao_corporal_morte',
                'metric_name': 'Lesão Corporal Seguida de Morte',
                'description': 'Bodily injury that results in death',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 4,
                'metric_code': 'policiais_vitimas',
                'metric_name': 'Policiais Vítimas de CVLI',
                'description': 'Police officers killed by violent crime (CVLI)',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 5,
                'metric_code': 'intervencao_policial',
                'metric_name': 'Morte por Intervenção Policial',
                'description': 'Deaths from police intervention (excessive use of force)',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 6,
                'metric_code': 'mvi_absoluto',
                'metric_name': 'Total MVI',
                'description': 'Aggregate of all five violence indicators',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 7,
                'metric_code': 'mvi_taxa',
                'metric_name': 'Taxa MVI por 100k hab.',
                'description': 'MVI rate standardized per 100,000 inhabitants (for comparability)',
                'unit': 'Taxa per 100k'
            },
        ]

    @property
    def silver_parquet_output(self) -> str:
        """
        Returns the filename for MVI data in Parquet format (Silver layer).
        
        Returns:
            str: 'cleaned_mvi_wide.parquet'
        """
        return self._silver_parquet_output

    @property
    def silver_csv_output(self) -> str:
        """
        Returns the filename for MVI data in CSV format (Silver layer).
        
        Returns:
            str: 'cleaned_mvi_wide.csv'
        """
        return self._silver_csv_output

    @property
    def columns_header(self) -> List[str]:
        """
        Returns the standardized column names for MVI wide-format output.
        
        The structure includes:
        - Location identifier
        - Five violence indicators (2024 and 2025 values)
        - Aggregate MVI values (absolute and rate-based)
        - Year-over-year percentage variation
        
        Returns:
            List[str]: Ordered list of 16 column headers
        """
        return self._columns_header

    @property
    def region_map(self) -> Dict[str, str]:
        """
        Returns the geographical mapping from Brazilian states to macro-regions.
        
        Supports 27 entries: 26 states + Distrito Federal, plus 'Brasil' for national aggregate.
        Maps to 5 macro-regions: Norte, Nordeste, Centro-Oeste, Sudeste, Sul, and Nacional.
        
        Returns:
            Dict[str, str]: State name → Region name mapping
        """
        return self._region_map

    @property
    def metrics_metadata(self) -> List[Dict[str, str | int]]:
        """
        Returns metadata for all seven tracked MVI metrics.
        
        Each entry contains:
        - metric_id: Unique identifier (1-7)
        - metric_code: Machine-readable identifier for data processing
        - metric_name: Portuguese human-readable name
        - description: English description of what the metric measures
        - unit: Measurement unit (Absoluto or Taxa per 100k)
        
        Returns:
            List[Dict[str, str | int]]: List of 7 metric metadata dictionaries
        """
        return self._metrics_metadata

    def get_data_block(self, lignes: List[str]) -> List[str]:
        """
        Extracts the MVI data block from raw input and parses it into structured rows.
        
        The raw MVI file (Bronze layer) contains:
        - Header metadata (lines 0-9)
        - Data rows with semicolon-separated values (lines 10-38)
        - Possible footer content (lines 39+)
        
        This method:
        1. Skips header lines (0-9)
        2. Extracts data rows (10-38), which correspond to 29 locations:
           - 26 states + Distrito Federal
           - 1 national aggregate (Brasil)
           - 1 blank line buffer
        3. Parses each row by splitting on semicolon delimiter
        4. Trims whitespace from each field
        5. Filters out empty rows
        6. Retains only the first 16 columns (matching columns_header)
        
        Args:
            lignes (List[str]): Raw lines from the input CSV file (Bronze layer)
            
        Returns:
            List[str]: List of parsed data rows, each containing up to 16 fields
                      corresponding to the column structure:
                      [location, metric1_2024, metric1_2025, ..., mvi_variacao_pct]
                      
        Note:
            Empty rows and rows with blank location names are automatically filtered out.
            This ensures the output data is clean and ready for the transformation stage.
        """
        # Data block spans from line 10 to 38 (29 rows total)
        data_rows = []
        for line in lignes[10:39]:
            # Split by semicolon delimiter and strip whitespace from each field
            parts = [p.strip() for p in line.strip().split(';')]
            # Filter out empty rows (location field is empty)
            if len(parts) > 0 and parts[0] != '':
                # Keep only the first 16 columns (matching the wide-format structure)
                data_rows.append(parts[:16])

        return data_rows
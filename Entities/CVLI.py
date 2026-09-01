"""
CVLI Entity Module

CVLI (Crimes Violentos Letais Intencionais) is the metric family described in
T06-CVLI-anuario-2026.csv. The raw file contains fatal victim counts for civil and
military police officers in service and outside service, as well as the aggregate
CVLI total and rate.

This module follows the same interface contract as the MVI entity and provides
standardized extraction, column naming, and metadata for the ETL pipeline.
"""

from typing import Dict, List

from Entities.IMetricEntity import IMetricEntity


class CVLI(IMetricEntity):
    """
    Concrete implementation for the CVLI metric entity.

    The CVLI dataset tracks deaths of police officers linked to violent crime,
    broken down into:
    1. Civil police officers killed in service
    2. Military police officers killed in service
    3. Civil police officers killed outside service
    4. Military police officers killed outside service
    5. Total CVLI victims
    6. Rate per 100k inhabitants
    """

    def __init__(self):
        self._silver_parquet_output = "cleaned_cvli_wide.parquet"
        self._silver_csv_output = "cleaned_cvli_wide.csv"
        self._dim_location_parquet = "dim_cvli_location.parquet"
        self._dim_metric_parquet = "dim_cvli_metric.parquet"
        self._fct_to_parquet = "fct_cvli.parquet"

        self._columns_header = [
            'location_raw',
            'policiais_civis_servico_2024', 'policiais_civis_servico_2025',
            'policiais_militares_servico_2024', 'policiais_militares_servico_2025',
            'policiais_civis_fora_servico_2024', 'policiais_civis_fora_servico_2025',
            'policiais_militares_fora_servico_2024', 'policiais_militares_fora_servico_2025',
            'total_cvli_2024', 'total_cvli_2025',
            'taxa_cvli_2024', 'taxa_cvli_2025',
            'variacao_pct'
        ]

        self._region_map = {
            'Acre': 'Norte', 'Amapá': 'Norte', 'Amazonas': 'Norte', 'Pará': 'Norte',
            'Rondônia': 'Norte', 'Roraima': 'Norte', 'Tocantins': 'Norte',
            'Alagoas': 'Nordeste', 'Bahia': 'Nordeste', 'Ceará': 'Nordeste',
            'Maranhão': 'Nordeste', 'Paraíba': 'Nordeste', 'Pernambuco': 'Nordeste',
            'Piauí': 'Nordeste', 'Rio Grande do Norte': 'Nordeste', 'Sergipe': 'Nordeste',
            'Distrito Federal': 'Centro-Oeste', 'Goiás': 'Centro-Oeste',
            'Mato Grosso': 'Centro-Oeste', 'Mato Grosso do Sul': 'Centro-Oeste',
            'Espírito Santo': 'Sudeste', 'Minas Gerais': 'Sudeste',
            'Rio de Janeiro': 'Sudeste', 'São Paulo': 'Sudeste',
            'Paraná': 'Sul', 'Rio Grande do Sul': 'Sul', 'Santa Catarina': 'Sul',
            'Brasil': 'Nacional'
        }

        self._metrics_metadata = [
            {
                'metric_id': 1,
                'metric_code': 'policiais_civis_servico',
                'metric_name': 'Policiais Civis mortos em confronto em serviço',
                'description': 'Civil police officers killed in service due to CVLI',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 2,
                'metric_code': 'policiais_militares_servico',
                'metric_name': 'Policiais Militares mortos em confronto em serviço',
                'description': 'Military police officers killed in service due to CVLI',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 3,
                'metric_code': 'policiais_civis_fora_servico',
                'metric_name': 'Policiais Civis mortos fora de serviço',
                'description': 'Civil police officers killed outside service due to CVLI',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 4,
                'metric_code': 'policiais_militares_fora_servico',
                'metric_name': 'Policiais Militares mortos fora de serviço',
                'description': 'Military police officers killed outside service due to CVLI',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 5,
                'metric_code': 'total_cvli',
                'metric_name': 'Total de vítimas de CVLI',
                'description': 'Total number of police officers killed by violent crime',
                'unit': 'Absoluto'
            },
            {
                'metric_id': 6,
                'metric_code': 'taxa_cvli',
                'metric_name': 'Taxa de CVLI por 100 mil hab.',
                'description': 'CVLI rate per 100,000 inhabitants',
                'unit': 'Taxa per 100k'
            },
        ]

    @property
    def silver_parquet_output(self) -> str:
        return self._silver_parquet_output

    @property
    def silver_csv_output(self) -> str:
        return self._silver_csv_output

    @property
    def dim_location_parquet(self) -> str:
        return self._dim_location_parquet

    @property
    def dim_metric_parquet(self) -> str:
        return self._dim_metric_parquet

    @property
    def fct_to_parquet(self) -> str:
        return self._fct_to_parquet

    @property
    def columns_header(self) -> List[str]:
        return self._columns_header

    @property
    def region_map(self) -> Dict[str, str]:
        return self._region_map

    @property
    def metrics_metadata(self) -> List[Dict[str, str | int]]:
        return self._metrics_metadata

    def get_data_block(self, lignes: List[str]) -> List[str]:
        """
        Extract the relevant data block from the raw CVLI CSV file.

        The bronze file contains descriptive header rows and a table with a
        semicolon-delimited structure. This method keeps only actual data rows that
        contain a location name and numeric metric values, then truncates them to the
        14 columns used in the standardized output schema.
        """
        data_rows = []
        for line in lignes[8:38]:
            # Split by semicolon delimiter and strip whitespace from each field
            parts = [p.strip() for p in line.strip().split(';')]
            # Filter out empty rows (location field is empty)
            if len(parts) > 0 and parts[0] != '':
                # Keep only the first 16 columns (matching the wide-format structure)
                data_rows.append(parts[:14])

        return data_rows

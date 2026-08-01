import pandas as pd
import etl_pipeline2 as etl


def test_silver_to_gold_reads_parquet_input(tmp_path, monkeypatch):
    df_silver = pd.DataFrame([
        {
            'location': 'Brasil',
            'homicidio_doloso_2024': 1,
            'homicidio_doloso_2025': 2,
            'latrocinio_2024': 3,
            'latrocinio_2025': 4,
            'lesao_corporal_morte_2024': 5,
            'lesao_corporal_morte_2025': 6,
            'policiais_vitimas_2024': 7,
            'policiais_vitimas_2025': 8,
            'intervencao_policial_2024': 9,
            'intervencao_policial_2025': 10,
            'mvi_absoluto_2024': 11,
            'mvi_absoluto_2025': 12,
            'mvi_taxa_2024': 13,
            'mvi_taxa_2025': 14,
            'mvi_variacao_pct': 15,
        }
    ])

    silver_path = tmp_path / 'silver.parquet'
    df_silver.to_parquet(silver_path, index=False)

    monkeypatch.setattr(etl, 'GOLD_DIR', str(tmp_path))

    etl.silver_to_gold(str(silver_path))

    assert (tmp_path / 'dim_location.csv').exists()
    assert (tmp_path / 'dim_metric.csv').exists()
    assert (tmp_path / 'fct_mvi.csv').exists()

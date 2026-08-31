import pytest

from Entities.CVLI import CVLI


def test_cvli_entity_has_standard_metadata_and_data_block():
    cvli = CVLI()

    assert cvli.silver_parquet_output == 'cleaned_cvli_wide.parquet'
    assert cvli.silver_csv_output == 'cleaned_cvli_wide.csv'
    assert len(cvli.columns_header) == 14
    assert 'location_raw' in cvli.columns_header
    assert 'total_cvli_2024' in cvli.columns_header
    assert 'variacao_pct' in cvli.columns_header
    assert 'Brasil' in cvli.region_map
    assert any(metric['metric_code'] == 'total_cvli' for metric in cvli.metrics_metadata)

    sample_lines = [
        'TABELA 06;;;;;;;;;;;;;(Voltar ao índice)',
        'Policiais Civis e Militares vítimas de CVLI, em serviço e fora de serviço (1);;;;;;;;;;;;;',
        'Brasil e Unidades da Federação 2024-2025;;;;;;;;;;;;;',
        ';;;;;;;;;;;;;',
        'Brasil e Unidades da Federação;Policiais Civis mortos em confronto em serviço ;;Policiais Militares mortos em confronto em serviço ;;Policiais Civis mortos em confronto ou por lesão não natural fora de serviço ;;Policiais Militares mortos em confronto ou por lesão não natural fora de serviço ;;Total;;;;',
        ';Ns. Absolutos;;Ns. Absolutos;;Ns. Absolutos;;Ns. Absolutos;;Ns. Absolutos;;Taxa (2) (3);;Variação (%)',
        ';2024 (4);2025;2024 (4);2025;2024 (4);2025;2024 (4);2025;2024 (4);2025;2024 (4);2025;',
        ';;;;;;;;;;;;;',
        'Brasil;1;8;31;21;8;13;76;46;144;139;0,28;0,27;-3,5',
    ]

    data_rows = cvli.get_data_block(sample_lines)
    assert data_rows[0][0] == 'Brasil'
    assert data_rows[0][-1] == '-3,5'

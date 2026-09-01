# BR Security Data Analysis

This project builds a medallion ETL pipeline for Brazilian security data, with support for multiple metric families such as MVI and CVLI. It reads raw bronze CSV files, cleans and standardizes them in the silver layer, and creates analytics-ready star schema tables in the gold layer.

## Overview

The current implementation uses a class-based design:

- `Star_pipeline/etl_pipeline.py` contains the `ETLPipeline` orchestrator
- `Entities/` contains the metric definitions and parsing contracts
- raw input files live in `data/1_bronze/`
- cleaned silver outputs are written to `data/2_silver/`
- gold tables are generated in `data/3_gold/`

The active architecture supports:

- MVI (Mortality, Violence and Injury)
- CVLI (Crimes Violentos Letais Intencionais)
- dimension modeling with location and metric metadata
- fact table creation for metrics by location, year, and indicator

---

## Repository structure

- `Star_pipeline/etl_pipeline.py` — active ETL pipeline implementation
- `Entities/` — metric entity definitions and contracts
  - `IMetricEntity.py` — abstract interface
  - `MetricEntity.py` — shared base entity logic
  - `MVI.py` — MVI-specific schema and extraction rules
  - `CVLI.py` — CVLI-specific schema and extraction rules
  - `__init__.py` — package exports
- `data/` — source and generated datasets
  - `1_bronze/` — raw CSV files
  - `2_silver/` — cleaned parquet and CSV outputs
  - `3_gold/` — star schema output tables
- `tests/` — pytest coverage for the ETL logic
- `Pipeline_test.ipynb` — notebook used to validate the pipeline with MVI and CVLI
- `query_analytics.py` — analytics helpers and SQL-style queries
- `visualize_results.py` — plotting and visualization utilities
- `requirements.txt` — dependency list

---

## Supported data sources

The bronze layer contains files such as:

- `T01-MVI-anuario-2026.csv`
- `T06-CVLI-anuario-2026.csv`

The ETL pipeline expects the file names to be present under:

- `data/1_bronze/`

---

## Pipeline flow

The project follows the medallion pattern:

1. Bronze layer
   - reads raw CSV files
   - extracts data blocks relevant to the metric
   - removes non-data header/footer noise

2. Silver layer
   - cleans location names and numeric values
   - normalizes missing values and number formatting
   - writes a wide-format cleaned table

3. Gold layer
   - creates `dim_location`
   - creates `dim_metric`
   - creates `fct_mvi`/metric-specific fact table

---

## Current ETL usage

### Run the pipeline from Python

```python
from Star_pipeline import etl_pipeline as etl
from Entities import MVI, CVLI

mvi_entity = MVI()
pipeline_mvi = etl.ETLPipeline(mvi_entity)
pipeline_mvi.run_pipeline("T01-MVI-anuario-2026.csv")

cvli_entity = CVLI()
pipeline_cvli = etl.ETLPipeline(cvli_entity)
pipeline_cvli.run_pipeline("T06-CVLI-anuario-2026.csv")
```

### Run the notebook

Open and execute:

- [Pipeline_test.ipynb](Pipeline_test.ipynb)

---

## Project requirements

Install dependencies with:

```bash
pip install -r requirements.txt
```

Or manually:

```bash
pip install pandas numpy pyarrow fastparquet duckdb matplotlib seaborn pytest
```

---

## Virtual environment setup

Recommended for reproducible runs:

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Then install dependencies and run the notebook or ETL code.

---

## Testing

Run the project tests with:

```bash
pytest -q
```

The current test suite validates the ETL behavior using the active module structure and the parquet-based gold outputs.

---

## Notes

- This project intentionally uses the active ETL implementation in `Star_pipeline/etl_pipeline.py`.
- The legacy `etl_pipeline2.py` is not the current production path and should be ignored for active project work.
- The pipeline includes a known pyarrow/pandas compatibility workaround for repeated notebook execution in the ETL class.


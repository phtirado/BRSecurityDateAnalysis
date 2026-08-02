# BR Security Data Analysis

This repository contains a small ETL pipeline and analysis notebooks to process Brazil security statistics (anuario-2026.csv) into a star schema (bronze → silver → gold) and produce visualizations.

**Key Features**
- ETL extraction and cleaning (bronze → silver) via `CSV_File_Treatment.ipynb` and `etl_pipeline.py`.
- Normalization into a star schema (gold): `dim_location`, `dim_metric`, `fct_mvi`.
- Analysis and visualizations using DuckDB, Matplotlib and Seaborn.

**Project Structure**

- `CSV_File_Treatment.ipynb` — Jupyter notebook with ETL steps, queries and plots.
- `etl_pipeline.py` — Script wrapper for the ETL flow (bronze → silver → gold).
- `query_analytics.py` — Additional query utilities.
- `visualize_results.py` — Plotting helpers for the Gold layer outputs.
- `data/` — Data folders: `1_bronze/`, `2_silver/`, `3_gold/`.
- `tests/` — Pytest tests for the ETL pipeline.

Data: place the raw CSV at [data/1_bronze/anuario-2026.csv](data/1_bronze/anuario-2026.csv). The pipeline writes cleaned CSV/parquet to `data/2_silver/` and parquet star tables to `data/3_gold/`.

## Prerequisites

- Python 3.10+ recommended
- pip

Install the main dependencies:

```bash
pip install pandas numpy pyarrow fastparquet duckdb matplotlib seaborn pytest
```

## Usage

Run the ETL pipeline (script):

```bash
python etl_pipeline.py
```

Or open and run the notebook:

- [CSV_File_Treatment.ipynb](CSV_File_Treatment.ipynb)

After running, gold parquet files will be available in `data/3_gold/` and a cleaned silver file in `data/2_silver/`.

## Visualizations & Analysis

Use `visualize_results.py` or the plotting cells in the notebook to generate figures. The notebook includes DuckDB SQL queries against `data/3_gold/*.parquet` and plotting code with Matplotlib/Seaborn.

## Tests

Run the test suite with:

```bash
pytest -q
```

## Notes

- The notebook contains a small workaround for pyarrow extension collisions when re-running cells.

## Virtual Environment

It's recommended to use a virtual environment for reproducible installs. Example commands:

- Create a virtual environment:
```bash
python -m venv .venv
```

- Activate the environment:
	- Windows (PowerShell):
	```powershell
	.venv\Scripts\Activate.ps1
	```
	- Windows (cmd):
	```cmd
	.venv\Scripts\activate.bat
	```
	- macOS / Linux:
	```bash
	source .venv/bin/activate
	```

- Install dependencies:
```bash
pip install -r requirements.txt
```

- Run the ETL script or notebook:
```bash
python etl_pipeline.py
# or
jupyter lab
```

- Run tests:
```bash
pytest -q
```

- Deactivate when finished:
```bash
deactivate
```


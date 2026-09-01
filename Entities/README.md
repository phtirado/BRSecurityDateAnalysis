# Entities package

This package defines the metric entities used by the ETL pipeline to describe each dataset and how it should be extracted, standardized, and mapped into the gold schema.

## Purpose

Each entity in this folder represents a single metric family from the bronze data source. The classes define:

- output file names for the silver and gold layers
- column structure for the cleaned data frame
- geographic region mapping for each Brazilian state or aggregate
- metric metadata used to build the dimensional model
- raw-file extraction logic for the bronze layer

---

## `IMetricEntity`

File: `IMetricEntity.py`

This is the formal abstract interface for all metric entities. It inherits from `ABC` and declares the required contract for implementing a new dataset.

### Responsibilities

- requires a silver Parquet output name
- requires a silver CSV output name
- requires dimension and fact table output names
- requires a standard column schema
- requires a state/region mapping
- requires metric metadata definitions
- requires a `get_data_block()` extraction method

### Why it matters

It guarantees that every metric family exposes the same ETL contract, which makes the pipeline generic and easier to extend.

---

## `MetricEntity`

File: `MetricEntity.py`

This class provides a shared base implementation for entity metadata and contract definitions. It is a lightweight reusable definition, not a strict abstract base class in the Python sense.

### Responsibilities

- acts as the common metric definition layer
- exposes the same property names expected by the ETL pipeline
- centralizes the structure of metadata and output names
- supports concrete classes such as `MVI` and `CVLI`

### Notes

This class is conceptually close to an abstract base but is not implemented as a formal `ABC`. Concrete subclasses still define the actual values and behavior.

---

## `MVI`

File: `MVI.py`

This entity models the Mortality, Violence and Injury dataset.

### What it defines

- `cleaned_mvi_wide.parquet`
- `cleaned_mvi_wide.csv`
- `dim_mvi_location.parquet`
- `dim_mvi_metric.parquet`
- `fct_mvi.parquet`

### Column structure

It standardizes rows like:

- `location_raw`
- `homicidio_doloso_2024`, `homicidio_doloso_2025`
- `latrocinio_2024`, `latrocinio_2025`
- `lesao_corporal_morte_2024`, `lesao_corporal_morte_2025`
- `policiais_vitimas_2024`, `policiais_vitimas_2025`
- `intervencao_policial_2024`, `intervencao_policial_2025`
- `mvi_absoluto_2024`, `mvi_absoluto_2025`
- `mvi_taxa_2024`, `mvi_taxa_2025`
- `mvi_variacao_pct`

### Region mapping

It maps each Brazilian state and the national aggregate to a macro-region such as:

- Norte
- Nordeste
- Centro-Oeste
- Sudeste
- Sul
- Nacional

### Metric metadata

It defines the seven MVI metrics used in the dim_metric table, including:

- homicide rate
- robbery homicide
- lethal bodily injury
- police victim deaths
- police intervention deaths
- absolute MVI total
- MVI rate per 100k

### Extraction method

`get_data_block()` reads the relevant section of the raw bronze CSV and keeps only the actual data rows for the MVI table.

---

## `CVLI`

File: `CVLI.py`

This entity models the Violent Lethal Intentional Crime dataset.

### What it defines

- `cleaned_cvli_wide.parquet`
- `cleaned_cvli_wide.csv`
- `dim_cvli_location.parquet`
- `dim_cvli_metric.parquet`
- `fct_cvli.parquet`

### Column structure

It standardizes rows like:

- `location_raw`
- `policiais_civis_servico_2024`, `policiais_civis_servico_2025`
- `policiais_militares_servico_2024`, `policiais_militares_servico_2025`
- `policiais_civis_fora_servico_2024`, `policiais_civis_fora_servico_2025`
- `policiais_militares_fora_servico_2024`, `policiais_militares_fora_servico_2025`
- `total_cvli_2024`, `total_cvli_2025`
- `taxa_cvli_2024`, `taxa_cvli_2025`
- `variacao_pct`

### Region mapping

It mirrors the same geographic region structure used by the MVI entity.

### Metric metadata

It defines the CVLI metrics such as:

- civil police officers killed in service
- military police officers killed in service
- civil police officers killed outside service
- military police officers killed outside service
- total CVLI victims
- CVLI rate per 100k inhabitants

### Extraction method

`get_data_block()` keeps the relevant rows from the raw bronze CSV and discards non-data header/footer content.

---

## `__init__.py`

File: `__init__.py`

This module exposes the package API for external use.

It imports and exports:

- `MVI`
- `CVLI`
- `IMetricEntity`

This allows code like:

```python
from Entities import MVI, CVLI
```

---

## Relationship between classes

The typical flow is:

1. `IMetricEntity` defines the required behavior
2. `MetricEntity` provides a shared implementation base
3. `MVI` and `CVLI` become concrete dataset-specific implementations
4. The ETL pipeline consumes these entities to parse and transform data uniformly

This design keeps the ETL logic generic while allowing each metric family to have its own schema and parsing rules.

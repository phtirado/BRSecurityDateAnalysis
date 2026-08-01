import duckdb

con = duckdb.connect()

# Query 1: Top 5 States with highest MVI rate in 2025
top5_query = """
SELECT 
    l.location_name AS state,
    l.region,
    f.value AS mvi_rate_2025
FROM 'data/3_gold/fct_mvi.parquet' f
JOIN 'data/3_gold/dim_location.parquet' l ON f.location_id = l.location_id
JOIN 'data/3_gold/dim_metric.parquet' m ON f.metric_id = m.metric_id
WHERE m.metric_code = 'mvi_taxa' 
  AND f.year = 2025 
  AND l.is_national = FALSE
ORDER BY mvi_rate_2025 DESC
LIMIT 5;
"""

print("--- Top 5 States by MVI Rate (2025) ---")
print(con.execute(top5_query).df())

# Query 2: Regional aggregation of total MVI deaths in 2024 vs 2025
region_query = """
SELECT 
    l.region,
    f.year,
    SUM(f.value) AS total_mvi_deaths
FROM 'data/3_gold/fct_mvi.parquet' f
JOIN 'data/3_gold/dim_location.parquet' l ON f.location_id = l.location_id
JOIN 'data/3_gold/dim_metric.parquet' m ON f.metric_id = m.metric_id
WHERE m.metric_code = 'mvi_absoluto' 
  AND l.is_national = FALSE
GROUP BY l.region, f.year
ORDER BY f.year DESC, total_mvi_deaths DESC;
"""

print("\n--- Total MVI Deaths by Region (2024 vs 2025) ---")
print(con.execute(region_query).df())
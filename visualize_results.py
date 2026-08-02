import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

# Set visual style
sns.set_theme(style="whitegrid")
plt.rcParams.update({'font.size': 12})

# Connect to DuckDB
con = duckdb.connect()

# -------------------------------------------------------------------
# Query 1: Top 5 States with Highest MVI Rate in 2025
# -------------------------------------------------------------------
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
df_top5 = con.execute(top5_query).df()

# -------------------------------------------------------------------
# Query 2: Regional Aggregation of Total MVI Deaths (2024 vs 2025)
# -------------------------------------------------------------------
region_query = """
SELECT 
    l.region,
    CAST(f.year AS VARCHAR) AS year,
    SUM(f.value) AS total_mvi_deaths
FROM 'data/3_gold/fct_mvi.parquet' f
JOIN 'data/3_gold/dim_location.parquet' l ON f.location_id = l.location_id
JOIN 'data/3_gold/dim_metric.parquet' m ON f.metric_id = m.metric_id
WHERE m.metric_code = 'mvi_absoluto' 
  AND l.is_national = FALSE
GROUP BY l.region, f.year
ORDER BY total_mvi_deaths DESC;
"""
df_region = con.execute(region_query).df()

# -------------------------------------------------------------------
# Plotting Figure with 2 Subplots
# -------------------------------------------------------------------
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Plot 1: Horizontal Bar Chart for Top 5 States
sns.barplot(
    data=df_top5,
    x='mvi_rate_2025',
    y='state',
    hue='region',
    dodge=False,
    ax=axes[0],
    palette='Reds_r'
)
axes[0].set_title('Top 5 States by intentional violent deaths rate (2025)\n[per 100k inhabitants]', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Taxa per 100k Population')
axes[0].set_ylabel('State (UF)')

# Add value labels inside bars
for container in axes[0].containers:
    axes[0].bar_label(container, fmt='%.1f', padding=5)

# Plot 2: Grouped Bar Chart for Regional Comparison (2024 vs 2025)
sns.barplot(
    data=df_region,
    x='region',
    y='total_mvi_deaths',
    hue='year',
    ax=axes[1],
    palette='Blues_d'
)
axes[1].set_title('Total intentional violent deaths by Region (2024 vs 2025)', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Region')
axes[1].set_ylabel('Total Absolute Deaths')

# Add value labels on top of bars
for container in axes[1].containers:
    axes[1].bar_label(container, fmt='%.0f', padding=3)

plt.tight_layout()
plt.show()
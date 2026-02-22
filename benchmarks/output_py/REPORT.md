# Benchmark Report

- Bench root: `benchmarks/output_py`
- Date range: `2025-01-01` → `2025-12-31`

## Data Sizes
| total_records | users | posts | comments |
| --- | --- | --- | --- | --- |
| 100 | 1 | 10 | 89 |
| 1,000 | 16 | 160 | 824 |
| 10,000 | 163 | 1,630 | 8,207 |

| name | total_records | timing_ms | p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- |
| single_table_cold_load | 100 | 0.601 | - | - | - |
| full_database_cold_load | 100 | 38.868 | - | - | - |
| pk_lookup | 100 | - | 0.000188 | 0.000211 | 0.000242 |
| gsi_partition_lookup | 100 | - | 0.070368 | 0.085592 | 0.094481 |
| gsi_sorted_query | 100 | - | 0.080371 | 0.096835 | 0.105310 |
| incremental_refresh | 100 | 0.520 | - | - | - |
| single_table_cold_load | 1,000 | 5.771 | - | - | - |
| full_database_cold_load | 1,000 | 394.230 | - | - | - |
| pk_lookup | 1,000 | - | 0.000192 | 0.000211 | 0.000238 |
| gsi_partition_lookup | 1,000 | - | 0.545315 | 0.624114 | 0.675125 |
| gsi_sorted_query | 1,000 | - | 0.568779 | 0.675431 | 0.738485 |
| incremental_refresh | 1,000 | 0.729 | - | - | - |
| single_table_cold_load | 10,000 | 62.605 | - | - | - |
| full_database_cold_load | 10,000 | 3663.864 | - | - | - |
| pk_lookup | 10,000 | - | 0.000168 | 0.000186 | 0.000219 |
| gsi_partition_lookup | 10,000 | - | 5.231900 | 6.682243 | 8.834802 |
| gsi_sorted_query | 10,000 | - | 5.093813 | 5.485679 | 6.002760 |
| incremental_refresh | 10,000 | 1.738 | - | - | - |

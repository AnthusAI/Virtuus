# Benchmark Report

- Bench root: `benchmarks/output`
- Date range: `2025-01-01` → `2025-12-31`

## Data Sizes
| total_records | users | posts | comments |
| --- | --- | --- | --- | --- |
| 100 | 1 | 10 | 89 |
| 500 | 8 | 80 | 412 |
| 1,000 | 16 | 160 | 824 |
| 5,000 | 81 | 810 | 4,109 |
| 10,000 | 163 | 1,630 | 8,207 |
| 50,000 | 819 | 8,190 | 40,991 |
| 100,000 | 1,639 | 16,390 | 81,971 |

| name | total_records | timing_ms | p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- |
| single_table_cold_load | 100 | 2.884 | - | - | - |
| full_database_cold_load | 100 | 46.292 | - | - | - |
| pk_lookup | 100 | - | 0.000083 | 0.000125 | 0.000542 |
| gsi_partition_lookup | 100 | - | 0.065833 | 0.090542 | 0.105000 |
| gsi_sorted_query | 100 | - | 0.082917 | 0.102917 | 0.105334 |
| incremental_refresh | 100 | 0.505 | - | - | - |
| single_table_cold_load | 500 | 3.835 | - | - | - |
| full_database_cold_load | 500 | 200.861 | - | - | - |
| pk_lookup | 500 | - | 0.000083 | 0.000208 | 0.000709 |
| gsi_partition_lookup | 500 | - | 0.234125 | 0.316500 | 0.446041 |
| gsi_sorted_query | 500 | - | 0.239709 | 0.502500 | 0.584750 |
| incremental_refresh | 500 | 0.961 | - | - | - |
| single_table_cold_load | 1,000 | 6.312 | - | - | - |
| full_database_cold_load | 1,000 | 378.206 | - | - | - |
| pk_lookup | 1,000 | - | 0.000083 | 0.000125 | 0.000167 |
| gsi_partition_lookup | 1,000 | - | 0.351916 | 0.482542 | 0.522583 |
| gsi_sorted_query | 1,000 | - | 0.346125 | 0.416042 | 0.542833 |
| incremental_refresh | 1,000 | 0.578 | - | - | - |
| single_table_cold_load | 5,000 | 24.148 | - | - | - |
| full_database_cold_load | 5,000 | 1563.824 | - | - | - |
| pk_lookup | 5,000 | - | 0.000083 | 0.000125 | 0.000208 |
| gsi_partition_lookup | 5,000 | - | 1.637416 | 2.067750 | 2.120125 |
| gsi_sorted_query | 5,000 | - | 1.742167 | 2.164833 | 2.330875 |
| incremental_refresh | 5,000 | 1.258 | - | - | - |
| single_table_cold_load | 10,000 | 51.837 | - | - | - |
| full_database_cold_load | 10,000 | 3206.181 | - | - | - |
| pk_lookup | 10,000 | - | 0.000083 | 0.000167 | 0.000209 |
| gsi_partition_lookup | 10,000 | - | 3.795958 | 4.583584 | 4.804750 |
| gsi_sorted_query | 10,000 | - | 3.708708 | 4.511000 | 4.828292 |
| incremental_refresh | 10,000 | 1.310 | - | - | - |
| single_table_cold_load | 50,000 | 278.976 | - | - | - |
| full_database_cold_load | 50,000 | 17372.534 | - | - | - |
| pk_lookup | 50,000 | - | 0.000083 | 0.000167 | 0.000417 |
| gsi_partition_lookup | 50,000 | - | 20.318750 | 22.723750 | 23.952875 |
| gsi_sorted_query | 50,000 | - | 19.790417 | 21.927833 | 23.144959 |
| incremental_refresh | 50,000 | 4.232 | - | - | - |
| single_table_cold_load | 100,000 | 722.692 | - | - | - |
| full_database_cold_load | 100,000 | 41399.483 | - | - | - |
| pk_lookup | 100,000 | - | 0.000083 | 0.000167 | 0.000292 |
| gsi_partition_lookup | 100,000 | - | 45.895333 | 48.992166 | 59.552959 |
| gsi_sorted_query | 100,000 | - | 42.858500 | 49.144875 | 65.029792 |
| incremental_refresh | 100,000 | 8.648 | - | - | - |

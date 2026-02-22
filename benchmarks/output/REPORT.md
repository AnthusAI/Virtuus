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
| single_table_cold_load | 100 | 1.191 | - | - | - |
| full_database_cold_load | 100 | 34.240 | - | - | - |
| pk_lookup | 100 | - | 0.000195 | 0.000227 | 0.000255 |
| gsi_partition_lookup | 100 | - | 0.064624 | 0.081578 | 0.084216 |
| gsi_sorted_query | 100 | - | 0.064383 | 0.075190 | 0.084031 |
| incremental_refresh | 100 | 0.504 | - | - | - |
| single_table_cold_load | 500 | 3.525 | - | - | - |
| full_database_cold_load | 500 | 170.978 | - | - | - |
| pk_lookup | 500 | - | 0.000186 | 0.000223 | 0.000249 |
| gsi_partition_lookup | 500 | - | 0.178282 | 0.216110 | 0.238187 |
| gsi_sorted_query | 500 | - | 0.190898 | 0.228277 | 0.257521 |
| incremental_refresh | 500 | 0.566 | - | - | - |
| single_table_cold_load | 1,000 | 6.503 | - | - | - |
| full_database_cold_load | 1,000 | 333.204 | - | - | - |
| pk_lookup | 1,000 | - | 0.000188 | 0.000224 | 0.000245 |
| gsi_partition_lookup | 1,000 | - | 0.370841 | 0.430324 | 0.495911 |
| gsi_sorted_query | 1,000 | - | 0.378094 | 0.417025 | 0.432498 |
| incremental_refresh | 1,000 | 2.637 | - | - | - |
| single_table_cold_load | 5,000 | 27.703 | - | - | - |
| full_database_cold_load | 5,000 | 1641.187 | - | - | - |
| pk_lookup | 5,000 | - | 0.000179 | 0.000211 | 0.000250 |
| gsi_partition_lookup | 5,000 | - | 1.732678 | 2.132511 | 3.137871 |
| gsi_sorted_query | 5,000 | - | 1.690544 | 1.824119 | 1.898644 |
| incremental_refresh | 5,000 | 1.044 | - | - | - |
| single_table_cold_load | 10,000 | 68.553 | - | - | - |
| full_database_cold_load | 10,000 | 3872.506 | - | - | - |
| pk_lookup | 10,000 | - | 0.000169 | 0.000194 | 0.000221 |
| gsi_partition_lookup | 10,000 | - | 3.775260 | 4.298998 | 4.600171 |
| gsi_sorted_query | 10,000 | - | 3.596981 | 3.858287 | 3.996288 |
| incremental_refresh | 10,000 | 1.455 | - | - | - |
| single_table_cold_load | 50,000 | 401.025 | - | - | - |
| full_database_cold_load | 50,000 | 17834.794 | - | - | - |
| pk_lookup | 50,000 | - | 0.000165 | 0.000193 | 0.000222 |
| gsi_partition_lookup | 50,000 | - | 20.078980 | 21.731253 | 23.121137 |
| gsi_sorted_query | 50,000 | - | 19.632283 | 23.147729 | 25.764612 |
| incremental_refresh | 50,000 | 4.731 | - | - | - |
| single_table_cold_load | 100,000 | 665.224 | - | - | - |
| full_database_cold_load | 100,000 | 43004.822 | - | - | - |
| pk_lookup | 100,000 | - | 0.000166 | 0.000176 | 0.000206 |
| gsi_partition_lookup | 100,000 | - | 41.124985 | 44.042472 | 45.675532 |
| gsi_sorted_query | 100,000 | - | 43.047902 | 45.497652 | 48.485750 |
| incremental_refresh | 100,000 | 9.023 | - | - | - |

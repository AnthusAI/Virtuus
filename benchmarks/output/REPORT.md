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
| single_table_cold_load | 100 | 2.357 | - | - | - |
| full_database_cold_load | 100 | 1.828 | - | - | - |
| pk_lookup | 100 | - | 0.000563 | 0.000606 | 0.000770 |
| gsi_partition_lookup | 100 | - | 0.054919 | 0.076213 | 0.084526 |
| gsi_sorted_query | 100 | - | 0.065877 | 0.082998 | 0.090517 |
| incremental_refresh | 100 | 0.364 | - | - | - |
| single_table_cold_load | 500 | 0.316 | - | - | - |
| full_database_cold_load | 500 | 6.536 | - | - | - |
| pk_lookup | 500 | - | 0.000594 | 0.000636 | 0.000728 |
| gsi_partition_lookup | 500 | - | 0.153195 | 0.182070 | 0.198878 |
| gsi_sorted_query | 500 | - | 0.156217 | 0.189540 | 0.191554 |
| incremental_refresh | 500 | 0.585 | - | - | - |
| single_table_cold_load | 1,000 | 0.419 | - | - | - |
| full_database_cold_load | 1,000 | 14.257 | - | - | - |
| pk_lookup | 1,000 | - | 0.000587 | 0.000626 | 0.000710 |
| gsi_partition_lookup | 1,000 | - | 0.260968 | 0.301298 | 0.327123 |
| gsi_sorted_query | 1,000 | - | 0.284173 | 0.356810 | 0.394098 |
| incremental_refresh | 1,000 | 1.288 | - | - | - |
| single_table_cold_load | 5,000 | 1.223 | - | - | - |
| full_database_cold_load | 5,000 | 179.197 | - | - | - |
| pk_lookup | 5,000 | - | 0.000595 | 0.000711 | 0.001036 |
| gsi_partition_lookup | 5,000 | - | 1.175726 | 1.430307 | 1.636407 |
| gsi_sorted_query | 5,000 | - | 1.144798 | 1.334052 | 1.467917 |
| incremental_refresh | 5,000 | 0.862 | - | - | - |
| single_table_cold_load | 10,000 | 2.196 | - | - | - |
| full_database_cold_load | 10,000 | 430.054 | - | - | - |
| pk_lookup | 10,000 | - | 0.000595 | 0.000756 | 0.000955 |
| gsi_partition_lookup | 10,000 | - | 2.715418 | 3.150968 | 3.213314 |
| gsi_sorted_query | 10,000 | - | 2.668781 | 3.041115 | 3.203654 |
| incremental_refresh | 10,000 | 1.373 | - | - | - |
| single_table_cold_load | 50,000 | 18.752 | - | - | - |
| full_database_cold_load | 50,000 | 2161.276 | - | - | - |
| pk_lookup | 50,000 | - | 0.000575 | 0.000652 | 0.000765 |
| gsi_partition_lookup | 50,000 | - | 16.125766 | 17.763582 | 18.403920 |
| gsi_sorted_query | 50,000 | - | 15.282085 | 17.004725 | 18.812604 |
| incremental_refresh | 50,000 | 3.807 | - | - | - |
| single_table_cold_load | 100,000 | 65.175 | - | - | - |
| full_database_cold_load | 100,000 | 4756.610 | - | - | - |
| pk_lookup | 100,000 | - | 0.000568 | 0.000655 | 0.000851 |
| gsi_partition_lookup | 100,000 | - | 34.416593 | 36.072348 | 39.396325 |
| gsi_sorted_query | 100,000 | - | 32.710471 | 34.462162 | 37.072017 |
| incremental_refresh | 100,000 | 5.894 | - | - | - |

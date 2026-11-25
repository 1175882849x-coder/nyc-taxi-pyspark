## Dataset and Source

This project uses the [NYC Taxi and Limousine Commission Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) from January 2025 through April 2026. The input consists of 16 monthly Parquet files totaling 1,085,531,096 bytes (1.09 GB). The analysis uses pickup timestamps, pickup zone IDs, trip distances, and base fares.

Spark read 63,631,048 records and retained 59,045,917 after filtering for positive trip distance and positive base fare.

## How to Run

This project uses Python 3.11, Java 17, and PySpark 3.5.6. Place the 16 monthly Yellow Taxi Parquet files in `~/Desktop/taxi_data/`.

From the project folder, run:

```bash
conda activate taxi-spark
python -m pip install pyspark==3.5.6
python read_taxi.py
```

The script writes the hourly summary to `~/Desktop/taxi_hourly_summary.parquet` and prints the local Spark UI URL. Press Enter in the terminal when you are finished viewing the UI.

## Performance Analysis

The physical plan shows predicate pushdown and column pruning in the Parquet scan. Spark passed the `trip_distance > 0` and `fare_amount > 0` predicates, along with their null checks, to the Parquet reader. In this run, the scan still output 63,631,048 rows across 16 files, and the row-level filter reduced that to 59,045,917 rows. The `ReadSchema` shows that the hourly aggregation requested only three columns from Parquet: `tpep_pickup_datetime`, `trip_distance`, and `fare_amount`. Applying the filters before the aggregation reduced the number of rows entering that stage.

The main work occurred before the shuffle, where Spark scanned, filtered, and partially aggregated the input. The first `HashAggregate` computed partial counts and averages within each input partition, so Spark shuffled only 312 intermediate records, about 25.7 KiB. The physical plan showed 200 initial shuffle partitions for grouping by `pickup_hour`. At runtime, Adaptive Query Execution coalesced the shuffle into one read partition. The final aggregation produced 24 hourly rows, which Spark wrote to one Parquet part file. Neither hash aggregation reported any spill.

### Five Busiest Pickup Hours

| Pickup hour | Trip count | Average base fare | Average distance (mi) |
| ---: | ---: | ---: | ---: |
| 18 | 3,961,578 | $18.92 | 5.68 |
| 17 | 3,802,070 | $20.09 | 5.90 |
| 19 | 3,583,847 | $19.11 | 5.13 |
| 21 | 3,569,516 | $19.78 | 5.48 |
| 15 | 3,529,575 | $21.43 | 5.38 |

### Five Busiest Pickup Zones

| Pickup zone ID | Trip count | Average base fare |
| ---: | ---: | ---: |
| 237 | 2,683,364 | $13.49 |
| 161 | 2,567,580 | $17.25 |
| 132 | 2,446,194 | $63.16 |
| 236 | 2,377,078 | $14.09 |
| 186 | 1,900,562 | $17.22 |

## Key Findings

- The busiest pickup hour was 6:00–6:59 p.m., with 3,961,578 trips and an average base fare of $18.92. The next busiest hour was 5:00–5:59 p.m., with 3,802,070 trips.
- Pickup zone 237 had the most trips overall, with 2,683,364 pickups and an average base fare of $13.49. During the busiest hour, zone 161 led with 225,905 pickups.
- Zone 132 ranked third by pickup count, with 2,446,194 trips. Its average base fare of $63.16 was the highest among the five busiest pickup zones.

## Execution Evidence

- [Physical execution plan](screenshots/physical-plan.png)
- [Successful pipeline execution](screenshots/pipeline-success.png)
- [Spark UI Query Details](screenshots/query-details.pdf)
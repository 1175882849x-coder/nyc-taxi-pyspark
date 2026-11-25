from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    input_path = Path.home() / "Desktop" / "taxi_data" / "yellow_tripdata_*.parquet"
    output_path = Path.home() / "Desktop" / "taxi_hourly_summary.parquet"

    spark = (
        SparkSession.builder.appName("NYC Taxi Assignment")
        .master("local[2]")
        .getOrCreate()
    )

    try:
        trips = spark.read.parquet(str(input_path))

        print("Rows before filtering:", trips.count())

        sc = spark.sparkContext
        tracker = sc.statusTracker()
        group_id = "lazy-evaluation-demo"
        sc.setJobGroup(group_id, "Compare transformations and actions")

        filtered_trips = trips.filter(F.col("trip_distance") > 0).filter(
            F.col("fare_amount") > 0
        )

        print(
            "Spark jobs after defining filters:",
            len(tracker.getJobIdsForGroup(group_id)),
        )

        print("Rows after filtering:", filtered_trips.count())
        print("Spark jobs after count():", len(tracker.getJobIdsForGroup(group_id)))

        sc.setLocalProperty("spark.jobGroup.id", None)
        sc.setLocalProperty("spark.job.description", None)

        trips_with_hour = filtered_trips.withColumn(
            "pickup_hour",
            F.hour(F.col("tpep_pickup_datetime")),
        )

        print("\nSample pickup times and hours")
        trips_with_hour.select(
            "tpep_pickup_datetime",
            "pickup_hour",
        ).show(5, truncate=False)

        hourly_summary = trips_with_hour.groupBy("pickup_hour").agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )

        print("\nPhysical execution plan for the hourly summary")
        hourly_summary.explain(mode="formatted")

        print("\nFive busiest pickup hours")
        hourly_summary.orderBy(F.desc("trip_count"), "pickup_hour").show(5)

        trips_with_hour.createOrReplaceTempView("taxi_trips")

        print("\nSQL query 1 — Five busiest pickup zones")
        busiest_zones = spark.sql("""
            SELECT
                PULocationID AS pickup_zone_id,
                COUNT(*) AS trip_count,
                ROUND(AVG(fare_amount), 2) AS avg_fare
            FROM taxi_trips
            WHERE PULocationID IS NOT NULL
            GROUP BY PULocationID
            ORDER BY trip_count DESC, pickup_zone_id
            LIMIT 5
        """)
        busiest_zones.show()

        print("\nSQL query 2 — Busiest pickup zone for each hour")
        hourly_top_zones = spark.sql("""
            WITH zone_counts AS (
                SELECT
                    pickup_hour,
                    PULocationID AS pickup_zone_id,
                    COUNT(*) AS trip_count
                FROM taxi_trips
                WHERE pickup_hour IS NOT NULL
                  AND PULocationID IS NOT NULL
                GROUP BY pickup_hour, PULocationID
            ),
            ranked AS (
                SELECT
                    pickup_hour,
                    pickup_zone_id,
                    trip_count,
                    DENSE_RANK() OVER (
                        PARTITION BY pickup_hour
                        ORDER BY trip_count DESC
                    ) AS zone_rank
                FROM zone_counts
            )
            SELECT pickup_hour, pickup_zone_id, trip_count
            FROM ranked
            WHERE zone_rank = 1
            ORDER BY pickup_hour, pickup_zone_id
        """)
        # At most five result rows per hour are shown if zones tie.
        hourly_top_zones.show(120)

        hourly_summary.write.mode("overwrite").parquet(str(output_path))
        print("\nHourly summary saved to:", output_path)
        print("Spark UI:", spark.sparkContext.uiWebUrl)
        input("Press Enter after capturing the Spark UI screenshots...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

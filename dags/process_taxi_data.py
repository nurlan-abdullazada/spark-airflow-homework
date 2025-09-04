# process_taxi_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

def create_spark_session():
    """Create Spark session with proper configuration"""
    return SparkSession.builder \
        .appName("TaxiDataProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_data(spark, data_path):
    """Load taxi data and zone lookup data"""
    print("Loading taxi trip data...")
    
    # Load main taxi data
    taxi_df = spark.read.parquet(f"{data_path}/yellow_tripdata_2024-01.parquet")
    
    # Load zone lookup data
    zones_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{data_path}/taxi_zone_lookup.csv")
    
    print(f"Loaded {taxi_df.count()} taxi records")
    print(f"Loaded {zones_df.count()} zone records")
    
    return taxi_df, zones_df

def apply_narrow_transformations(taxi_df):
    """Apply narrow transformations (filter, withColumn, map operations)"""
    print("\n=== APPLYING NARROW TRANSFORMATIONS ===")
    
    # 1. FILTER: Remove invalid records (narrow transformation)
    print("1. Filtering valid trips...")
    cleaned_df = taxi_df.filter(
        (col("trip_distance") > 0) & 
        (col("trip_distance") < 100) &  # Remove outliers
        (col("fare_amount") > 0) & 
        (col("fare_amount") < 1000) &   # Remove outliers
        (col("passenger_count") > 0) &
        (col("passenger_count") <= 6)
    )
    
    print(f"Records after filtering: {cleaned_df.count()}")
    
    # 2. WITH_COLUMN: Add calculated columns (narrow transformation)
    print("2. Adding calculated columns...")
    enhanced_df = cleaned_df \
        .withColumn("trip_duration_minutes", 
                   (unix_timestamp("tpep_dropoff_datetime") - 
                    unix_timestamp("tpep_pickup_datetime")) / 60) \
        .withColumn("fare_per_mile", 
                   round(col("fare_amount") / col("trip_distance"), 2)) \
        .withColumn("fare_per_minute", 
                   round(col("fare_amount") / col("trip_duration_minutes"), 2)) \
        .withColumn("pickup_hour", 
                   hour("tpep_pickup_datetime")) \
        .withColumn("pickup_day_of_week", 
                   dayofweek("tpep_pickup_datetime")) \
        .withColumn("is_weekend", 
                   when(col("pickup_day_of_week").isin([1, 7]), 1).otherwise(0))
    
    # 3. Additional filtering after calculations (narrow transformation)
    print("3. Filtering calculated fields...")
    final_df = enhanced_df.filter(
        (col("trip_duration_minutes") > 0) & 
        (col("trip_duration_minutes") < 180) &  # Less than 3 hours
        (col("fare_per_mile") < 50)  # Reasonable fare per mile
    )
    
    print(f"Records after narrow transformations: {final_df.count()}")
    
    # Show sample of narrow transformations
    print("\nSample of enhanced data:")
    final_df.select("trip_distance", "fare_amount", "trip_duration_minutes", 
                   "fare_per_mile", "pickup_hour", "is_weekend").show(5)
    
    return final_df

def apply_wide_transformations(enhanced_df, zones_df):
    """Apply wide transformations (groupBy, join, aggregations)"""
    print("\n=== APPLYING WIDE TRANSFORMATIONS ===")
    
    # 1. JOIN: Add pickup zone names (wide transformation)
    print("1. Joining with pickup zone data...")
    pickup_zones_df = zones_df.select(
        col("LocationID").alias("pickup_zone_id"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone")
    )
    
    joined_df = enhanced_df.join(
        pickup_zones_df, 
        enhanced_df.PULocationID == pickup_zones_df.pickup_zone_id,
        "left"
    )
    
    # 2. Another JOIN: Add dropoff zone names (wide transformation)
    print("2. Joining with dropoff zone data...")
    dropoff_zones_df = zones_df.select(
        col("LocationID").alias("dropoff_zone_id"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone")
    )
    
    fully_joined_df = joined_df.join(
        dropoff_zones_df,
        joined_df.DOLocationID == dropoff_zones_df.dropoff_zone_id,
        "left"
    )
    
    # 3. GROUP BY: Borough statistics (wide transformation)
    print("3. Creating borough-level statistics...")
    borough_stats = fully_joined_df.groupBy("pickup_borough") \
        .agg(
            count("*").alias("total_trips"),
            avg("trip_distance").alias("avg_trip_distance"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_duration_minutes").alias("avg_duration"),
            sum("fare_amount").alias("total_revenue"),
            max("fare_amount").alias("max_fare"),
            min("fare_amount").alias("min_fare")
        ) \
        .orderBy(desc("total_trips"))
    
    print("\nBorough Statistics:")
    borough_stats.show()
    
    # 4. GROUP BY: Hourly patterns (wide transformation)
    print("4. Creating hourly trip patterns...")
    hourly_stats = fully_joined_df.groupBy("pickup_hour") \
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_distance")
        ) \
        .orderBy("pickup_hour")
    
    print("\nHourly Trip Patterns:")
    hourly_stats.show(24)
    
    # 5. GROUP BY: Weekend vs Weekday analysis (wide transformation)
    print("5. Weekend vs Weekday analysis...")
    weekend_stats = fully_joined_df.groupBy("is_weekend") \
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_distance"),
            avg("trip_duration_minutes").alias("avg_duration")
        )
    
    print("\nWeekend vs Weekday Stats (0=Weekday, 1=Weekend):")
    weekend_stats.show()
    
    return fully_joined_df, borough_stats, hourly_stats, weekend_stats

def save_results(spark, processed_df, borough_stats, hourly_stats, weekend_stats, output_path):
    """Save processed results"""
    print(f"\n=== SAVING RESULTS to {output_path} ===")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # Save main processed dataset
    print("1. Saving processed trip data...")
    processed_df.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/processed_trips")
    
    # Save aggregated statistics
    print("2. Saving borough statistics...")
    borough_stats.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/borough_stats")
    
    print("3. Saving hourly statistics...")
    hourly_stats.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/hourly_stats")
    
    print("4. Saving weekend statistics...")
    weekend_stats.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/weekend_stats")
    
    print("All results saved successfully!")

def main():
    """Main processing function"""
    # Define paths
    base_path = "/opt/airflow"  # This is the path inside Docker container
    data_path = f"{base_path}/data"
    output_path = f"{base_path}/output"
    
    print("=== NYC TAXI DATA PROCESSING WITH SPARK ===")
    print(f"Data path: {data_path}")
    print(f"Output path: {output_path}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load data
        taxi_df, zones_df = load_data(spark, data_path)
        
        # Apply narrow transformations
        enhanced_df = apply_narrow_transformations(taxi_df)
        
        # Apply wide transformations
        final_df, borough_stats, hourly_stats, weekend_stats = apply_wide_transformations(enhanced_df, zones_df)
        
        # Save results
        save_results(spark, final_df, borough_stats, hourly_stats, weekend_stats, output_path)
        
        print("\n=== PROCESSING COMPLETED SUCCESSFULLY! ===")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

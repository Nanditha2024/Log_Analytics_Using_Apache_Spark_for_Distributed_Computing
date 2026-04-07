#!/usr/bin/env python3
"""
Script to trim parquet files to keep only first 100,000 records.
Usage: spark-submit trim_parquet_files.py
"""

from pyspark.sql import SparkSession
import sys

def trim_parquet_file(spark, file_path, limit=100000):
    """
    Read a parquet file, keep first 'limit' records, and write back.
    """
    try:
        print(f"Processing: {file_path}")
        
        # Read the parquet file
        df = spark.read.parquet(file_path)
        original_count = df.count()
        print(f"  Original record count: {original_count:,}")
        
        if original_count <= limit:
            print(f"  File already has {original_count:,} records (limit: {limit:,}). Skipping.")
            return
        
        # Keep only first 'limit' records
        df_trimmed = df.limit(limit)
        
        # Write back to the same location (overwrite)
        df_trimmed.write.mode("overwrite").parquet(file_path)
        
        new_count = df_trimmed.count()
        removed_count = original_count - new_count
        print(f"  New record count: {new_count:,}")
        print(f"  Records removed: {removed_count:,}")
        print(f"  ✓ Success!\n")
        
    except Exception as e:
        print(f"  ✗ Error processing {file_path}: {str(e)}\n")
        return False
    
    return True

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("TrimParquetFiles") \
        .getOrCreate()
    
    print("=" * 60)
    print("Trimming Parquet Files to 100,000 Records (1 Lakh)")
    print("=" * 60 + "\n")
    
    # Define files to process
    files_to_process = [
        "data/deployments.parquet",
        "data/host_meta.parquet",
        "data/logs.parquet"
    ]
    
    processed_count = 0
    successful_count = 0
    
    for file_path in files_to_process:
        if trim_parquet_file(spark, file_path, limit=100000):
            successful_count += 1
        processed_count += 1
    
    print("=" * 60)
    print(f"Processing Complete: {successful_count}/{processed_count} files processed successfully")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()

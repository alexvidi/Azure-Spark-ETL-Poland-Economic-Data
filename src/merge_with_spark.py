"""
merge_with_spark.py

Script to load cleaned regional data using PySpark,
perform INNER JOINs by 'voivodeship', validate the result,
and save the final dataset as a single CSV file.

Inputs (CSV, header=True) under ../data/processed/ :
    - gdp_per_capita_2023.csv         [voivodeship, gdp_per_capita_pln]
    - unemployment_rate_2023.csv      [voivodeship, unemployment_rate]
    - average_gross_wage_2023.csv     [voivodeship, average_gross_wage]
    - population_2023.csv             [voivodeship, population_total]

Output:
    - ../data/processed/regional_economic_data_2023_spark.csv

Author: Alex Vidal
Created: 2024-04-08
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os, glob, shutil

# === Initialize Spark session ===
# Creates or retrieves a SparkSession object, which is the entry point for PySpark.
spark = SparkSession.builder \
    .appName("Merge Regional Economic Data") \
    .getOrCreate()

# === Define paths ===
# Input folder for cleaned CSV files.
# Temporary folder for Spark output (Spark writes to directories).
# Final single CSV file path after renaming.
input_path = "../data/processed/"
output_dir = "../data/processed/regional_economic_data_2023_spark_tmp"
final_csv = "../data/processed/regional_economic_data_2023_spark.csv"

# === Load individual datasets as DataFrames ===
# Each CSV file is read into a Spark DataFrame with header recognition.
gdp_df = spark.read.option("header", True).csv(f"{input_path}gdp_per_capita_2023.csv")
unemployment_df = spark.read.option("header", True).csv(f"{input_path}unemployment_rate_2023.csv")
wages_df = spark.read.option("header", True).csv(f"{input_path}average_gross_wage_2023.csv")
population_df = spark.read.option("header", True).csv(f"{input_path}population_2023.csv")

# === Convert numeric columns to double ===
# By default, Spark reads CSVs as strings.
# Here, the main metric columns are explicitly casted to DoubleType for analysis.
gdp_df = gdp_df.withColumn("gdp_per_capita_pln", col("gdp_per_capita_pln").cast("double"))
unemployment_df = unemployment_df.withColumn("unemployment_rate", col("unemployment_rate").cast("double"))
wages_df = wages_df.withColumn("average_gross_wage", col("average_gross_wage").cast("double"))
population_df = population_df.withColumn("population_total", col("population_total").cast("double"))

# === Merge datasets on 'voivodeship' ===
# INNER JOIN ensures only regions present in ALL datasets are included.
# Join order: GDP → Unemployment → Wages → Population.
merged_df = gdp_df.join(unemployment_df, on="voivodeship", how="inner") \
                  .join(wages_df, on="voivodeship", how="inner") \
                  .join(population_df, on="voivodeship", how="inner")

# === Show preview of merged dataset ===
# Display first 5 rows, sorted alphabetically by region name.
# truncate=False prevents Spark from shortening long column values.
merged_df.orderBy("voivodeship").show(5, truncate=False)

# === Save merged dataset as a single CSV file ===
# Spark normally writes multiple part files; coalesce(1) reduces to a single file.
merged_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_dir)

# === Rename output file ===
# Spark writes part-*.csv inside the temporary folder.
# The part file is renamed to a stable final name, then the temp folder is deleted.
part_file = glob.glob(os.path.join(output_dir, "part-*.csv"))[0]
shutil.move(part_file, final_csv)
shutil.rmtree(output_dir)

print(f"Final dataset saved as CSV: {final_csv}")

# === Stop Spark session ===
# Releases resources and closes the Spark application.
spark.stop()




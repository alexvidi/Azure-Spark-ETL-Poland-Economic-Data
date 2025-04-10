"""
merge_with_spark.py

Script to load cleaned regional data using PySpark,
perform joins by voivodeship, validate the result,
and save the final dataset as a single CSV file.

Author: Alex Vidal
Updated: 2024-04-08
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# === Initialize Spark session ===
spark = SparkSession.builder \
    .appName("Merge Regional Economic Data") \
    .getOrCreate()

# === Define paths ===
input_path = "../data/processed/"
output_path = "../data/processed/regional_economic_data_2023_spark.csv"

# === Load CSVs as DataFrames ===
gdp_df = spark.read.option("header", True).csv(f"{input_path}gdp_per_capita_2023.csv")
unemployment_df = spark.read.option("header", True).csv(f"{input_path}unemployment_rate_2023.csv")
wages_df = spark.read.option("header", True).csv(f"{input_path}average_gross_wage_2023.csv")
population_df = spark.read.option("header", True).csv(f"{input_path}population_2023.csv")

# === Convert numeric columns ===
gdp_df = gdp_df.withColumn("gdp_per_capita_pln", col("gdp_per_capita_pln").cast("double"))
unemployment_df = unemployment_df.withColumn("unemployment_rate", col("unemployment_rate").cast("double"))
wages_df = wages_df.withColumn("average_gross_wage", col("average_gross_wage").cast("double"))
population_df = population_df.withColumn("population_total", col("population_total").cast("double"))

# === Merge datasets ===
merged_df = gdp_df.join(unemployment_df, on="voivodeship", how="inner") \
                  .join(wages_df, on="voivodeship", how="inner") \
                  .join(population_df, on="voivodeship", how="inner")

# === Show preview ===
merged_df.orderBy("voivodeship").show(5, truncate=False)

# === Save to single CSV file ===
merged_df.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(output_path.replace(".csv", ""))  # Spark creates folder

print(f" Final dataset saved as CSV to folder: {output_path.replace('.csv', '')}")

# === Stop Spark ===
spark.stop()


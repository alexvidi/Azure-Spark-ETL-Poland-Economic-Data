"""
extract_population_data.py

Script to extract and clean total population by voivodeship
from the Statistics Poland 2023 dataset.

Author: Alex Vidal
Created: 2024-04-08
"""

import pandas as pd
from pathlib import Path

# === Define project directories and file paths ===
# RAW_DATA_DIR: folder with the original Excel file from GUS
# PROCESSED_DATA_DIR: folder where the cleaned CSV will be saved
# RAW_FILE: Excel source file containing population data
# OUTPUT_FILE: name of the final cleaned dataset
RAW_DATA_DIR = Path("../data/raw")
PROCESSED_DATA_DIR = Path("../data/processed")
RAW_FILE = RAW_DATA_DIR / "population_2023.xlsx"
OUTPUT_FILE = PROCESSED_DATA_DIR / "population_2023.csv"

# === Load Excel sheet, skipping unnecessary header rows ===
# Sheet "15 (33)" contains resident population data
# skiprows=10 → jump the first 10 rows (titles, subtitles, header info, and Polska total)
# → the first row in Pandas will already be "Dolnośląskie" (row 11 in Excel)
sheet_name = "15 (33)"
skip_rows = 10  # Skip first 10 rows with titles and subtitles

print(f"Reading file: {RAW_FILE.name}, sheet: {sheet_name}, skipping first {skip_rows} rows...")
df = pd.read_excel(RAW_FILE, sheet_name=sheet_name, skiprows=skip_rows)

# === Preview loaded data ===
print("\nLoaded data preview:")
print(df.head())

# === Rename relevant columns ===
# Column A (Excel col 0) → voivodeship (region name, text)
# Column B (Excel col 1) → population_total (total population, usually in thousands)
df = df.rename(columns={
    df.columns[0]: "voivodeship",
    df.columns[1]: "population_total"
})

# === Remove non-valid rows ===
# Drop rows without a voivodeship or without a population value
# This ensures that empty rows or notes at the end are excluded
df = df.dropna(subset=['voivodeship', 'population_total'])

# === Select only relevant columns for the pipeline ===
df_clean = df[["voivodeship", "population_total"]].copy()

# === Clean DataFrame formatting ===
# Reset index to start at 0..15 (after filtering rows)
# Sort alphabetically by voivodeship for easier inspection and merging
df_clean = df_clean.reset_index(drop=True)
df_clean = df_clean.sort_values(by="voivodeship")

# === Save cleaned dataset ===
# Ensure the folder exists (create if needed)
# Export CSV without the Pandas index column
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
df_clean.to_csv(OUTPUT_FILE, index=False)

# === Final confirmation: show path and preview ===
print(f"\nCleaned data saved to: {OUTPUT_FILE}")
print(df_clean.head())

"""
extract_gus_data.py

Script to extract and clean GDP per capita data by voivodeship
from the Statistics Poland 2023 dataset.

Author: Alex Vidal
Created: 2024-04-08
"""

import pandas as pd
from pathlib import Path

# === Define project directories and file paths ===
# RAW_DATA_DIR: folder with the original Excel files from GUS
# PROCESSED_DATA_DIR: folder where the cleaned CSV will be saved
# RAW_FILE: specific Excel file with GDP data
# OUTPUT_FILE: name of the final cleaned dataset
RAW_DATA_DIR = Path("../data/raw")
PROCESSED_DATA_DIR = Path("../data/processed")
RAW_FILE = RAW_DATA_DIR / "regional_accounts_2023.xlsx"
OUTPUT_FILE = PROCESSED_DATA_DIR / "gdp_per_capita_2023.csv"

# === Load Excel sheet, skipping unnecessary header rows ===
# Sheet "1(148)" contains GDP per capita data
# skiprows=10 → jump the first 10 rows (titles, subtitles, "POLSKA" total)
# → the first row in Pandas will already be "Dolnośląskie" (row 11 in Excel)
sheet_name = "1(148)"
skip_rows = 10  # Skip first 10 rows with titles and subtitles

print(f"Reading file: {RAW_FILE.name}, sheet: {sheet_name}, skipping first {skip_rows} rows...")
df = pd.read_excel(RAW_FILE, sheet_name=sheet_name, skiprows=skip_rows)

# === Preview raw loaded data ===
print("\nLoaded data preview:")
print(df.head())

# === Rename columns of interest ===
# Column A (Excel col 0) → voivodeship (region name, text)
# Column D (Excel col 3) → gdp_per_capita_pln (GDP per capita in PLN, numeric)
df = df.rename(columns={
    df.columns[0]: "voivodeship",
    df.columns[3]: "gdp_per_capita_pln"
})

# === Remove non-valid rows ===
# Drop any row without a numeric value in GDP per capita
# This ensures that footnotes or extra text rows are excluded
df = df.dropna(subset=['gdp_per_capita_pln'])

# === Select only relevant columns for the pipeline ===
df_clean = df[["voivodeship", "gdp_per_capita_pln"]].copy()

# === Clean DataFrame formatting ===
# Reset index to start at 0..15 (after filtering rows)
# Sort alphabetically by voivodeship
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


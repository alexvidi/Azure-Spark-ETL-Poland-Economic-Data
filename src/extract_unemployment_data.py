"""
extract_unemployment_data.py

Script to extract and clean unemployment rate by voivodeship
from the Statistics Poland 2023 dataset.

Author: Alex Vidal
Created: 2024-04-08
"""

import pandas as pd
from pathlib import Path

# === Define project directories and file paths ===
# RAW_DATA_DIR: folder with the original Excel file from GUS
# PROCESSED_DATA_DIR: folder where the cleaned CSV will be saved
# RAW_FILE: Excel source file containing unemployment data
# OUTPUT_FILE: name of the final cleaned dataset
RAW_DATA_DIR = Path("../data/raw")
PROCESSED_DATA_DIR = Path("../data/processed")
RAW_FILE = RAW_DATA_DIR / "labour_market_2023.xlsx"
OUTPUT_FILE = PROCESSED_DATA_DIR / "unemployment_rate_2023.csv"

# === Load Excel sheet, skipping unnecessary header rows ===
# Sheet "1(34)" contains unemployment rate data
# skiprows=13 → jump the first 13 rows (titles, subtitles, Polska total)
# → the first row in Pandas will already be "Dolnośląskie" (row 14 in Excel)
sheet_name = "1(34)"
skip_rows = 13  # Skip metadata and headers

print(f"Reading file: {RAW_FILE.name}, sheet: {sheet_name}, skipping first {skip_rows} rows...")
df = pd.read_excel(RAW_FILE, sheet_name=sheet_name, skiprows=skip_rows)

# === Rename relevant columns ===
# Column A (Excel col 0) → voivodeship (region name, text)
# Column I (Excel col 8) → unemployment_rate (registered unemployment rate in %, numeric)
df = df.rename(columns={
    df.columns[0]: "voivodeship",
    df.columns[7]: "unemployment_rate"  # This column contains the % value directly
})

# === Remove non-valid rows ===
# Drop rows without a voivodeship or without an unemployment value
# This ensures that empty rows or notes at the end are excluded
df = df.dropna(subset=['voivodeship', 'unemployment_rate']).copy()


# === Select only relevant columns for the pipeline ===
# Keep only voivodeship and unemployment_rate
df_clean = df[["voivodeship", "unemployment_rate"]].copy()

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
print(f"\n Cleaned data saved to: {OUTPUT_FILE}")
print(df_clean.head())



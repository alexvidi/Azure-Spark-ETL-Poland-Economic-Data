"""
extract_gus_data.py

Script to extract and clean GDP per capita data by voivodeship
from the Statistics Poland 2023 dataset.

Author: Alex Vidal
Created: 2024-04-08
"""

import pandas as pd
from pathlib import Path

# === Define file paths ===
RAW_DATA_DIR = Path("../data/raw")
PROCESSED_DATA_DIR = Path("../data/processed")
RAW_FILE = RAW_DATA_DIR / "regional_accounts_2023.xlsx"
OUTPUT_FILE = PROCESSED_DATA_DIR / "gdp_per_capita_2023.csv"

# === Load the sheet, skipping unnecessary header rows ===
sheet_name = "1(148)"
skip_rows = 6  # Skip first 6 rows with titles and subtitles

print(f"Reading file: {RAW_FILE.name}, sheet: {sheet_name}, skipping first {skip_rows} rows...")
df = pd.read_excel(RAW_FILE, sheet_name=sheet_name, skiprows=skip_rows)

# === Preview raw loaded data ===
print("\nLoaded data preview:")
print(df.head())

# === Rename relevant columns ===
df = df.rename(columns={
    df.columns[0]: "voivodeship",
    df.columns[3]: "gdp_per_capita_pln"
})

# === Filter out total or non-region rows ===
df = df[df["voivodeship"].notna()]
df = df[~df["voivodeship"].str.contains("Polska", case=False, na=False)]

# === Keep only relevant columns ===
df_clean = df[["voivodeship", "gdp_per_capita_pln"]].copy()

# === Reset index and sort alphabetically (optional) ===
df_clean = df_clean.reset_index(drop=True)
df_clean = df_clean.sort_values(by="voivodeship")

# === Save cleaned data ===
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
df_clean.to_csv(OUTPUT_FILE, index=False)

# === Output summary ===
print(f"\nCleaned data saved to: {OUTPUT_FILE}")
print(df_clean.head())


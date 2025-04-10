"""
extract_population_data.py

Script to extract and clean total population by voivodeship
from the Statistics Poland 2023 dataset.

Author: Alex Vidal
Created: 2024-04-08
"""

import pandas as pd
from pathlib import Path

# === Define file paths ===
RAW_DATA_DIR = Path("../data/raw")
PROCESSED_DATA_DIR = Path("../data/processed")
RAW_FILE = RAW_DATA_DIR / "population_2023.xlsx"
OUTPUT_FILE = PROCESSED_DATA_DIR / "population_2023.csv"

# === Load the specific sheet, skipping non-data rows ===
sheet_name = "15 (33)"
skip_rows = 6  # Adjust if necessary depending on header position

print(f"Reading file: {RAW_FILE.name}, sheet: {sheet_name}, skipping first {skip_rows} rows...")
df = pd.read_excel(RAW_FILE, sheet_name=sheet_name, skiprows=skip_rows)

# === Preview loaded data ===
print("\nLoaded data preview:")
print(df.head())

# === Rename relevant columns (adjust if column indexes differ)
df = df.rename(columns={
    df.columns[0]: "voivodeship",
    df.columns[1]: "population_total"
})

# === Clean rows: keep only voivodeships ===
df = df[df["voivodeship"].notna()]
df = df[~df["voivodeship"].str.contains("Polska", case=False, na=False)]

# === Keep only relevant columns ===
df_clean = df[["voivodeship", "population_total"]].copy()

# === Reset index and sort ===
df_clean = df_clean.reset_index(drop=True)
df_clean = df_clean.sort_values(by="voivodeship")

# === Save cleaned data ===
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
df_clean.to_csv(OUTPUT_FILE, index=False)

# === Output summary ===
print(f"\nCleaned data saved to: {OUTPUT_FILE}")
print(df_clean.head())

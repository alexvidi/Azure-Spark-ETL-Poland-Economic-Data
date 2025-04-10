"""
extract_unemployment_data.py

Script to extract and clean unemployment rate by voivodeship
from the Statistics Poland 2023 dataset.

Author: Alex Vidal
Created: 2024-04-08
"""

import pandas as pd
from pathlib import Path

# === Define file paths ===
RAW_DATA_DIR = Path("../data/raw")
PROCESSED_DATA_DIR = Path("../data/processed")
RAW_FILE = RAW_DATA_DIR / "labour_market_2023.xlsx"
OUTPUT_FILE = PROCESSED_DATA_DIR / "unemployment_rate_2023.csv"

# === Load the specific sheet, skipping top metadata rows ===
sheet_name = "1(34)"
skip_rows = 6  # Skip metadata and headers

print(f"Reading file: {RAW_FILE.name}, sheet: {sheet_name}, skipping first {skip_rows} rows...")
df = pd.read_excel(RAW_FILE, sheet_name=sheet_name, skiprows=skip_rows)

# === Rename relevant columns ===
df = df.rename(columns={
    df.columns[0]: "voivodeship",
    df.columns[7]: "unemployment_rate"  # This column contains the % value directly
})

# === Drop rows with missing or unwanted labels ===
df = df[df["voivodeship"].notna()]
df = df[~df["voivodeship"].str.contains("Polska|VOIVODSHIPS|Osoby|Persons", case=False, na=False)]

# === Clean voivodeship names (remove leading/trailing spaces) ===
df["voivodeship"] = df["voivodeship"].str.strip()

# === Keep only required columns ===
df_clean = df[["voivodeship", "unemployment_rate"]].copy()

# === Reset index and sort ===
df_clean = df_clean.sort_values(by="voivodeship").reset_index(drop=True)

# === Save cleaned data ===
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
df_clean.to_csv(OUTPUT_FILE, index=False)

# === Output summary ===
print(f"\n✅ Cleaned data saved to: {OUTPUT_FILE}")
print(df_clean.head())



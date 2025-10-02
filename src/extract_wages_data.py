"""
extract_wages_data.py

Script to extract and clean average gross monthly wage data
by voivodeship from the Statistics Poland 2023 dataset.

Author: Alex Vidal
Created: 2024-04-08
"""

import pandas as pd
from pathlib import Path

# === Define project directories and file paths ===
# RAW_DATA_DIR: folder with the original Excel file from GUS
# PROCESSED_DATA_DIR: folder where the cleaned CSV will be saved
# RAW_FILE: Excel source file containing wage data
# OUTPUT_FILE: name of the final cleaned dataset
RAW_DATA_DIR = Path("../data/raw")
PROCESSED_DATA_DIR = Path("../data/processed")
RAW_FILE = RAW_DATA_DIR / "wages_and_salaries_2023.xlsx"
OUTPUT_FILE = PROCESSED_DATA_DIR / "average_gross_wage_2023.csv"

# === Load Excel sheet, skipping unnecessary header rows ===
# Sheet "1(47)" contains average gross monthly wages
# skiprows=11 → skip the first 11 rows (titles, subtitles, Polska total, and header info)
# → the first row in Pandas will already be "Dolnośląskie" (row 12 in Excel)
sheet_name = "1(47)"
skip_rows = 11

print(f"Reading file: {RAW_FILE.name}, sheet: {sheet_name}, skipping first {skip_rows} rows...")
df = pd.read_excel(RAW_FILE, sheet_name=sheet_name, skiprows=skip_rows)

# === Rename relevant columns ===
# Column A (Excel col 0) → voivodeship (region name, text)
# Column B (Excel col 1) → average_gross_wage (average gross monthly wage, numeric)
df = df.rename(columns={
    df.columns[0]: "voivodeship",
    df.columns[1]: "average_gross_wage"
})

# === Remove invalid or irrelevant rows ===
# Drop rows without region name (NaN)
# Exclude rows containing "Polska", "VOIVODSHIPS", "WOJEWÓDZTWA", or "Of which" (aggregated data or headers)
df = df[df["voivodeship"].notna()]
df = df[~df["voivodeship"].str.contains("Polska|VOIVODSHIPS|WOJEWÓDZTWA|Of which", case=False, na=False)]

# === Drop duplicates ===
# Ensure each voivodeship appears only once
df = df.drop_duplicates(subset="voivodeship")

# === Select only relevant columns for the pipeline ===
# Keep only voivodeship and average_gross_wage
df_clean = df[["voivodeship", "average_gross_wage"]].copy()
df_clean = df_clean.drop_duplicates(subset="voivodeship", keep="first")

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


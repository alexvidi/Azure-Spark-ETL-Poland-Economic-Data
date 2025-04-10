"""
extract_wages_data.py

Script to extract and clean average gross monthly wage data
by voivodeship from the Statistics Poland 2023 dataset.

Author: Alex Vidal
Updated: 2024-04-08
"""

import pandas as pd
from pathlib import Path

# === Define file paths ===
RAW_DATA_DIR = Path("../data/raw")
PROCESSED_DATA_DIR = Path("../data/processed")
RAW_FILE = RAW_DATA_DIR / "wages_and_salaries_2023.xlsx"
OUTPUT_FILE = PROCESSED_DATA_DIR / "average_gross_wage_2023.csv"

# === Load the specific sheet, skipping metadata ===
sheet_name = "1(47)"
skip_rows = 6

print(f"Reading file: {RAW_FILE.name}, sheet: {sheet_name}, skipping first {skip_rows} rows...")
df = pd.read_excel(RAW_FILE, sheet_name=sheet_name, skiprows=skip_rows)

# === Rename columns ===
df = df.rename(columns={
    df.columns[0]: "voivodeship",
    df.columns[1]: "average_gross_wage"
})

# === Remove empty rows and invalid headers ===
df = df[df["voivodeship"].notna()]
df = df[~df["voivodeship"].str.contains("Polska|VOIVODSHIPS|WOJEWÓDZTWA|Of which", case=False, na=False)]

# === Drop duplicates just in case ===
df = df.drop_duplicates(subset="voivodeship")

# === Keep only needed columns ===
df_clean = df[["voivodeship", "average_gross_wage"]].copy()

# === Reset index and sort ===
df_clean = df_clean.reset_index(drop=True)
df_clean = df_clean.sort_values(by="voivodeship")

# === Save cleaned data ===
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
df_clean.to_csv(OUTPUT_FILE, index=False)

# === Output summary ===
print(f"\n✅ Cleaned data saved to: {OUTPUT_FILE}")
print(df_clean.head())


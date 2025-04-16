"""
load_to_azure_sql.py

Script to load the cleaned regional economic dataset into Azure SQL Database.

Author: Alex Vidal
Created: 2024-04-09
"""

import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

# === Load credentials from .env file ===
load_dotenv("../.env")  

# === Azure SQL connection parameters ===
server = os.getenv("AZURE_SQL_SERVER")        
database = os.getenv("AZURE_SQL_DATABASE")    
username = os.getenv("AZURE_SQL_USERNAME")    
password = os.getenv("AZURE_SQL_PASSWORD")    
driver = '{ODBC Driver 17 for SQL Server}'    

# === Build connection string ===
conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

# === Load CSV ===
csv_path = "../data/processed/regional_economic_data_2023_spark.csv"
df = pd.read_csv(csv_path)

# === Connect to Azure SQL ===
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# === Create table ===
cursor.execute("""
IF OBJECT_ID('regional_economic_data_2023', 'U') IS NULL
CREATE TABLE regional_economic_data_2023 (
    voivodeship NVARCHAR(100),
    gdp_per_capita_pln FLOAT,
    unemployment_rate FLOAT,
    average_gross_wage FLOAT,
    population_total FLOAT
)
""")
conn.commit()

# === Insert data ===
insert_sql = """
INSERT INTO regional_economic_data_2023 (
    voivodeship, gdp_per_capita_pln, unemployment_rate, average_gross_wage, population_total
) VALUES (?, ?, ?, ?, ?)
"""

for _, row in df.iterrows():
    cursor.execute(insert_sql, tuple(row))

conn.commit()
cursor.close()
conn.close()

print("Data successfully loaded into Azure SQL Database.")

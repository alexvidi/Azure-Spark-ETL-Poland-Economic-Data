# 🇵🇱 End-to-End Azure & Spark ETL Pipeline for Polish Economic Insights (2023)

A professional, portfolio-ready data engineering project for regional economic and demographic analysis in Poland, leveraging Spark, Azure SQL, and Power BI.

---

## Executive Summary

This project implements a robust, end-to-end ETL pipeline to process, integrate, and visualize key socio-economic indicators for Polish voivodeships (2023). The solution enables data-driven insights for policy makers, analysts, and researchers through a cloud-based architecture and an interactive Power BI dashboard.

---

## Table of Contents
- [Overview](#overview)
- [Key Indicators](#key-indicators)
- [Data Sources](#data-sources)
- [Technologies](#technologies)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Business Value](#business-value)
- [SQL Analysis](#sql-analysis)
- [Getting Started](#getting-started)
- [Author](#author)

---

## Overview

This pipeline extracts, cleans, and integrates official datasets from Statistics Poland (GUS), covering GDP per capita, unemployment rate, average gross wage, and population by region. The processed data is loaded into Azure SQL Database and visualized in Power BI for actionable insights.

**Dashboard Demo:**
![Interactive Dashboard Demo](gif/interactive_dashboard_demo-ezgif.com-video-to-gif-converter.gif)

## Key Indicators
- GDP per capita
- Average gross monthly wage
- Unemployment rate
- Population

## Data Sources
- Official publications from [Statistics Poland (GUS)](https://stat.gov.pl/en/)

## Technologies
- **Python** (pandas, openpyxl, pyodbc): ETL and data processing
- **Spark (PySpark):** Distributed data transformation
- **Azure SQL Database:** Cloud data storage
- **Power BI:** Interactive dashboard and reporting
- **Azure Data Studio:** SQL analysis and database management

## Architecture

```mermaid
graph TD
    A[Raw Excel files (GUS)] --> B[Python ETL scripts]
    B --> C[Cleaned CSV files]
    C --> D[PySpark join & transformation]
    D --> E[Azure SQL Database]
    E --> F[Power BI Dashboard]
```

## Project Structure

```text
POLAND-ECONOMIC-PIPELINE/
├── dashboard/         # Dashboard demo video
├── data/
│   ├── processed/     # Cleaned datasets (CSV)
│   └── raw/           # Raw GUS Excel files
├── images/            # Power BI visualizations
├── powerbi/           # Power BI project file
├── sql/               # SQL analysis scripts
├── src/               # Python ETL scripts
├── requirements.txt   # Python dependencies
└── README.md          # Project documentation
```

## Business Value
- **Policy makers:** Compare regional economic performance
- **Researchers/analysts:** Detect disparities between GDP and wages
- **Decision makers:** Visualize unemployment and socio-economic patterns
- **Stakeholders:** Communicate insights via interactive dashboards

## SQL Analysis

Key insights are derived using SQL queries in Azure Data Studio. Example:

**GDP to Wage Ratio by Voivodeship**
```sql
SELECT
    voivodeship,
    gdp_per_capita_pln / average_gross_wage AS gdp_to_wage_ratio
FROM regional_economic_data_2023 
ORDER BY gdp_to_wage_ratio DESC;
```
Additional queries are available in the [`sql/`](./sql/) directory.

## Getting Started

### Prerequisites
- Python 3.8+
- Azure SQL Database instance
- Power BI Desktop

### Setup Instructions
1. **Clone the repository:**
   ```bash
   git clone https://github.com/alexvidi/Azure-Spark-ETL-Poland-Economic-Data.git
   ```
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Run ETL scripts:**
   Execute scripts in `src/` to process raw data and generate cleaned datasets.
4. **Load data to Azure SQL:**
   Upload processed CSVs from `data/processed/` to your Azure SQL Database.
5. **Open Power BI dashboard:**
   Open `regional_dashboard_poland_2023.pbix` in Power BI Desktop.
6. **Configure data source:**
   Refresh the data connection to link with your Azure SQL Database.
7. **Explore and analyze:**
   Interact with the dashboard or export visualizations as needed.

---

## Author

**Alexandre Vidal**  
[alexvidaldepalol@gmail.com](mailto:alexvidaldepalol@gmail.com)  
[LinkedIn](https://www.linkedin.com/in/alex-vidal-de-palol-a18538155/)  
[GitHub](https://github.com/alexvidi)

Project Repository: [Azure-Spark-ETL-Poland-Economic-Data](https://github.com/alexvidi/Azure-Spark-ETL-Poland-Economic-Data)

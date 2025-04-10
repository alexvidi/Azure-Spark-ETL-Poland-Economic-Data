# Regional Socio-Economic Data Pipeline: Poland 2023

## Overview

This project delivers a complete end-to-end data engineering pipeline focused on regional economic and demographic indicators in Poland for the year 2023. The final result is an interactive Power BI dashboard, which visualizes and analyzes GDP per capita, unemployment rate, gross average wage, and population across Polish voivodeships.

![Interactive Dashboard Demo](gif/interactive_dashboard_demo-ezgif.com-video-to-gif-converter.gif)

## Technologies Used

* **Python** (pandas, openpyxl, pyodbc) - For data extraction, transformation, and loading.
* **Azure SQL Database** - Cloud-based relational database for storing structured data.
* **Spark (via PySpark)** - For distributed data merging and transformation.
* **Power BI** - For creating interactive dashboards and visual analysis.
* **Azure Data Studio** - For executing SQL queries and database inspection.

## Objectives

* Extract and process official socio-economic datasets from Statistics Poland (GUS).
* Clean, transform, and integrate the data using Python and SQL.
* Load structured data into Azure SQL Database.
* Create insightful visualizations with Power BI.
* Showcase a fully professional and portfolio-ready data pipeline.

## Project Structure

```
POLAND-ECONOMIC-PIPELINE/
├── dashboard/
│   └── interactive_dashboard_demo.mp4         # Demo video showing dashboard interactivity
├── data/
│   ├── processed/                            # Cleaned and transformed datasets
│   │   ├── average_gross_wage_2023.csv
│   │   ├── gdp_per_capita_2023.csv
│   │   ├── population_2023.csv
│   │   ├── regional_economic_data_2023_spark.csv
│   │   └── unemployment_rate_2023.csv
│   ├── raw/                                 # Raw source files from Statistics Poland
│   │   ├── labour_market_2023.xlsx
│   │   ├── population_2023.xlsx
│   │   ├── regional_accounts_2023.xlsx
│   │   └── wages_and_salaries_2023.xlsx
├── images/                                  # Exported visualizations from Power BI
│   ├── GDP to Wage Ratio by Voivodeship.png
│   ├── Top 5 Voivodeships by GDP per Capita.png
│   └── Voivodeships with the Highest Unemployment Rates.png
├── powerbi/                                 # Power BI dashboard project
│   └── regional_dashboard_poland_2023.pbix
├── sql/                                     # Key SQL queries executed in Azure Data Studio
│   ├── gdp_to_wage_ratio.sql
│   ├── top_gdp_per_capita.sql
│   └── unemployment_highest.sql
├── src/                                     # ETL scripts in Python
│   ├── extract_gus_data.py
│   ├── extract_population_data.py
│   ├── extract_unemployment_data.py
│   ├── extract_wages_data.py
│   ├── load_to_azure_sql.py
│   └── merge_with_spark.py
├── requirements.txt                         # Python dependencies
└── README.md                                # Project documentation
```
## Highlights

* Fully reproducible ETL pipeline using Python scripts.
* Clean code structure with comments in English.
* Cloud integration leveraging Azure services.
* Powerful and interactive regional dashboard featuring an animated map selection.
* SQL-based analysis for deriving additional insights and validating data.

## Getting Started

1.  Clone the repository to your local machine.
2.  Install the required Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Execute the ETL scripts located in the `src/` directory to process the raw data and generate the cleaned datasets.
4.  Upload the processed datasets (from `data/processed/`) to your Azure SQL Database instance.
5.  Open the Power BI project file (`regional_dashboard_poland_2023.pbix`) located in the `powerbi/` directory.
6.  Refresh the data connection within Power BI to link to your Azure SQL Database.
7.  Explore the interactive dashboard or export visualizations/reports as needed.

## Author

* **Alexandre Vidal**  

    10/04/2025
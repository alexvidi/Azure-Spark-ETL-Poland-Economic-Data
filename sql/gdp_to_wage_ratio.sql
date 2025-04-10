-- Query 3: GDP to Wage Ratio by Voivodeship
-- Evaluates the ratio between economic output and average gross wages
SELECT
    voivodeship,
    gdp_per_capita_pln AS [GDP per Capita (PLN)],
    average_gross_wage AS [Average Monthly Gross Wage (PLN)],
    ROUND(CAST(gdp_per_capita_pln AS FLOAT) / average_gross_wage, 2) AS [GDP to Wage Ratio]
FROM regional_economic_data_2023
ORDER BY [GDP to Wage Ratio] DESC;


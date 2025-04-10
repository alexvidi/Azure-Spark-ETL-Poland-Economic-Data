-- Query 1: Top 5 Voivodeships by GDP per Capita
-- Shows the top-performing regions in terms of economic productivity
SELECT TOP 5
    voivodeship,
    gdp_per_capita_pln AS [GDP per Capita (PLN)]
FROM regional_economic_data_2023
ORDER BY gdp_per_capita_pln DESC;


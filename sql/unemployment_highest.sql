-- Query 2: Voivodeships with the Highest Unemployment Rates
-- Identifies regions with the most significant employment challenges
SELECT
    voivodeship,
    unemployment_rate AS [Unemployment Rate (%)],
    population_total AS [Population (in thousands)]
FROM regional_economic_data_2023
ORDER BY unemployment_rate DESC;


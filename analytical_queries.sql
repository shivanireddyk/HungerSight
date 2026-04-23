-- ============================================================
-- HungerSight Analytical SQL Queries
-- Database: SQLite (SQLite syntax = SQL Server compatible)
-- Author: Shivani Krishnama | U.S. Hunger Data Analytics Intern Portfolio
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- QUERY 1: Food Insecurity Ranking — All Florida Counties (2023)
-- ─────────────────────────────────────────────────────────────
SELECT
    county_name,
    insecurity_rate,
    insecurity_count,
    meal_gap_millions,
    RANK() OVER (ORDER BY insecurity_rate DESC) AS insecurity_rank
FROM feeding_america_data
WHERE year = 2023
ORDER BY insecurity_rank;

-- ─────────────────────────────────────────────────────────────
-- QUERY 2: Year-Over-Year Insecurity Change (Window Function: LAG)
-- ─────────────────────────────────────────────────────────────
SELECT
    county_name,
    year,
    insecurity_rate,
    LAG(insecurity_rate, 1) OVER (PARTITION BY county_name ORDER BY year) AS prev_year_rate,
    ROUND(insecurity_rate - LAG(insecurity_rate, 1) OVER (PARTITION BY county_name ORDER BY year), 2) AS yoy_change
FROM feeding_america_data
ORDER BY county_name, year;

-- ─────────────────────────────────────────────────────────────
-- QUERY 3: Partner Efficiency Stored Procedure (as parameterized query)
-- Returns partner efficiency scores for a given county
-- ─────────────────────────────────────────────────────────────
-- Usage: Replace :county_param with target county name
SELECT
    partner_name,
    county,
    org_type,
    annual_meals_delivered,
    annual_budget_usd,
    meals_per_dollar,
    population_served,
    composite_efficiency_score,
    efficiency_tier,
    RANK() OVER (ORDER BY composite_efficiency_score DESC) AS efficiency_rank
FROM partner_data
WHERE county = 'Seminole'   -- :county_param
ORDER BY composite_efficiency_score DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 4: County Profile — Unified View (CTE)
-- Joins Census + USDA + BLS + Feeding America
-- ─────────────────────────────────────────────────────────────
WITH census_snapshot AS (
    SELECT county_name, poverty_rate, median_income, snap_participation_rate,
           total_pop, unemployment_rate_annual
    FROM census_data WHERE year = 2023
),
usda_snapshot AS (
    SELECT county_name, food_desert_score, grocery_stores_per_10k,
           low_income_low_access_pct
    FROM usda_data WHERE year = 2023
),
insecurity_snapshot AS (
    SELECT county_name, insecurity_rate, child_insecurity_rate, meal_gap_millions
    FROM feeding_america_data WHERE year = 2023
)
SELECT
    c.county_name,
    c.total_pop,
    c.poverty_rate,
    c.median_income,
    c.snap_participation_rate,
    u.food_desert_score,
    u.grocery_stores_per_10k,
    i.insecurity_rate,
    i.child_insecurity_rate,
    i.meal_gap_millions
FROM census_snapshot c
LEFT JOIN usda_snapshot u  ON c.county_name = u.county_name
LEFT JOIN insecurity_snapshot i ON c.county_name = i.county_name
ORDER BY i.insecurity_rate DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 5: COVID-19 Impact — Unemployment Spike Detection
-- ─────────────────────────────────────────────────────────────
SELECT
    county_name,
    year,
    month,
    unemployment_rate,
    AVG(unemployment_rate) OVER (
        PARTITION BY county_name
        ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3mo_avg,
    unemployment_rate - AVG(unemployment_rate) OVER (
        PARTITION BY county_name
        ORDER BY year, month
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS deviation_from_12mo_avg
FROM bls_data
WHERE year IN (2019, 2020, 2021)
ORDER BY county_name, year, month;

-- ─────────────────────────────────────────────────────────────
-- QUERY 6: ZIP Code Risk Score Quartiles (NTILE window function)
-- ─────────────────────────────────────────────────────────────
SELECT
    zip_code,
    county,
    raw_risk_score,
    poverty_rate,
    unemployment_rate,
    food_desert_score,
    NTILE(4) OVER (ORDER BY raw_risk_score DESC) AS risk_quartile,
    CASE NTILE(4) OVER (ORDER BY raw_risk_score DESC)
        WHEN 1 THEN 'Critical'
        WHEN 2 THEN 'High'
        WHEN 3 THEN 'Moderate'
        WHEN 4 THEN 'Low'
    END AS risk_label
FROM zip_data
ORDER BY raw_risk_score DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 7: Monthly Unemployment Trend with MOM Change
-- ─────────────────────────────────────────────────────────────
SELECT
    county_name,
    year,
    month,
    unemployment_rate,
    ROUND(
        unemployment_rate -
        LAG(unemployment_rate, 1) OVER (PARTITION BY county_name ORDER BY year, month),
    2) AS mom_change,
    ROUND(
        unemployment_rate -
        LAG(unemployment_rate, 12) OVER (PARTITION BY county_name ORDER BY year, month),
    2) AS yoy_change
FROM bls_data
WHERE county_name IN ('Seminole', 'Orange', 'Osceola')
ORDER BY county_name, year, month;

-- ─────────────────────────────────────────────────────────────
-- QUERY 8: SNAP Coverage Gap Analysis
-- Counties where SNAP participation is far below poverty rate
-- ─────────────────────────────────────────────────────────────
SELECT
    c.county_name,
    c.poverty_rate,
    c.snap_participation_rate,
    ROUND(c.poverty_rate - c.snap_participation_rate, 2) AS snap_gap,
    f.insecurity_rate,
    CASE
        WHEN (c.poverty_rate - c.snap_participation_rate) > 10 THEN 'Critical Gap'
        WHEN (c.poverty_rate - c.snap_participation_rate) > 5  THEN 'Moderate Gap'
        ELSE 'Adequate Coverage'
    END AS snap_coverage_status
FROM census_data c
JOIN feeding_america_data f ON c.county_name = f.county_name
WHERE c.year = 2023 AND f.year = 2023
ORDER BY snap_gap DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 9: Partner Meals Per Dollar — All Organizations
-- ─────────────────────────────────────────────────────────────
SELECT
    partner_name,
    county,
    org_type,
    annual_meals_delivered,
    annual_budget_usd,
    ROUND(CAST(annual_meals_delivered AS FLOAT) / annual_budget_usd, 3) AS meals_per_dollar,
    RANK() OVER (ORDER BY CAST(annual_meals_delivered AS FLOAT) / annual_budget_usd DESC) AS mpd_rank,
    efficiency_tier
FROM partner_data
ORDER BY meals_per_dollar DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 10: County Insecurity Trend — 5-Year Rolling Average
-- ─────────────────────────────────────────────────────────────
SELECT
    county_name,
    year,
    insecurity_rate,
    ROUND(AVG(insecurity_rate) OVER (
        PARTITION BY county_name
        ORDER BY year
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 3) AS rolling_5yr_avg,
    MIN(insecurity_rate) OVER (PARTITION BY county_name) AS historical_min,
    MAX(insecurity_rate) OVER (PARTITION BY county_name) AS historical_max
FROM feeding_america_data
ORDER BY county_name, year;

-- ─────────────────────────────────────────────────────────────
-- QUERY 11: High-Risk ZIP Codes in Seminole County
-- Directly relevant to Longwood, FL headquarters
-- ─────────────────────────────────────────────────────────────
SELECT
    zip_code,
    county,
    raw_risk_score,
    poverty_rate,
    unemployment_rate,
    food_desert_score,
    snap_participation_rate,
    median_income
FROM zip_data
WHERE county = 'Seminole'
ORDER BY raw_risk_score DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 12: Food Desert Correlation with Insecurity (CTE + JOIN)
-- ─────────────────────────────────────────────────────────────
WITH food_desert_ranked AS (
    SELECT county_name,
           food_desert_score,
           NTILE(3) OVER (ORDER BY food_desert_score DESC) AS desert_tier
    FROM usda_data WHERE year = 2023
)
SELECT
    d.county_name,
    d.food_desert_score,
    d.desert_tier,
    CASE d.desert_tier WHEN 1 THEN 'Severe Desert' WHEN 2 THEN 'Moderate' ELSE 'Good Access' END AS access_label,
    f.insecurity_rate,
    f.meal_gap_millions
FROM food_desert_ranked d
JOIN feeding_america_data f ON d.county_name = f.county_name AND f.year = 2023
ORDER BY d.food_desert_score DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 13: Child vs. Adult Insecurity Gap
-- ─────────────────────────────────────────────────────────────
SELECT
    county_name,
    year,
    insecurity_rate AS adult_insecurity_rate,
    child_insecurity_rate,
    ROUND(child_insecurity_rate - insecurity_rate, 2) AS child_adult_gap,
    RANK() OVER (PARTITION BY year ORDER BY child_insecurity_rate - insecurity_rate DESC) AS gap_rank
FROM feeding_america_data
WHERE year = 2023
ORDER BY child_adult_gap DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 14: Org Type Performance Summary (Aggregation + GROUP BY)
-- ─────────────────────────────────────────────────────────────
SELECT
    org_type,
    COUNT(*) AS num_organizations,
    SUM(annual_meals_delivered) AS total_meals,
    ROUND(AVG(meals_per_dollar), 3) AS avg_meals_per_dollar,
    ROUND(AVG(composite_efficiency_score), 4) AS avg_efficiency_score,
    SUM(population_served) AS total_population_served
FROM partner_data
GROUP BY org_type
ORDER BY avg_efficiency_score DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 15: Pandemic Recovery Tracking
-- Counties slowest to recover post-COVID
-- ─────────────────────────────────────────────────────────────
WITH pre_covid AS (
    SELECT county_name, insecurity_rate AS rate_2019
    FROM feeding_america_data WHERE year = 2019
),
post_covid AS (
    SELECT county_name, insecurity_rate AS rate_2023
    FROM feeding_america_data WHERE year = 2023
)
SELECT
    pre.county_name,
    pre.rate_2019,
    post.rate_2023,
    ROUND(post.rate_2023 - pre.rate_2019, 2) AS net_change,
    CASE
        WHEN post.rate_2023 - pre.rate_2019 > 2  THEN 'Not Recovered'
        WHEN post.rate_2023 - pre.rate_2019 > 0  THEN 'Partial Recovery'
        ELSE 'Full Recovery'
    END AS recovery_status
FROM pre_covid pre
JOIN post_covid post ON pre.county_name = post.county_name
ORDER BY net_change DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 16: Unemployment-to-Insecurity Correlation by County
-- ─────────────────────────────────────────────────────────────
SELECT
    c.county_name,
    c.unemployment_rate_annual,
    f.insecurity_rate,
    ROUND(c.unemployment_rate_annual / f.insecurity_rate, 3) AS ue_to_insecurity_ratio
FROM census_data c
JOIN feeding_america_data f ON c.county_name = f.county_name
WHERE c.year = 2023 AND f.year = 2023
ORDER BY ue_to_insecurity_ratio DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 17: Weighted Risk Score — Composite County Index
-- ─────────────────────────────────────────────────────────────
SELECT
    c.county_name,
    ROUND(
        (c.poverty_rate * 0.35) +
        (c.unemployment_rate_annual * 0.25) +
        (u.food_desert_score * 0.25) +
        ((100 - c.snap_participation_rate) * 0.15),
    2) AS weighted_risk_index,
    f.insecurity_rate,
    RANK() OVER (ORDER BY
        (c.poverty_rate * 0.35) +
        (c.unemployment_rate_annual * 0.25) +
        (u.food_desert_score * 0.25) +
        ((100 - c.snap_participation_rate) * 0.15)
    DESC) AS risk_rank
FROM census_data c
JOIN usda_data u ON c.county_name = u.county_name AND u.year = 2023
JOIN feeding_america_data f ON c.county_name = f.county_name AND f.year = 2023
WHERE c.year = 2023
ORDER BY weighted_risk_index DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 18: Grocery Access vs. Food Insecurity
-- ─────────────────────────────────────────────────────────────
SELECT
    u.county_name,
    u.grocery_stores_per_10k,
    u.low_income_low_access_pct,
    f.insecurity_rate,
    CASE
        WHEN u.grocery_stores_per_10k < 1.5 THEN 'Critically Underserved'
        WHEN u.grocery_stores_per_10k < 2.5 THEN 'Underserved'
        ELSE 'Adequately Served'
    END AS grocery_access_tier
FROM usda_data u
JOIN feeding_america_data f ON u.county_name = f.county_name AND f.year = 2023
WHERE u.year = 2023
ORDER BY u.grocery_stores_per_10k ASC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 19: Partner Budget Efficiency — Cost per Person Served
-- ─────────────────────────────────────────────────────────────
SELECT
    partner_name,
    county,
    annual_budget_usd,
    population_served,
    ROUND(CAST(annual_budget_usd AS FLOAT) / NULLIF(population_served, 0), 2) AS cost_per_person,
    ROUND(CAST(annual_meals_delivered AS FLOAT) / NULLIF(population_served, 0), 1) AS meals_per_person,
    efficiency_tier
FROM partner_data
ORDER BY cost_per_person ASC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 20: Central Florida Focus — Seminole + Orange + Osceola
-- Executive summary query for stakeholder reporting
-- ─────────────────────────────────────────────────────────────
WITH central_fl AS (
    SELECT 'Seminole' AS county UNION ALL
    SELECT 'Orange'   UNION ALL
    SELECT 'Osceola'  UNION ALL
    SELECT 'Lake'     UNION ALL
    SELECT 'Volusia'
)
SELECT
    f.county_name,
    f.insecurity_rate,
    f.meal_gap_millions,
    c.poverty_rate,
    c.median_income,
    u.food_desert_score,
    (SELECT COUNT(*) FROM partner_data p WHERE p.county = f.county_name) AS active_partners,
    (SELECT SUM(annual_meals_delivered) FROM partner_data p WHERE p.county = f.county_name) AS total_partner_meals
FROM feeding_america_data f
JOIN census_data c ON f.county_name = c.county_name AND c.year = 2023
JOIN usda_data u   ON f.county_name = u.county_name AND u.year = 2023
JOIN central_fl cf ON f.county_name = cf.county
WHERE f.year = 2023
ORDER BY f.insecurity_rate DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 21: Volunteer Capacity vs. Population Served
-- ─────────────────────────────────────────────────────────────
SELECT
    partner_name,
    volunteers,
    population_served,
    ROUND(CAST(population_served AS FLOAT) / NULLIF(volunteers, 0), 1) AS people_per_volunteer,
    annual_meals_delivered,
    ROUND(CAST(annual_meals_delivered AS FLOAT) / NULLIF(volunteers, 0), 0) AS meals_per_volunteer,
    efficiency_tier
FROM partner_data
ORDER BY meals_per_volunteer DESC;

-- ─────────────────────────────────────────────────────────────
-- QUERY 22: Median Income Bracket Analysis
-- ─────────────────────────────────────────────────────────────
WITH income_brackets AS (
    SELECT
        county_name,
        median_income,
        CASE
            WHEN median_income < 40000 THEN 'Low Income (<$40K)'
            WHEN median_income < 55000 THEN 'Lower-Middle ($40K-$55K)'
            WHEN median_income < 70000 THEN 'Middle ($55K-$70K)'
            ELSE 'Upper-Middle ($70K+)'
        END AS income_bracket
    FROM census_data WHERE year = 2023
)
SELECT
    ib.income_bracket,
    COUNT(*) AS county_count,
    ROUND(AVG(f.insecurity_rate), 2) AS avg_insecurity_rate,
    ROUND(AVG(c.poverty_rate), 2) AS avg_poverty_rate,
    SUM(f.meal_gap_millions) AS total_meal_gap
FROM income_brackets ib
JOIN feeding_america_data f ON ib.county_name = f.county_name AND f.year = 2023
JOIN census_data c ON ib.county_name = c.county_name AND c.year = 2023
GROUP BY ib.income_bracket
ORDER BY avg_insecurity_rate DESC;

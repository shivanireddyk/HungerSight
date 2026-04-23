# HungerSight Data Dictionary

## Database: `hungersight.db` (SQLite)

---

## Tables

### `census_data`
U.S. Census Bureau demographic data by Florida county.

| Column | Type | Description |
|--------|------|-------------|
| county_name | TEXT | Florida county name |
| total_pop | INTEGER | Total population |
| poverty_rate | REAL | % of population below poverty line |
| child_poverty_rate | REAL | % of children below poverty line |
| median_income | INTEGER | Median household income (USD) |
| snap_participation_rate | REAL | % of eligible households receiving SNAP |
| households_with_children | INTEGER | Count of households with children |
| single_parent_households_pct | REAL | % single-parent households |
| unemployment_rate_annual | REAL | Annual unemployment rate (%) |
| year | INTEGER | Data year |

---

### `usda_data`
USDA Economic Research Service food access metrics.

| Column | Type | Description |
|--------|------|-------------|
| county_name | TEXT | Florida county name |
| food_desert_score | REAL | 1–10 score (10 = severe food desert) |
| grocery_stores_per_10k | REAL | Grocery stores per 10,000 residents |
| fast_food_ratio | REAL | Fast food to grocery store ratio |
| vehicle_access_pct | REAL | % households with vehicle access |
| low_income_low_access_pct | REAL | % low-income residents with low food access |
| snap_authorized_stores_per_10k | REAL | SNAP-authorized retailers per 10,000 |
| year | INTEGER | Data year |

---

### `bls_data`
Bureau of Labor Statistics monthly unemployment series.

| Column | Type | Description |
|--------|------|-------------|
| county_name | TEXT | Florida county name |
| year | INTEGER | Year |
| month | INTEGER | Month (1–12) |
| unemployment_rate | REAL | Monthly unemployment rate (%) |
| labor_force | INTEGER | Total labor force size |
| employed | INTEGER | Employed count |
| unemployed | INTEGER | Unemployed count |

---

### `feeding_america_data`
Feeding America Map the Meal Gap annual county statistics.

| Column | Type | Description |
|--------|------|-------------|
| county_name | TEXT | Florida county name |
| year | INTEGER | Data year (2019–2023) |
| insecurity_rate | REAL | Food insecurity rate (%) |
| insecurity_count | INTEGER | # food-insecure individuals |
| child_insecurity_rate | REAL | Child food insecurity rate (%) |
| meal_gap_millions | REAL | Annual meal gap in millions |
| cost_per_meal | REAL | Average cost per meal (USD) |
| snap_coverage_gap_pct | REAL | % of insecure not covered by SNAP |

---

### `zip_data`
ZIP-code level risk indicators for Central Florida.

| Column | Type | Description |
|--------|------|-------------|
| zip_code | TEXT | 5-digit ZIP code |
| county | TEXT | Parent county |
| latitude | REAL | Centroid latitude |
| longitude | REAL | Centroid longitude |
| poverty_rate | REAL | ZIP-level poverty rate (%) |
| unemployment_rate | REAL | ZIP-level unemployment rate (%) |
| food_desert_score | REAL | 1–10 food desert score |
| snap_participation_rate | REAL | SNAP participation rate (%) |
| median_income | INTEGER | Median household income (USD) |
| total_pop | INTEGER | ZIP population |
| raw_risk_score | REAL | Composite risk score 0–100 |

---

### `partner_data`
U.S. Hunger partner organization efficiency metrics.

| Column | Type | Description |
|--------|------|-------------|
| partner_id | INTEGER | Unique partner ID |
| partner_name | TEXT | Organization name |
| county | TEXT | Operating county |
| org_type | TEXT | Organization type (food_bank, faith_based, etc.) |
| annual_budget_usd | INTEGER | Annual operating budget (USD) |
| annual_meals_delivered | INTEGER | Annual meals delivered |
| meals_per_dollar | REAL | Efficiency metric: meals / budget dollar |
| population_served | INTEGER | Annual unique individuals served |
| geographic_reach_score | REAL | 0–1 geographic coverage score |
| composite_efficiency_score | REAL | Weighted composite: mpd(40%) + pop(30%) + geo(30%) |
| efficiency_tier | TEXT | Platinum / Gold / Silver / Bronze |
| year_established | INTEGER | Founding year |
| volunteers | INTEGER | Active volunteer count |

---

## Views

| View | Description |
|------|-------------|
| `v_insecurity_trends` | YOY insecurity change using LAG() window function |
| `v_partner_efficiency` | Clean partner efficiency summary |
| `v_zip_risk` | ZIP risk scores with NTILE quartile ranking |
| `v_county_profile` | Unified county view joining all 4 sources |
| `v_unemployment_trends` | MOM unemployment change using LAG() |
| `v_insecurity_ranking` | 2023 county insecurity RANK() |

---

## Risk Score Formula

```
risk_score = (poverty_rate × 0.35) + 
             (unemployment_rate × 0.25) + 
             (food_desert_score × 0.25) + 
             ((100 - snap_participation_rate) × 0.15)
```

Scale: 0 = No risk | 30 = Low | 50 = Moderate | 70 = High | 100 = Critical

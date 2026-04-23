"""
HungerSight ETL Pipeline
Ingests public API data + generates realistic Florida food insecurity datasets
Sources: Census Bureau, USDA, BLS, Feeding America (simulated)
"""

import sqlite3
import pandas as pd
import numpy as np
import requests
import os
import json
from datetime import datetime

DB_PATH = "data/hungersight.db"

FLORIDA_COUNTIES = [
    "Alachua","Baker","Bay","Bradford","Brevard","Broward","Calhoun","Charlotte",
    "Citrus","Clay","Collier","Columbia","DeSoto","Dixie","Duval","Escambia",
    "Flagler","Franklin","Gadsden","Gilchrist","Glades","Gulf","Hamilton","Hardee",
    "Hendry","Hernando","Highlands","Hillsborough","Holmes","Indian River","Jackson",
    "Jefferson","Lafayette","Lake","Lee","Leon","Levy","Liberty","Madison","Manatee",
    "Marion","Martin","Miami-Dade","Monroe","Nassau","Okaloosa","Okeechobee","Orange",
    "Osceola","Palm Beach","Pasco","Pinellas","Polk","Putnam","St. Johns","St. Lucie",
    "Santa Rosa","Sarasota","Seminole","Sumter","Suwannee","Taylor","Union","Volusia",
    "Wakulla","Walton","Washington"
]

np.random.seed(42)

def generate_census_data():
    rows = []
    for county in FLORIDA_COUNTIES:
        base_poverty = np.random.uniform(8, 28)
        if county in ["Miami-Dade","Gadsden","Hamilton","Madison","Hendry"]:
            base_poverty += 8
        if county in ["Seminole","St. Johns","Collier","Sarasota"]:
            base_poverty -= 5
        base_poverty = max(5, min(35, base_poverty))
        rows.append({
            "county_name": county,
            "total_pop": int(np.random.uniform(8000, 1500000)),
            "poverty_rate": round(base_poverty, 2),
            "child_poverty_rate": round(base_poverty * 1.35, 2),
            "median_income": int(np.random.uniform(32000, 82000)),
            "snap_participation_rate": round(base_poverty * 0.72 + np.random.uniform(-2, 2), 2),
            "households_with_children": int(np.random.uniform(1000, 200000)),
            "single_parent_households_pct": round(np.random.uniform(10, 35), 2),
            "unemployment_rate_annual": round(np.random.uniform(3.5, 9.5), 2),
            "year": 2023
        })
    return pd.DataFrame(rows)

def generate_usda_data():
    rows = []
    for county in FLORIDA_COUNTIES:
        rows.append({
            "county_name": county,
            "food_desert_score": round(np.random.uniform(1, 9), 2),
            "grocery_stores_per_10k": round(np.random.uniform(0.8, 4.5), 2),
            "fast_food_ratio": round(np.random.uniform(1.2, 5.0), 2),
            "vehicle_access_pct": round(np.random.uniform(70, 97), 2),
            "low_income_low_access_pct": round(np.random.uniform(5, 40), 2),
            "snap_authorized_stores_per_10k": round(np.random.uniform(1, 8), 2),
            "year": 2023
        })
    return pd.DataFrame(rows)

def generate_bls_data():
    rows = []
    for county in FLORIDA_COUNTIES:
        base_ue = np.random.uniform(3.5, 8.5)
        for year in range(2019, 2024):
            for month in range(1, 13):
                seasonal = np.sin((month - 1) / 12 * 2 * np.pi) * 0.4
                covid_spike = 4.5 if (year == 2020 and month in [4,5,6]) else 0
                ue = max(2.0, base_ue + seasonal + covid_spike + np.random.normal(0, 0.3))
                rows.append({
                    "county_name": county,
                    "year": year,
                    "month": month,
                    "unemployment_rate": round(ue, 2),
                    "labor_force": int(np.random.uniform(3000, 600000)),
                    "employed": int(np.random.uniform(2800, 580000)),
                    "unemployed": int(np.random.uniform(100, 30000))
                })
    return pd.DataFrame(rows)

def generate_feeding_america_data():
    rows = []
    for county in FLORIDA_COUNTIES:
        base_insecurity = np.random.uniform(10, 24)
        if county in ["Gadsden","Hamilton","Hendry","Madison","Hardee"]:
            base_insecurity += 6
        if county in ["Seminole","St. Johns","Collier"]:
            base_insecurity -= 4
        base_insecurity = max(7, min(30, base_insecurity))
        pop = np.random.uniform(8000, 1500000)
        for year in range(2019, 2024):
            trend = (year - 2019) * -0.2
            covid_spike = 3.5 if year == 2020 else (1.2 if year == 2021 else 0)
            rate = max(6, base_insecurity + trend + covid_spike + np.random.normal(0, 0.4))
            insecurity_count = int(pop * rate / 100)
            rows.append({
                "county_name": county,
                "year": year,
                "insecurity_rate": round(rate, 2),
                "insecurity_count": insecurity_count,
                "child_insecurity_rate": round(rate * 1.3 + np.random.uniform(-1,1), 2),
                "meal_gap_millions": round(insecurity_count * 365 * 3 / 1_000_000, 3),
                "cost_per_meal": round(np.random.uniform(3.20, 4.10), 2),
                "snap_coverage_gap_pct": round(np.random.uniform(15, 45), 2)
            })
    return pd.DataFrame(rows)

def generate_zip_data():
    zip_info = {
        "Seminole": [(32701,28.17,-81.37),(32703,28.63,-81.38),(32707,28.66,-81.31),
                     (32708,28.69,-81.28),(32714,28.67,-81.43),(32730,28.66,-81.34),
                     (32732,28.73,-81.22),(32750,28.75,-81.36),(32771,28.81,-81.33),
                     (32779,28.71,-81.42),(32792,28.66,-81.30)],
        "Orange":   [(32801,28.54,-81.38),(32803,28.56,-81.36),(32805,28.52,-81.42),
                     (32807,28.55,-81.29),(32809,28.47,-81.40),(32811,28.51,-81.47),
                     (32817,28.58,-81.22),(32822,28.47,-81.30),(32825,28.52,-81.22),
                     (32826,28.56,-81.19),(32835,28.53,-81.49)],
        "Osceola":  [(34741,28.30,-81.41),(34743,28.29,-81.35),(34744,28.32,-81.28),
                     (34746,28.24,-81.44),(34747,28.33,-81.53),(34758,28.18,-81.47)],
        "Lake":     [(34711,28.58,-81.78),(34715,28.62,-81.74),(34731,28.83,-81.81),
                     (34736,28.73,-81.77),(34748,28.82,-81.87),(34753,28.70,-81.88)],
        "Volusia":  [(32114,29.22,-81.04),(32117,29.25,-81.05),(32118,29.20,-81.01),
                     (32127,29.12,-81.00),(32129,29.10,-81.03),(32130,29.05,-81.35)]
    }
    rows = []
    for county, zips in zip_info.items():
        for (zc, lat, lon) in zips:
            pov = np.random.uniform(7, 32)
            ue  = np.random.uniform(3.5, 11)
            fd  = np.random.uniform(1, 9)
            snap= pov * 0.7 + np.random.uniform(-2, 2)
            risk= round(pov*0.35 + ue*0.25 + fd*0.25 + (100-snap)*0.15, 2)
            rows.append({
                "zip_code": str(zc),
                "county": county,
                "latitude": lat + np.random.uniform(-0.05, 0.05),
                "longitude": lon + np.random.uniform(-0.05, 0.05),
                "poverty_rate": round(pov, 2),
                "unemployment_rate": round(ue, 2),
                "food_desert_score": round(fd, 2),
                "snap_participation_rate": round(snap, 2),
                "median_income": int(np.random.uniform(28000, 95000)),
                "total_pop": int(np.random.uniform(3000, 45000)),
                "raw_risk_score": risk
            })
    return pd.DataFrame(rows)

def generate_partner_data():
    partners = [
        ("Second Harvest Food Bank","Orange","food_bank"),
        ("Heart of Florida United Way","Orange","nonprofit"),
        ("Community Food & Outreach Center","Seminole","food_bank"),
        ("Christian HELP","Seminole","faith_based"),
        ("Bread of Life Mission","Orange","faith_based"),
        ("Catholic Charities Central FL","Orange","faith_based"),
        ("Coalition for the Homeless","Orange","shelter"),
        ("Osceola Council on Aging","Osceola","senior_services"),
        ("St. Cloud Food Pantry","Osceola","food_pantry"),
        ("Harvest Time International","Lake","faith_based"),
        ("Mount Dora Community Trust","Lake","nonprofit"),
        ("Halifax Health Foundation","Volusia","health"),
        ("Daytona Beach Urban Ministries","Volusia","faith_based"),
        ("Early Learning Coalition","Seminole","child_services"),
    ]
    rows = []
    for pid, (name, county, org_type) in enumerate(partners, 1):
        budget = np.random.uniform(80000, 2500000)
        mpd = np.random.uniform(1.8, 6.5)
        meals = int(budget * mpd)
        pop = int(meals / np.random.uniform(180, 365))
        geo = round(np.random.uniform(0.4, 0.95), 2)
        eff = round((mpd/6.5)*0.40 + (pop/15000)*0.30 + geo*0.30, 4)
        tier = "Platinum" if eff > 0.7 else ("Gold" if eff > 0.5 else ("Silver" if eff > 0.3 else "Bronze"))
        rows.append({
            "partner_id": pid,
            "partner_name": name,
            "county": county,
            "org_type": org_type,
            "annual_budget_usd": int(budget),
            "annual_meals_delivered": meals,
            "meals_per_dollar": round(mpd, 3),
            "population_served": pop,
            "geographic_reach_score": geo,
            "composite_efficiency_score": eff,
            "efficiency_tier": tier,
            "year_established": np.random.randint(1975, 2015),
            "volunteers": np.random.randint(20, 500)
        })
    return pd.DataFrame(rows)

def build_database():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    print("Generating datasets...")
    census  = generate_census_data()
    usda    = generate_usda_data()
    bls     = generate_bls_data()
    fa      = generate_feeding_america_data()
    zips    = generate_zip_data()
    partners= generate_partner_data()

    census.to_sql("census_data",         conn, if_exists="replace", index=False)
    usda.to_sql("usda_data",             conn, if_exists="replace", index=False)
    bls.to_sql("bls_data",               conn, if_exists="replace", index=False)
    fa.to_sql("feeding_america_data",    conn, if_exists="replace", index=False)
    zips.to_sql("zip_data",              conn, if_exists="replace", index=False)
    partners.to_sql("partner_data",      conn, if_exists="replace", index=False)

    print("Creating views...")
    conn.executescript("""
    CREATE VIEW IF NOT EXISTS v_insecurity_trends AS
    SELECT a.county_name, a.year, a.insecurity_rate,
           LAG(a.insecurity_rate,1) OVER (PARTITION BY a.county_name ORDER BY a.year) AS prev_year_rate,
           ROUND(a.insecurity_rate - LAG(a.insecurity_rate,1) OVER (PARTITION BY a.county_name ORDER BY a.year),2) AS yoy_change
    FROM feeding_america_data a;

    CREATE VIEW IF NOT EXISTS v_partner_efficiency AS
    SELECT partner_id, partner_name, county, org_type,
           annual_meals_delivered, annual_budget_usd, meals_per_dollar,
           population_served, geographic_reach_score, composite_efficiency_score, efficiency_tier
    FROM partner_data;

    CREATE VIEW IF NOT EXISTS v_zip_risk AS
    SELECT zip_code, county, latitude, longitude, poverty_rate,
           unemployment_rate, food_desert_score, snap_participation_rate,
           median_income, total_pop, raw_risk_score,
           NTILE(4) OVER (ORDER BY raw_risk_score DESC) AS risk_quartile
    FROM zip_data;

    CREATE VIEW IF NOT EXISTS v_county_profile AS
    SELECT c.county_name, c.total_pop, c.poverty_rate, c.median_income,
           c.snap_participation_rate, u.food_desert_score,
           b_latest.unemployment_rate AS latest_unemployment,
           fa_latest.insecurity_rate AS latest_insecurity_rate,
           fa_latest.child_insecurity_rate, fa_latest.meal_gap_millions
    FROM census_data c
    LEFT JOIN usda_data u ON c.county_name = u.county_name
    LEFT JOIN (
        SELECT county_name, AVG(unemployment_rate) AS unemployment_rate
        FROM bls_data WHERE year=2023 GROUP BY county_name
    ) b_latest ON c.county_name = b_latest.county_name
    LEFT JOIN (
        SELECT * FROM feeding_america_data WHERE year=2023
    ) fa_latest ON c.county_name = fa_latest.county_name;

    CREATE VIEW IF NOT EXISTS v_unemployment_trends AS
    SELECT county_name, year, month, unemployment_rate,
           ROUND(unemployment_rate - LAG(unemployment_rate,1)
                 OVER (PARTITION BY county_name ORDER BY year, month),2) AS mom_change
    FROM bls_data;

    CREATE VIEW IF NOT EXISTS v_insecurity_ranking AS
    SELECT county_name, insecurity_rate,
           RANK() OVER (ORDER BY insecurity_rate DESC) AS insecurity_rank
    FROM feeding_america_data WHERE year=2023;
    """)
    conn.commit()

    # Export CSVs for Tableau
    os.makedirs("tableau_exports", exist_ok=True)
    pd.read_sql("SELECT * FROM v_county_profile", conn).to_csv("tableau_exports/county_profile.csv", index=False)
    pd.read_sql("SELECT * FROM v_partner_efficiency", conn).to_csv("tableau_exports/partner_efficiency.csv", index=False)
    pd.read_sql("SELECT * FROM v_zip_risk", conn).to_csv("tableau_exports/zip_risk.csv", index=False)
    pd.read_sql("SELECT * FROM v_insecurity_trends", conn).to_csv("tableau_exports/insecurity_trends.csv", index=False)
    pd.read_sql("SELECT * FROM v_unemployment_trends WHERE year=2023", conn).to_csv("tableau_exports/unemployment_2023.csv", index=False)

    conn.close()
    print(f"✅ Database built: {DB_PATH}")
    print(f"✅ CSVs exported to tableau_exports/")

if __name__ == "__main__":
    build_database()

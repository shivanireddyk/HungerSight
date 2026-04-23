"""
HungerSight Streamlit App
Live food insecurity risk lookup by Florida ZIP code
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import pickle
import json
import os
import plotly.express as px
import plotly.graph_objects as go

# ── Page Config ────────────────────────────────────────────────
st.set_page_config(
    page_title="HungerSight | Food Insecurity Intelligence",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ─────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1a3c5e 0%, #2d6a4f 100%);
        padding: 2rem; border-radius: 12px; margin-bottom: 2rem;
        text-align: center; color: white;
    }
    .metric-card {
        background: white; border-radius: 10px; padding: 1.2rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-left: 4px solid #2d6a4f;
        margin-bottom: 1rem;
    }
    .risk-critical { border-left-color: #d62828 !important; }
    .risk-high     { border-left-color: #f77f00 !important; }
    .risk-moderate { border-left-color: #fcbf49 !important; }
    .risk-low      { border-left-color: #2d6a4f !important; }
    .stSelectbox label { font-weight: 600; }
</style>
""", unsafe_allow_html=True)

DB_PATH = "data/hungersight.db"
MODEL_PATH = "data/models/risk_model.pkl"

@st.cache_resource
def load_model():
    if os.path.exists(MODEL_PATH):
        with open(MODEL_PATH, "rb") as f:
            return pickle.load(f)
    return None

@st.cache_data
def load_zip_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM v_zip_risk", conn)
    conn.close()
    return df

@st.cache_data
def load_partner_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM v_partner_efficiency", conn)
    conn.close()
    return df

@st.cache_data
def load_county_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM v_county_profile", conn)
    conn.close()
    return df

@st.cache_data
def load_trend_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM v_insecurity_trends", conn)
    conn.close()
    return df

def risk_color(score):
    if score >= 70: return "#d62828", "Critical", "risk-critical"
    if score >= 50: return "#f77f00", "High",     "risk-high"
    if score >= 30: return "#fcbf49", "Moderate", "risk-moderate"
    return "#2d6a4f", "Low", "risk-low"

# ── Header ─────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>🍽️ HungerSight</h1>
    <p style="font-size:1.1rem; margin:0; opacity:0.9;">
        Predictive Food Insecurity Intelligence Platform · Central Florida
    </p>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/3/3f/Placeholder_view_vector.svg/200px-Placeholder_view_vector.svg.png", width=60)
    st.markdown("### Navigation")
    page = st.radio("", ["🔍 ZIP Lookup","📊 County Dashboard","🤝 Partner Analytics","📈 Trends & Forecast"])
    st.markdown("---")
    st.markdown("**Data Sources**")
    st.markdown("• U.S. Census Bureau\n• USDA ERS\n• Bureau of Labor Statistics\n• Feeding America")
    st.markdown("---")
    st.markdown("*Built for U.S. Hunger · Longwood, FL*")

zip_df     = load_zip_data()
partner_df = load_partner_data()
county_df  = load_county_data()
trend_df   = load_trend_data()
model      = load_model()

# ══════════════════════════════════════════════════════════════
# PAGE 1 — ZIP LOOKUP
# ══════════════════════════════════════════════════════════════
if page == "🔍 ZIP Lookup":
    st.subheader("ZIP Code Food Insecurity Risk Lookup")
    col1, col2 = st.columns([1,2])

    with col1:
        if not zip_df.empty:
            zip_options = sorted(zip_df["zip_code"].astype(str).tolist())
        else:
            zip_options = ["32701","32703","32707","32750","32771","32801","32803"]

        selected_zip = st.selectbox("Select a Florida ZIP Code", zip_options)
        st.markdown("---")
        if not zip_df.empty and selected_zip in zip_df["zip_code"].astype(str).values:
            row = zip_df[zip_df["zip_code"].astype(str) == selected_zip].iloc[0]
            risk_score = row["raw_risk_score"]
            county = row["county"]
            color, label, css_class = risk_color(risk_score)
            county_avg = zip_df[zip_df["county"]==county]["raw_risk_score"].mean()

            st.markdown(f"""
            <div class="metric-card {css_class}">
                <h2 style="color:{color}; margin:0;">Risk Score: {risk_score:.1f}/100</h2>
                <h3 style="margin:4px 0; color:#333;">⚠️ {label} Risk</h3>
                <p style="color:#666; margin:0;">ZIP {selected_zip} · {county} County</p>
            </div>
            """, unsafe_allow_html=True)

            delta = risk_score - county_avg
            delta_str = f"+{delta:.1f}" if delta > 0 else f"{delta:.1f}"
            st.metric("vs. County Average", f"{county_avg:.1f}", delta_str, delta_color="inverse")
            st.metric("Poverty Rate",      f"{row['poverty_rate']:.1f}%")
            st.metric("Unemployment Rate", f"{row['unemployment_rate']:.1f}%")
            st.metric("Food Desert Score", f"{row['food_desert_score']:.1f}/10")
            st.metric("SNAP Participation",f"{row['snap_participation_rate']:.1f}%")

    with col2:
        if not zip_df.empty and selected_zip in zip_df["zip_code"].astype(str).values:
            row = zip_df[zip_df["zip_code"].astype(str) == selected_zip].iloc[0]

            # Feature contribution bar chart
            factors = {
                "Poverty Rate":      row["poverty_rate"] * 0.35,
                "Unemployment Rate": row["unemployment_rate"] * 0.25,
                "Food Desert Score": row["food_desert_score"] * 0.25,
                "SNAP Gap":          (100 - row["snap_participation_rate"]) * 0.15,
            }
            fig_factors = go.Figure(go.Bar(
                x=list(factors.values()),
                y=list(factors.keys()),
                orientation='h',
                marker_color=["#d62828","#f77f00","#fcbf49","#2d6a4f"]
            ))
            fig_factors.update_layout(
                title=f"Risk Factor Breakdown · ZIP {selected_zip}",
                xaxis_title="Contribution to Risk Score",
                height=300, margin=dict(l=0,r=0,t=40,b=0)
            )
            st.plotly_chart(fig_factors, use_container_width=True)

            # Nearby partners
            county = row["county"]
            st.markdown("**Nearby High-Efficiency Partners**")
            nearby = partner_df[partner_df["county"]==county].sort_values("composite_efficiency_score", ascending=False)
            if not nearby.empty:
                for _, p in nearby.head(3).iterrows():
                    tier_color = {"Platinum":"🏆","Gold":"🥇","Silver":"🥈","Bronze":"🥉"}.get(p["efficiency_tier"],"⭐")
                    st.markdown(f"{tier_color} **{p['partner_name']}** — {p['annual_meals_delivered']:,} meals/yr · Score: {p['composite_efficiency_score']:.2f}")

# ══════════════════════════════════════════════════════════════
# PAGE 2 — COUNTY DASHBOARD
# ══════════════════════════════════════════════════════════════
elif page == "📊 County Dashboard":
    st.subheader("Florida County Food Insecurity Dashboard")

    if not county_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Counties Tracked", len(county_df))
        col2.metric("Avg Insecurity Rate", f"{county_df['latest_insecurity_rate'].mean():.1f}%")
        col3.metric("Worst County Rate",   f"{county_df['latest_insecurity_rate'].max():.1f}%")
        col4.metric("Total Meal Gap (M)",  f"{county_df['meal_gap_millions'].sum():.1f}M")

        st.markdown("---")

        # Ranked bar chart
        top20 = county_df.nlargest(20, "latest_insecurity_rate")
        colors = ["#d62828" if r > 20 else ("#f77f00" if r > 16 else "#2d6a4f")
                  for r in top20["latest_insecurity_rate"]]
        fig = go.Figure(go.Bar(
            x=top20["latest_insecurity_rate"],
            y=top20["county_name"],
            orientation="h",
            marker_color=colors,
            text=top20["latest_insecurity_rate"].round(1).astype(str) + "%",
            textposition="outside"
        ))
        fig.update_layout(title="Top 20 Counties by Food Insecurity Rate (2023)",
                          xaxis_title="Insecurity Rate (%)", height=500,
                          margin=dict(l=0,r=60,t=40,b=0))
        st.plotly_chart(fig, use_container_width=True)

        # Scatter: poverty vs insecurity
        fig2 = px.scatter(county_df, x="poverty_rate", y="latest_insecurity_rate",
                          size="meal_gap_millions", color="latest_insecurity_rate",
                          hover_name="county_name", color_continuous_scale="RdYlGn_r",
                          title="Poverty Rate vs Food Insecurity Rate (bubble = meal gap size)",
                          labels={"poverty_rate":"Poverty Rate (%)","latest_insecurity_rate":"Insecurity Rate (%)"})
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════════
# PAGE 3 — PARTNER ANALYTICS
# ══════════════════════════════════════════════════════════════
elif page == "🤝 Partner Analytics":
    st.subheader("Partner Organization Impact Leaderboard")

    if not partner_df.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Partners", len(partner_df))
        col2.metric("Total Meals Delivered", f"{partner_df['annual_meals_delivered'].sum():,.0f}")
        col3.metric("Avg Meals per Dollar", f"{partner_df['meals_per_dollar'].mean():.2f}")

        fig = go.Figure()
        colors = {"Platinum":"#a8dadc","Gold":"#f1c40f","Silver":"#bdc3c7","Bronze":"#e67e22"}
        for tier in ["Platinum","Gold","Silver","Bronze"]:
            sub = partner_df[partner_df["efficiency_tier"]==tier]
            fig.add_trace(go.Bar(
                x=sub["partner_name"], y=sub["composite_efficiency_score"],
                name=tier, marker_color=colors[tier]
            ))
        fig.update_layout(title="Partner Efficiency Scores by Tier",
                          yaxis_title="Composite Efficiency Score",
                          xaxis_tickangle=-35, barmode="group", height=450)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Full Partner Table")
        display_cols = ["partner_name","county","efficiency_tier","annual_meals_delivered",
                        "meals_per_dollar","population_served","composite_efficiency_score"]
        styled = partner_df[display_cols].sort_values("composite_efficiency_score", ascending=False)
        st.dataframe(styled, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════
# PAGE 4 — TRENDS
# ══════════════════════════════════════════════════════════════
elif page == "📈 Trends & Forecast":
    st.subheader("Food Insecurity Trends & Forecasting")

    if not trend_df.empty:
        counties_of_interest = ["Seminole","Orange","Osceola","Lake","Volusia"]
        sub = trend_df[trend_df["county_name"].isin(counties_of_interest)]

        fig = px.line(sub, x="year", y="insecurity_rate", color="county_name",
                      title="Food Insecurity Rate Over Time — Central Florida Counties",
                      labels={"insecurity_rate":"Insecurity Rate (%)","year":"Year","county_name":"County"},
                      markers=True)
        fig.add_vline(x=2020, line_dash="dash", line_color="red",
                      annotation_text="COVID-19", annotation_position="top right")
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

        # Simple 2-year forecast
        st.markdown("### 2-Year Linear Forecast")
        forecast_rows = []
        for county in counties_of_interest:
            cdf = sub[sub["county_name"]==county].dropna().sort_values("year")
            if len(cdf) >= 3:
                slope = np.polyfit(cdf["year"], cdf["insecurity_rate"], 1)[0]
                latest_rate = cdf.iloc[-1]["insecurity_rate"]
                forecast_rows.append({
                    "County": county,
                    "2023 Rate": f"{latest_rate:.1f}%",
                    "2024 Forecast": f"{max(0, latest_rate + slope):.1f}%",
                    "2025 Forecast": f"{max(0, latest_rate + 2*slope):.1f}%",
                    "Trajectory": "📈 Worsening" if slope > 0.3 else ("📉 Improving" if slope < -0.3 else "➡️ Stable")
                })
        st.dataframe(pd.DataFrame(forecast_rows), use_container_width=True, hide_index=True)

# ── Footer ─────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<p style='text-align:center; color:#999; font-size:0.85rem;'>"
    "HungerSight · Built by Shivani Krishnama · Data: U.S. Census Bureau, USDA ERS, BLS, Feeding America"
    "</p>",
    unsafe_allow_html=True
)

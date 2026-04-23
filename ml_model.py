"""
HungerSight ML Model
Trains Random Forest to predict food insecurity risk scores by ZIP code
Also computes Partner Efficiency composite scores
"""

import sqlite3
import pandas as pd
import numpy as np
import pickle
import os
import json
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
import warnings
warnings.filterwarnings("ignore")

DB_PATH = "data/hungersight.db"
MODEL_DIR = "data/models"

def load_training_data(conn):
    query = """
    SELECT
        z.zip_code,
        z.county,
        z.poverty_rate,
        z.unemployment_rate,
        z.food_desert_score,
        z.snap_participation_rate,
        z.median_income,
        z.total_pop,
        z.raw_risk_score AS risk_score
    FROM zip_data z
    """
    return pd.read_sql(query, conn)

def train_risk_model(df):
    features = ["poverty_rate","unemployment_rate","food_desert_score",
                "snap_participation_rate","median_income","total_pop"]
    X = df[features]
    y = df["risk_score"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=200, max_depth=8, min_samples_leaf=2, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae  = round(mean_absolute_error(y_test, y_pred), 3)
    r2   = round(r2_score(y_test, y_pred), 4)
    cv   = cross_val_score(model, X, y, cv=5, scoring="r2")

    importance = dict(zip(features, [round(v, 4) for v in model.feature_importances_]))

    metrics = {
        "model": "RandomForestRegressor",
        "n_estimators": 200,
        "test_mae": mae,
        "test_r2": r2,
        "cv_r2_mean": round(cv.mean(), 4),
        "cv_r2_std": round(cv.std(), 4),
        "feature_importances": importance,
        "training_samples": len(X_train),
        "test_samples": len(X_test)
    }
    print(f"  Risk Model → MAE: {mae} | R²: {r2} | CV R²: {cv.mean():.4f} ± {cv.std():.4f}")
    return model, metrics, features

def score_all_zips(model, features, conn):
    df = pd.read_sql("SELECT * FROM zip_data", conn)
    df["predicted_risk_score"] = model.predict(df[features]).round(2)
    df["risk_label"] = pd.cut(df["predicted_risk_score"],
                               bins=[0,30,50,70,100],
                               labels=["Low","Moderate","High","Critical"])
    return df

def save_outputs(model, metrics, scored_df):
    os.makedirs(MODEL_DIR, exist_ok=True)
    with open(f"{MODEL_DIR}/risk_model.pkl","wb") as f:
        pickle.dump(model, f)
    with open(f"{MODEL_DIR}/model_metrics.json","w") as f:
        json.dump(metrics, f, indent=2)
    scored_df.to_csv("data/zip_predictions.csv", index=False)
    scored_df.to_csv("tableau_exports/zip_predictions.csv", index=False)
    print(f"  Model saved to {MODEL_DIR}/risk_model.pkl")
    print(f"  Predictions saved to data/zip_predictions.csv")

def main():
    if not os.path.exists(DB_PATH):
        print("Database not found. Run etl_pipeline.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    print("Training food insecurity risk model...")
    df = load_training_data(conn)
    model, metrics, features = train_risk_model(df)
    scored = score_all_zips(model, features, conn)
    save_outputs(model, metrics, scored)
    conn.close()
    print("✅ Model training complete!")
    print(json.dumps(metrics["feature_importances"], indent=2))

if __name__ == "__main__":
    main()

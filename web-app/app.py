import sys
import os
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flask import Flask, request, jsonify, render_template
import numpy as np
import pandas as pd
import joblib

app = Flask(__name__)

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load manifest (metadata + effects for all models)
with open(os.path.join(BASE, "models", "manifest.json")) as f:
    MANIFEST = json.load(f)

# Lazy-load per-model joblib files to avoid loading 1400+ at startup
_model_cache = {}

def get_per_model(car_model: str):
    if car_model in _model_cache:
        return _model_cache[car_model]
    meta = MANIFEST.get(car_model)
    if not meta or not meta.get("model_file"):
        return None
    path = os.path.join(BASE, meta["model_file"])
    if not os.path.exists(path):
        return None
    mdl = joblib.load(path)
    _model_cache[car_model] = mdl
    return mdl

df = pd.read_csv(os.path.join(BASE, "data", "processed", "car_auction_data.csv"))
df = df[df["price"] > 0].copy()
df["auction_year"] = pd.to_numeric(df["auction_year"], errors="coerce")

# Load URL lookup from DB
import sqlite3 as _sqlite3
_conn = _sqlite3.connect(os.path.join(BASE, "data", "listings.db"))
_urls = pd.read_sql_query("SELECT id, url FROM listings WHERE url IS NOT NULL", _conn)
_conn.close()
df = df.merge(_urls, on="id", how="left")

FEATURES = ["year", "mileage", "is_manual", "auction_year"]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/models")
def get_models():
    models = sorted(df["model"].unique().tolist())
    return jsonify(models)


@app.route("/api/predict", methods=["POST"])
def predict():
    data = request.json
    car_model = data["model"]
    year = int(data["year"])
    mileage = int(data["mileage"])
    is_manual = int(data["is_manual"])
    auction_year = int(data.get("auction_year", 2025))

    meta = MANIFEST.get(car_model)

    # Mean-only fallback
    if not meta or meta["type"] == "mean_only":
        price = meta["median_price"] if meta else float(df["price"].median())
        return jsonify({
            "price": round(price, 2),
            "type": "mean_only",
            "n": meta["n"] if meta else 0,
            "effects": {},
        })

    mdl = get_per_model(car_model)
    if mdl is None:
        price = meta["median_price"]
        return jsonify({"price": round(price, 2), "type": "fallback", "n": meta["n"], "effects": {}})

    X = pd.DataFrame([{
        "year": year,
        "mileage": mileage,
        "is_manual": is_manual,
        "auction_year": auction_year,
    }])[FEATURES].astype(float)

    pred_log = mdl.predict(X)[0]
    price = float(np.expm1(pred_log))

    return jsonify({
        "price": round(price, 2),
        "type": meta["type"],
        "n": meta["n"],
        "r2": meta["r2"],
        "effects": meta["effects"],
    })


@app.route("/api/model-stats/<path:car_model>")
def model_stats(car_model):
    subset = df[df["model"] == car_model]
    if subset.empty:
        return jsonify({"error": "Model not found"}), 404

    meta = MANIFEST.get(car_model, {})

    # Price vs auction date — one point per sale, sorted by date
    auction_points = (
        subset[["auction_year", "auction_month", "price", "year", "mileage", "transmission", "url"]]
        .dropna(subset=["auction_year", "auction_month", "price"])
        .copy()
    )
    auction_points["date_val"] = (
        auction_points["auction_year"].astype(int).astype(str) + "-" +
        auction_points["auction_month"].astype(str).str.zfill(2)
    )
    auction_points = auction_points.sort_values("date_val")

    return jsonify({
        "count": len(subset),
        "median": float(subset["price"].median()),
        "mean": float(subset["price"].mean()),
        "min": float(subset["price"].min()),
        "max": float(subset["price"].max()),
        "min_year": int(subset["year"].min()),
        "r2": meta.get("r2"),
        "model_type": meta.get("type"),
        "effects": meta.get("effects", {}),
        "auction_scatter": [
            {
                "date": row["date_val"],
                "price": row["price"],
                "car_year": int(row["year"]) if pd.notna(row["year"]) else None,
                "mileage": int(row["mileage"]) if pd.notna(row["mileage"]) else None,
                "transmission": row["transmission"] if pd.notna(row["transmission"]) else None,
                "url": row["url"] if pd.notna(row.get("url")) else None,
            }
            for _, row in auction_points.iterrows()
        ],
    })


@app.route("/api/top-models")
def top_models():
    stats = (
        df.groupby("model")["price"]
        .agg(["median", "count"])
        .reset_index()
        .rename(columns={"median": "median_price", "count": "sales"})
    )
    top = stats.nlargest(15, "median_price")
    return jsonify(top.to_dict(orient="records"))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

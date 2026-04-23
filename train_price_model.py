"""
Per-model car price prediction.

For each make/model, trains a regression on that model's own auction history.
Features: year, mileage, is_manual, auction_year
Target:   log(price)

Model selection by sample count:
  >= 30 samples : XGBoost (cross-validated)
   5-29 samples : Ridge regression
   < 5  samples : mean-only baseline (stored as scalar)

Saves:
  models/per_model/<safe_name>.joblib   — fitted model per car model
  models/manifest.json                  — metadata, sample counts, R², factor effects
"""

import json
import os
import re
import warnings
import numpy as np
import pandas as pd
import joblib
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.model_selection import cross_val_predict, KFold
from xgboost import XGBRegressor

warnings.filterwarnings("ignore")

DATA_PATH  = "data/processed/car_auction_data.csv"
MODEL_DIR  = "models/per_model"
MANIFEST   = "models/manifest.json"
FEATURES   = ["year", "mileage", "is_manual", "auction_year"]
TARGET     = "price"
MIN_SAMPLES_RIDGE   = 5
MIN_SAMPLES_XGBOOST = 30


def safe_name(model_name: str) -> str:
    return re.sub(r"[^\w\-]", "_", model_name)[:80]


def factor_effects(model, feature_names: list, scaler=None) -> dict:
    """
    Extract per-feature effect as % price change per unit increase.
    For Ridge: use scaled coefficients (in log space → % change).
    For XGBoost: use feature_importances_ (relative, not directional).
    """
    effects = {}
    if hasattr(model, "coef_"):
        coefs = model.coef_
        for name, coef in zip(feature_names, coefs):
            effects[name] = round(float(coef), 6)
    elif hasattr(model, "feature_importances_"):
        for name, imp in zip(feature_names, model.feature_importances_):
            effects[name] = round(float(imp), 6)
    return effects


def train_model(X: pd.DataFrame, y: pd.Series, n: int):
    """Train the best model for a given sample size. Returns (model, scaler_or_None)."""
    if n >= MIN_SAMPLES_XGBOOST:
        mdl = XGBRegressor(
            n_estimators=200,
            learning_rate=0.05,
            max_depth=4,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=3,
            reg_lambda=2.0,
            random_state=42,
            n_jobs=-1,
        )
        mdl.fit(X, y, verbose=False)
        return mdl, None
    else:
        pipe = Pipeline([
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=10.0)),
        ])
        pipe.fit(X, y)
        return pipe, None


def evaluate_model(model, X: pd.DataFrame, y: pd.Series, n: int) -> dict:
    """Cross-validated R² and RMSE (or leave-one-out for tiny groups)."""
    if n < 4:
        preds = np.full(n, y.mean())
    elif n < MIN_SAMPLES_XGBOOST:
        cv = min(n, 5)
        preds = cross_val_predict(model, X, y, cv=cv)
    else:
        cv = KFold(n_splits=5, shuffle=True, random_state=42)
        preds = cross_val_predict(model, X, y, cv=cv)

    preds_price = np.expm1(preds)
    actuals_price = np.expm1(y)
    rmse = float(np.sqrt(mean_squared_error(actuals_price, preds_price)))
    r2   = float(r2_score(y, preds))
    return {"r2": round(r2, 4), "rmse": round(rmse, 2)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    os.makedirs(MODEL_DIR, exist_ok=True)

    print("Loading data...")
    df = pd.read_csv(DATA_PATH)
    df = df[df[TARGET] > 0].copy()
    df["log_price"] = np.log1p(df[TARGET])
    df["auction_year"] = pd.to_numeric(df["auction_year"], errors="coerce")
    df = df.dropna(subset=FEATURES + ["log_price"])
    print(f"  {len(df):,} rows, {df['model'].nunique():,} unique models")

    manifest = {}
    models_trained = 0
    skipped = 0

    groups = df.groupby("model")
    total = len(groups)

    for i, (car_model, group) in enumerate(groups, 1):
        n = len(group)
        X = group[FEATURES].astype(float).reset_index(drop=True)
        y = group["log_price"].reset_index(drop=True)

        fname = safe_name(car_model)
        model_path = os.path.join(MODEL_DIR, f"{fname}.joblib")

        if n < MIN_SAMPLES_RIDGE:
            # Too few — store mean price only
            mean_price = float(group[TARGET].mean())
            median_price = float(group[TARGET].median())
            manifest[car_model] = {
                "n": n, "type": "mean_only",
                "mean_price": round(mean_price, 2),
                "median_price": round(median_price, 2),
                "r2": None, "rmse": None,
                "effects": {},
                "model_file": None,
            }
            skipped += 1
            continue

        model, _ = train_model(X, y, n)
        metrics = evaluate_model(model, X, y, n)
        joblib.dump(model, model_path)

        # Feature effects
        if n >= MIN_SAMPLES_XGBOOST:
            effects = factor_effects(model, FEATURES)
            model_type = "xgboost"
        else:
            ridge = model.named_steps["ridge"]
            scaler = model.named_steps["scaler"]
            # Coefs in log-price/scaled-unit space — convert to log-price/raw-unit
            raw_coefs = ridge.coef_ / scaler.scale_
            effects = {f: round(float(c), 6) for f, c in zip(FEATURES, raw_coefs)}
            model_type = "ridge"

        manifest[car_model] = {
            "n": n,
            "type": model_type,
            "mean_price": round(float(group[TARGET].mean()), 2),
            "median_price": round(float(group[TARGET].median()), 2),
            "r2": metrics["r2"],
            "rmse": metrics["rmse"],
            "effects": effects,
            "model_file": os.path.join(MODEL_DIR, f"{fname}.joblib"),
        }
        models_trained += 1

        if i % 100 == 0 or i == total:
            print(f"  [{i}/{total}] trained={models_trained} skipped={skipped}")

    with open(MANIFEST, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nDone.")
    print(f"  Models trained : {models_trained}")
    print(f"  Mean-only      : {skipped}")
    print(f"  Manifest saved : {MANIFEST}")

    # Summary stats
    trained = [v for v in manifest.values() if v["type"] != "mean_only"]
    r2s = [v["r2"] for v in trained if v["r2"] is not None]
    print(f"\nAmong trained models:")
    print(f"  Median R²  : {np.median(r2s):.3f}")
    print(f"  Mean R²    : {np.mean(r2s):.3f}")

    # Show a few interesting models
    print("\nSample — large models (XGBoost):")
    xgb = sorted([v for v in manifest.values() if v["type"] == "xgboost"],
                 key=lambda x: -x["n"])[:5]
    for v in xgb:
        name = [k for k,val in manifest.items() if val is v][0]
        print(f"  {name:<45} n={v['n']:>4}  R²={v['r2']:.3f}  effects={v['effects']}")

    print("\nSample — Porsche 918 Spyder:")
    if "Porsche 918 Spyder" in manifest:
        v = manifest["Porsche 918 Spyder"]
        print(f"  n={v['n']}  type={v['type']}  R²={v['r2']}  median=${v['median_price']:,.0f}")
        print(f"  effects: {v['effects']}")

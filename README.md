# BaT Car Price Predictor

Predicts auction sale prices for collector cars on [Bring a Trailer](https://bringatrailer.com) using historical sold results. Trains a separate ML model per car make/model and serves predictions through a Flask web app.

## How it works

1. **Build dataset** — merges the SQLite auction database with supplementary JSON bid/listing data, fills missing fields via regex parsing of listing titles and descriptions, and writes a clean CSV.
2. **Train models** — fits a per-model regressor for each of the ~1,683 car models in the dataset, with model complexity scaled to sample size.
3. **Serve predictions** — a Flask app loads the trained models on demand and exposes a UI where you can look up any model, explore its auction history, and get a price estimate.

## Project structure

```
bat/
├── build_dataset.py          # Merge + clean raw data → CSV
├── train_price_model.py      # Train per-model regressors → joblib + manifest
├── data/
│   ├── listings.db           # SQLite: 107k auction records
│   ├── total.json            # Supplementary bid history + listing details
│   └── processed/
│       └── car_auction_data.csv
├── models/
│   ├── manifest.json         # Metadata, R², feature effects for all models
│   └── per_model/            # One .joblib file per trained model
└── web-app/
    ├── app.py                # Flask server
    └── templates/
        └── index.html        # Single-page UI
```

## ML approach

Features: `year`, `mileage`, `is_manual`, `auction_year`  
Target: `log(price)` (predictions exponentiated back to dollars)

Model selection by sample count per car model:

| Samples | Model |
|---------|-------|
| ≥ 30 | XGBoost |
| 5–29 | Ridge regression (StandardScaler pipeline) |
| < 5 | Historical mean/median only |

## Running

**Build the dataset:**
```bash
python build_dataset.py
```

**Train models:**
```bash
python train_price_model.py
```

**Start the web app:**
```bash
cd web-app
python app.py
# Open http://localhost:5000
```

## Web app

- Search any make/model with typeahead autocomplete
- See auction history in a scatter chart (click any dot to open the original BaT listing)
- Enter year, mileage, and transmission to get a predicted auction price
- Prediction overlaid as a star on the historical chart for context

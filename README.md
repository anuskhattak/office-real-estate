# Office Real Estate — ETL Pipeline

An end-to-end data engineering project that cleans, transforms, and loads real estate data into a PostgreSQL database using Apache Airflow.

---

## Project Structure

```
office-real-estate/
├── dags/                        # Airflow DAG
│   └── real_estate_etl_dag.py
├── data/
│   ├── raw/                     # Original CSV files
│   └── cleaned/                 # Cleaned CSV files
├── notebooks/                   # Jupyter cleaning notebooks (one per table)
│   ├── agencies.ipynb
│   ├── agents.ipynb
│   ├── buyers.ipynb
│   ├── listings.ipynb
│   ├── locations.ipynb
│   ├── properties.ipynb
│   ├── property_features.ipynb
│   └── transactions.ipynb
├── sql/
│   └── schema.sql               # PostgreSQL schema
├── main.ipynb                   # Project overview notebook
└── requirements.txt
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python / Pandas | Data cleaning |
| Jupyter Notebooks | Exploratory cleaning (one per CSV) |
| Apache Airflow | ETL pipeline orchestration |
| PostgreSQL 17 | Data storage |
| pgAdmin 4 | Database management |

---

## Database Schema

The pipeline loads data into 8 tables inside the `real_estate_db` PostgreSQL database:

- `agencies`
- `agents`
- `buyers`
- `listings`
- `locations`
- `properties`
- `property_features`
- `transactions`

---

## How It Works

1. **Extract** — Raw CSV files are stored in `data/raw/`
2. **Transform** — Each CSV is cleaned in its own Jupyter notebook (nulls handled, types fixed, duplicates removed), output saved to `data/cleaned/`
3. **Load** — Airflow DAG (`real_estate_etl_dag.py`) truncates existing tables and loads the cleaned CSVs into PostgreSQL

---

## Running the Pipeline

1. Start PostgreSQL and Airflow
2. Open Airflow UI at `http://localhost:8080`
3. Trigger the `real_estate_etl_pipeline` DAG manually
4. All 8 tables will be loaded in ~12 seconds

---

## Requirements

```
pip install -r requirements.txt
```

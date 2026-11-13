"""Configuracion compartida: rutas, conexiones y funciones chicas que usan todas las etapas."""
import os
from pathlib import Path

import duckdb
import psutil
import yaml

# --- Rutas -------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
QUARANTINE_DIR = DATA_DIR / "quarantine"
CURATED_DIR = DATA_DIR / "curated"
REPORTS_DIR = DATA_DIR / "reports"
DB_PATH = DATA_DIR / "nyc_taxi.duckdb"
CONTRACT_PATH = BASE_DIR / "contracts" / "yellow_schema.yaml"
RULES_PATH = BASE_DIR / "contracts" / "reglas_calidad.yaml"
SQL_DIR = BASE_DIR / "sql"
DBT_DIR = BASE_DIR / "dbt"
RESULTS_PATH = BASE_DIR / "RESULTADOS.md"
ZONE_LOOKUP_PATH = RAW_DIR / "taxi_zone_lookup.csv"

# --- Postgres (los valores reales llegan por variables de entorno desde docker-compose)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5434"))
PG_USER = os.getenv("PG_USER", "taxi")
PG_PASSWORD = os.getenv("PG_PASSWORD", "taxi")
PG_DB = os.getenv("PG_DB", "taxi")


def ensure_dirs():
    for d in [RAW_DIR, QUARANTINE_DIR, CURATED_DIR, REPORTS_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def raw_path(year: str, month: str, periodo: str, raw_dir=None) -> Path:
    """Ruta del Parquet crudo de un mes: data/raw/yellow/year=AAAA/month=MM/..."""
    raw_dir = raw_dir or RAW_DIR
    return raw_dir / "yellow" / f"year={year}" / f"month={month}" / f"yellow_tripdata_{periodo}.parquet"


def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_contract(path=CONTRACT_PATH):
    return load_yaml(path)


def load_rules(path=RULES_PATH):
    return load_yaml(path)["reglas"]


def contract_clean_columns(contract=None):
    """Nombres snake_case de las columnas del contrato."""
    contract = contract or load_contract()
    return [c["clean_name"] for c in contract["columns"]]


def get_db_connection(db_path=None):
    """Conexion a DuckDB con tope de memoria.

    3 GB de tope: deja lugar en una notebook de 8 GB para Postgres, Dagster y
    Grafana, que corren al mismo tiempo. Si DuckDB necesita mas, escribe a disco.
    Los tests pasan db_path para usar una base temporal y no tocar la real.
    """
    if db_path is None:
        ensure_dirs()
    con = duckdb.connect(str(db_path or DB_PATH))
    con.execute("SET memory_limit='3GB';")
    con.execute("SET threads TO 4;")
    return con


def postgres_url() -> str:
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"


def get_comparison_month(mes: str) -> str:
    """'2025-03' -> '2024-03' (mismo mes, anio anterior)."""
    anio, m = mes.split("-")
    return f"{int(anio) - 1}-{m}"


def get_target_months(mes: str):
    """'2025-03' -> [('2025', '03', '2025-03'), ('2024', '03', '2024-03')]."""
    anio, m = mes.split("-")
    anterior = get_comparison_month(mes)
    return [(anio, m, mes), (anterior[:4], m, anterior)]


def log_memory(etapa: str):
    mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    print(f"[{etapa}] RAM del proceso: {mb:.1f} MB")
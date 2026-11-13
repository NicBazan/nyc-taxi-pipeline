"""Datos sinteticos para los tests: no descargan nada ni tocan data/.

Cada test trabaja en una carpeta temporal (tmp_path de pytest) con su propia
base DuckDB, sus propios Parquet "crudos" y su propia tabla de zonas.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import raw_path  # noqa: E402

# Columnas y tipos como los publica la TLC (nombres originales, sin normalizar)
COLUMNAS_TLC = [
    ("VendorID", "INTEGER"), ("tpep_pickup_datetime", "TIMESTAMP"), ("tpep_dropoff_datetime", "TIMESTAMP"),
    ("passenger_count", "BIGINT"), ("trip_distance", "DOUBLE"), ("RatecodeID", "BIGINT"),
    ("store_and_fwd_flag", "VARCHAR"), ("PULocationID", "INTEGER"), ("DOLocationID", "INTEGER"),
    ("payment_type", "BIGINT"), ("fare_amount", "DOUBLE"), ("extra", "DOUBLE"), ("mta_tax", "DOUBLE"),
    ("tip_amount", "DOUBLE"), ("tolls_amount", "DOUBLE"), ("improvement_surcharge", "DOUBLE"),
    ("total_amount", "DOUBLE"), ("congestion_surcharge", "DOUBLE"), ("Airport_fee", "DOUBLE"),
    ("cbd_congestion_fee", "DOUBLE"),
]


def fila(**cambios):
    """Un viaje valido de 2 millas y 10 minutos el 10/01/2030; se pisan los campos pedidos."""
    subida = datetime(2030, 1, 10, 12, 0, 0)
    base = {
        "VendorID": 2, "tpep_pickup_datetime": subida, "tpep_dropoff_datetime": subida + timedelta(minutes=10),
        "passenger_count": 1, "trip_distance": 2.0, "RatecodeID": 1, "store_and_fwd_flag": "N",
        "PULocationID": 1, "DOLocationID": 2, "payment_type": 1, "fare_amount": 12.0, "extra": 0.0,
        "mta_tax": 0.5, "tip_amount": 2.0, "tolls_amount": 0.0, "improvement_surcharge": 1.0,
        "total_amount": 15.5, "congestion_surcharge": 0.0, "Airport_fee": 0.0, "cbd_congestion_fee": 0.75,
    }
    base.update(cambios)
    return base


def escribir_parquet(path: Path, filas: list, sin_columnas=()) -> Path:
    """Escribe las filas en un Parquet con los tipos de la TLC."""
    path.parent.mkdir(parents=True, exist_ok=True)
    columnas = [(n, t) for n, t in COLUMNAS_TLC if n not in sin_columnas]
    con = duckdb.connect()
    con.execute("CREATE TABLE t (" + ", ".join(f'"{n}" {t}' for n, t in columnas) + ")")
    con.executemany(f"INSERT INTO t VALUES ({', '.join('?' * len(columnas))})",
                    [[f[n] for n, _ in columnas] for f in filas])
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()
    return path


@pytest.fixture
def entorno(tmp_path):
    """Carpeta temporal con base DuckDB, carpeta raw y tabla de zonas chiquita."""
    raw_dir = tmp_path / "raw"
    zonas = raw_dir / "taxi_zone_lookup.csv"
    zonas.parent.mkdir(parents=True)
    zonas.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,EWR,Newark Airport,EWR\n"
        "2,Queens,Jamaica Bay,Boro Zone\n"
        "3,Bronx,Allerton/Pelham Gardens,Boro Zone\n"
        "264,Unknown,N/A,N/A\n"
        "265,N/A,Outside of NYC,N/A\n",
        encoding="utf-8",
    )
    con = duckdb.connect(str(tmp_path / "test.duckdb"))
    yield {"tmp": tmp_path, "raw_dir": raw_dir, "zonas": zonas, "con": con,
           "reports": tmp_path / "reports", "quarantine": tmp_path / "quarantine"}
    con.close()


def parquet_del_mes(raw_dir: Path, periodo: str) -> Path:
    year, month = periodo.split("-")
    return raw_path(year, month, periodo, raw_dir)
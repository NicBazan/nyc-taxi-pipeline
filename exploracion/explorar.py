"""Etapa 0: exploracion de los Parquet crudos con DuckDB.

No modifica nada: solo lee los archivos de data/raw/ y muestra numeros.
Sirve para justificar umbrales y severidades de las reglas de calidad.

Uso:  python exploracion/explorar.py --mes 2025-03
"""

import argparse
import sys
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data" / "raw"


def archivo(periodo: str) -> str:
    anio, mes = periodo.split("-")
    return (RAW / "yellow" / f"year={anio}" / f"month={mes}" / f"yellow_tripdata_{periodo}.parquet").as_posix()


def mostrar(con, titulo: str, sql: str) -> None:
    print(f"\n### {titulo}")
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    filas = cur.fetchall()
    anchos = [max(len(str(c)), *(len(f"{v:,}" if isinstance(v, int) else str(v)) for v in col)) if filas else len(str(c))
              for c, col in zip(cols, zip(*filas) if filas else [[]] * len(cols))]
    fmt = lambda v: f"{v:,}" if isinstance(v, int) else str(v)
    print(" | ".join(str(c).ljust(a) for c, a in zip(cols, anchos)))
    print("-+-".join("-" * a for a in anchos))
    for f in filas:
        print(" | ".join(fmt(v).ljust(a) for v, a in zip(f, anchos)))


def explorar(con, periodo: str) -> None:
    anio, mes = (int(x) for x in periodo.split("-"))
    con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{archivo(periodo)}', hive_partitioning=false)")
    print(f"\n================ {periodo} ================")

    mostrar(con, "Esquema (columnas y tipos)", "SELECT column_name, column_type FROM (DESCRIBE t)")

    mostrar(con, "Nulos por columna", """
        UNPIVOT (SELECT COUNT(*) - COUNT(COLUMNS(*)) FROM t)
        ON COLUMNS(*) INTO NAME columna VALUE nulos""")

    zonas = (RAW / "taxi_zone_lookup.csv").as_posix()
    mostrar(con, "Cuantas filas tocaria cada regla candidata", f"""
        WITH z AS (SELECT LocationID AS id FROM read_csv_auto('{zonas}')),
        b AS (SELECT *, epoch(tpep_dropoff_datetime - tpep_pickup_datetime) / 60.0 AS dur_min FROM t)
        SELECT regla, filas, ROUND(filas * 100.0 / (SELECT COUNT(*) FROM t), 3) AS pct FROM (
          SELECT 'TOTAL filas' AS regla, COUNT(*) AS filas FROM t
          UNION ALL SELECT 'DQ01 subida fuera del mes', COUNT(*) FROM t
              WHERE year(tpep_pickup_datetime) <> {anio} OR month(tpep_pickup_datetime) <> {mes}
          UNION ALL SELECT 'DQ02 bajada <= subida', COUNT(*) FROM t WHERE tpep_dropoff_datetime <= tpep_pickup_datetime
          UNION ALL SELECT 'DQ03a distancia <= 0', COUNT(*) FROM t WHERE trip_distance <= 0
          UNION ALL SELECT 'DQ03b distancia > 100', COUNT(*) FROM t WHERE trip_distance > 100
          UNION ALL SELECT 'DQ03c distancia > 200', COUNT(*) FROM t WHERE trip_distance > 200
          UNION ALL SELECT 'DQ04 total_amount < 0', COUNT(*) FROM t WHERE total_amount < 0
          UNION ALL SELECT '    total_amount = 0', COUNT(*) FROM t WHERE total_amount = 0
          UNION ALL SELECT 'DQ05a pasajeros NULL', COUNT(*) FROM t WHERE passenger_count IS NULL
          UNION ALL SELECT 'DQ05b pasajeros = 0', COUNT(*) FROM t WHERE passenger_count = 0
          UNION ALL SELECT 'DQ06 zona no existe en lookup', COUNT(*) FROM t
              WHERE PULocationID NOT IN (SELECT id FROM z) OR DOLocationID NOT IN (SELECT id FROM z)
          UNION ALL SELECT '    zona 264/265 (Unknown/Outside)', COUNT(*) FROM t
              WHERE PULocationID IN (264, 265) OR DOLocationID IN (264, 265)
          UNION ALL SELECT 'DQ07 filas duplicadas exactas (sobrantes)', COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t)) FROM t
          UNION ALL SELECT 'DQ08a duracion > 12 h', COUNT(*) FROM b WHERE dur_min > 720
          UNION ALL SELECT 'DQ08b velocidad > 85 mph', COUNT(*) FROM b WHERE dur_min > 0 AND trip_distance / (dur_min / 60) > 85
          UNION ALL SELECT 'DQ08c velocidad > 60 mph', COUNT(*) FROM b WHERE dur_min > 0 AND trip_distance / (dur_min / 60) > 60
          UNION ALL SELECT 'extra: propina > tarifa (tarifa > 10)', COUNT(*) FROM t WHERE tip_amount > fare_amount AND fare_amount > 10
          UNION ALL SELECT 'extra: fare_amount < 0', COUNT(*) FROM t WHERE fare_amount < 0
        )""")

    mostrar(con, "Distribucion de distancia (millas)", """
        SELECT ROUND(quantile_cont(trip_distance, 0.5), 2) AS p50, ROUND(quantile_cont(trip_distance, 0.99), 2) AS p99,
               ROUND(quantile_cont(trip_distance, 0.9999), 2) AS p9999, MAX(trip_distance) AS maximo FROM t""")

    mostrar(con, "Distribucion de duracion (minutos)", """
        WITH b AS (SELECT epoch(tpep_dropoff_datetime - tpep_pickup_datetime) / 60.0 AS d FROM t)
        SELECT ROUND(quantile_cont(d, 0.5), 1) AS p50, ROUND(quantile_cont(d, 0.99), 1) AS p99,
               ROUND(quantile_cont(d, 0.9999), 1) AS p9999, ROUND(MAX(d), 1) AS maximo FROM b""")

    mostrar(con, "Fechas de subida fuera del mes (las mas raras)", f"""
        SELECT strftime(tpep_pickup_datetime, '%Y-%m') AS mes_subida, COUNT(*) AS filas FROM t
        WHERE year(tpep_pickup_datetime) <> {anio} OR month(tpep_pickup_datetime) <> {mes}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8""")

    mostrar(con, "Tipos de pago (payment_type)", """
        SELECT payment_type, COUNT(*) AS filas,
               ROUND(AVG(tip_amount), 2) AS propina_media,
               SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) AS pasajeros_null
        FROM t GROUP BY 1 ORDER BY 1""")

    if "cbd_congestion_fee" in [r[0] for r in con.execute("DESCRIBE t").fetchall()]:
        mostrar(con, "Valores de cbd_congestion_fee", """
            SELECT cbd_congestion_fee, COUNT(*) AS filas FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 8""")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True, help="AAAA-MM; tambien explora el mismo mes del anio anterior")
    parser.add_argument("--comparar", type=int, help="anio de comparacion (por defecto, anio - 1)")
    args = parser.parse_args()
    anio, mes = args.mes.split("-")
    comparado = f"{args.comparar or int(anio) - 1}-{mes}"
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    for periodo in (args.mes, comparado):
        if not Path(archivo(periodo)).exists():
            sys.exit(f"Falta {archivo(periodo)}: correr primero src/ingesta.py --mes {args.mes}")
        explorar(con, periodo)


if __name__ == "__main__":
    main()
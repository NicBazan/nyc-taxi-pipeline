import argparse
import shutil

import duckdb

from config import CURATED_DIR, get_db_connection, get_target_months, log_memory


def contar_particiones(curated_dir=None) -> dict:
    """{'year=2025/month=03': filas, ...}: sirve para mostrar el antes y despues."""
    curated_dir = curated_dir or CURATED_DIR
    conteos = {}
    con = duckdb.connect()  # base en memoria, solo para contar
    for archivo in sorted(curated_dir.glob("yellow/year=*/month=*/*.parquet")):
        filas = con.execute(f"SELECT COUNT(*) FROM read_parquet('{archivo.as_posix()}')").fetchone()[0]
        conteos[f"{archivo.parent.parent.name}/{archivo.parent.name}"] = filas
    con.close()
    return conteos


def transformar(con, target_months, curated_dir=None) -> None:
    curated_dir = curated_dir or CURATED_DIR
    con.execute("""
        CREATE OR REPLACE TABLE curated_trips AS
        SELECT
            v.*,
            ROUND(EXTRACT(EPOCH FROM (v.dropoff_datetime - v.pickup_datetime)) / 60.0, 2)
                AS duracion_minutos,
            ROUND(v.trip_distance / (EXTRACT(EPOCH FROM (v.dropoff_datetime - v.pickup_datetime)) / 3600.0), 2)
                AS velocidad_mph,
            CAST(v.pickup_datetime AS DATE) AS pickup_date,
            CAST(EXTRACT(HOUR FROM v.pickup_datetime) AS INTEGER) AS pickup_hour,
            CAST(EXTRACT(ISODOW FROM v.pickup_datetime) AS INTEGER) AS pickup_dia_semana,
            ROUND(v.tip_amount / NULLIF(v.fare_amount, 0) * 100, 2) AS propina_pct,
            pu.borough AS pu_borough,
            pu.zone AS pu_zone,
            dz.borough AS do_borough,
            dz.zone AS do_zone
        FROM stg_viajes_validos v
        LEFT JOIN raw_zone_lookup pu ON v.pu_location_id = pu.location_id
        LEFT JOIN raw_zone_lookup dz ON v.do_location_id = dz.location_id;
    """)

    for year, month, periodo in target_months:
        particion = curated_dir / "yellow" / f"year={year}" / f"month={month}"
        if particion.exists():
            shutil.rmtree(particion)
        particion.mkdir(parents=True)
        destino = (particion / f"curated_{periodo}.parquet").as_posix()
        con.execute(f"""
            COPY (SELECT * FROM curated_trips WHERE source_period = '{periodo}')
            TO '{destino}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        filas = con.execute(f"SELECT COUNT(*) FROM read_parquet('{destino}')").fetchone()[0]
        print(f"[TRANSFORMACION] {particion.relative_to(curated_dir)}: {filas:,} filas")
    con.execute("DROP TABLE curated_trips;")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True)
    args = parser.parse_args()

    print(f"[TRANSFORMACION] Particiones ANTES: {contar_particiones()}")
    con = get_db_connection()
    transformar(con, get_target_months(args.mes))
    con.close()
    print(f"[TRANSFORMACION] Particiones DESPUES: {contar_particiones()}")
    log_memory("FIN TRANSFORMACION")


if __name__ == "__main__":
    main()

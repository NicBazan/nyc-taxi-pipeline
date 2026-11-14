import argparse

from config import (
    CURATED_DIR,
    ZONE_LOOKUP_PATH,
    get_db_connection,
    get_target_months,
    log_memory,
    postgres_url,
)

def conectar_postgres(con) -> None:
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{postgres_url()}' AS pg (TYPE postgres);")
    con.execute("CREATE SCHEMA IF NOT EXISTS pg.curado;")

def cargar_zonas(con) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE pg.curado.zonas AS
        SELECT CAST(LocationID AS INTEGER) AS location_id, Borough AS borough,
               Zone AS zone, service_zone
        FROM read_csv_auto('{ZONE_LOOKUP_PATH.as_posix()}');
    """)

def cargar_mes(con, year: str, month: str, periodo: str) -> int:
    parquet = (CURATED_DIR / "yellow" / f"year={year}" / f"month={month}" / f"curated_{periodo}.parquet").as_posix()
    con.execute(f"CREATE TABLE IF NOT EXISTS pg.curado.viajes AS SELECT * FROM read_parquet('{parquet}') LIMIT 0;")
    con.execute(f"DELETE FROM pg.curado.viajes WHERE source_period = '{periodo}';")
    con.execute(f"INSERT INTO pg.curado.viajes SELECT * FROM read_parquet('{parquet}');")
    filas = con.execute(f"SELECT COUNT(*) FROM pg.curado.viajes WHERE source_period = '{periodo}'").fetchone()[0]
    print(f"[CARGA] curado.viajes {periodo}: {filas:,} filas en Postgres")
    return filas

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True)
    args = parser.parse_args()

    con = get_db_connection()
    conectar_postgres(con)
    cargar_zonas(con)
    for year, month, periodo in get_target_months(args.mes):
        cargar_mes(con, year, month, periodo)
    con.close()
    log_memory("FIN CARGA")


if __name__ == "__main__":
    main()
"""Parte 3: Reglas de calidad, cuarentena y reconciliacion.

Las reglas NO estan escritas aca: se leen de contracts/reglas_calidad.yaml,
donde cada una tiene id, descripcion, severidad, umbral, justificacion y su
condicion SQL. Este script:

1. Evalua todas las reglas sobre v_stg_normalizada en una sola pasada SQL.
2. Separa las filas:
   - con alguna regla de RECHAZO violada -> stg_cuarentena, con la fila,
     el archivo de origen, el numero de fila y la lista de reglas violadas;
   - el resto -> stg_viajes_validos, con las ADVERTENCIAS en quality_flags
     ('OK' si no tiene ninguna). Es la entrada de la Parte 4.
3. Reconciliacion: para cada mes, filas leidas del Parquet crudo ==
   aceptadas + cuarentena. Si no da, el pipeline se corta con codigo 1.
4. Guarda la cuarentena en Parquet (data/quarantine/yellow/year=/month=/) y un
   reporte por mes (data/reports/calidad_AAAA-MM.md) con filas leidas,
   aceptadas, en cuarentena y cantidad por regla.
"""
import argparse
import shutil
import sys

from config import (
    QUARANTINE_DIR,
    REPORTS_DIR,
    ZONE_LOOKUP_PATH,
    contract_clean_columns,
    get_db_connection,
    get_target_months,
    load_rules,
    log_memory,
    raw_path,
)

COLUMNAS_AUXILIARES = ["duracion_min", "rn_dup", "pu_existe", "do_existe"]


def cargar_zonas(con, path=None):
    path = path or ZONE_LOOKUP_PATH
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_zone_lookup AS
        SELECT
            CAST(LocationID AS INTEGER) AS location_id,
            Borough AS borough,
            Zone AS zone,
            service_zone
        FROM read_csv_auto('{path.as_posix()}');
    """)


def lista_de_reglas(reglas, severidad: str) -> str:
    """Arma [CASE WHEN (cond) THEN 'DQ01' END, ...] para las reglas de esa severidad."""
    casos = [f"CASE WHEN ({r['condicion'].strip()}) THEN '{r['id']}' END"
             for r in reglas if r["severidad"] == severidad]
    if not casos:
        return "CAST([] AS VARCHAR[])"
    return f"list_filter([{', '.join(casos)}], lambda x: x IS NOT NULL)"


def evaluar_reglas(con, reglas=None, columnas_contrato=None):
    """Crea stg_viajes_validos y stg_cuarentena a partir de v_stg_normalizada."""
    reglas = reglas or load_rules()
    severidades = {r["severidad"] for r in reglas}
    if not severidades <= {"rechazo", "advertencia"}:
        sys.exit(f"[ERROR] Severidad desconocida en reglas_calidad.yaml: {severidades}")

    # Duplicado exacto = misma fila en TODAS las columnas del contrato, dentro
    # del mismo archivo. La primera aparicion (menor nro_fila_archivo) queda
    # con rn_dup = 1; las copias, con 2, 3...
    columnas = ", ".join(columnas_contrato or contract_clean_columns())
    con.execute(f"""
        CREATE OR REPLACE TABLE stg_evaluada AS
        WITH base AS (
            SELECT v.*,
                EXTRACT(EPOCH FROM (v.dropoff_datetime - v.pickup_datetime)) / 60.0 AS duracion_min,
                ROW_NUMBER() OVER (
                    PARTITION BY v.source_period, {columnas}
                    ORDER BY v.nro_fila_archivo
                ) AS rn_dup,
                pu.location_id IS NOT NULL AS pu_existe,
                dz.location_id IS NOT NULL AS do_existe
            FROM v_stg_normalizada v
            LEFT JOIN raw_zone_lookup pu ON v.pu_location_id = pu.location_id
            LEFT JOIN raw_zone_lookup dz ON v.do_location_id = dz.location_id
        )
        SELECT * EXCLUDE ({', '.join(COLUMNAS_AUXILIARES)}),
            {lista_de_reglas(reglas, 'rechazo')} AS reglas_rechazo,
            {lista_de_reglas(reglas, 'advertencia')} AS reglas_advertencia
        FROM base;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE stg_viajes_validos AS
        SELECT * EXCLUDE (reglas_rechazo, reglas_advertencia),
            CASE WHEN len(reglas_advertencia) = 0 THEN 'OK'
                 ELSE array_to_string(reglas_advertencia, '|') END AS quality_flags
        FROM stg_evaluada
        WHERE len(reglas_rechazo) = 0;
    """)
    con.execute("""
        CREATE OR REPLACE TABLE stg_cuarentena AS
        SELECT * EXCLUDE (reglas_rechazo, reglas_advertencia),
            array_to_string(reglas_rechazo, '|') AS reglas_violadas,
            array_to_string(reglas_advertencia, '|') AS advertencias,
            CURRENT_TIMESTAMP AS fecha_cuarentena
        FROM stg_evaluada
        WHERE len(reglas_rechazo) > 0;
    """)

    # Cantidad de filas por regla (una fila puede violar varias reglas)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE conteo_reglas AS
        SELECT source_period, regla, COUNT(*) AS filas
        FROM (SELECT source_period, unnest(reglas_rechazo || reglas_advertencia) AS regla FROM stg_evaluada)
        GROUP BY ALL;
    """)
    con.execute("DROP TABLE stg_evaluada;")


def reconciliar(con, target_months, raw_dir=None) -> list:
    """Leidas (contadas en el Parquet crudo) == aceptadas + cuarentena, por mes.

    Devuelve una lista de tuplas (periodo, leidas, aceptadas, cuarentena,
    con_advertencia). Si algun mes no cierra, corta el pipeline con codigo 1.
    """
    resultado = []
    for year, month, periodo in target_months:
        leidas = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{raw_path(year, month, periodo, raw_dir).as_posix()}')"
        ).fetchone()[0]
        aceptadas, con_advertencia = con.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE quality_flags <> 'OK') "
            "FROM stg_viajes_validos WHERE source_period = ?", [periodo]
        ).fetchone()
        cuarentena = con.execute(
            "SELECT COUNT(*) FROM stg_cuarentena WHERE source_period = ?", [periodo]
        ).fetchone()[0]

        if leidas != aceptadas + cuarentena:
            print(f"[ERROR DE RECONCILIACION] {periodo}: leidas={leidas:,} != "
                  f"aceptadas={aceptadas:,} + cuarentena={cuarentena:,}")
            sys.exit(1)
        print(f"[RECONCILIACION {periodo}] OK: {leidas:,} leidas = {aceptadas:,} aceptadas "
              f"+ {cuarentena:,} en cuarentena ({cuarentena / leidas * 100 if leidas else 0:.2f} %); "
              f"{con_advertencia:,} aceptadas con advertencia")
        resultado.append((periodo, leidas, aceptadas, cuarentena, con_advertencia))
    return resultado


def guardar_cuarentena(con, target_months, quarantine_dir=None) -> None:
    """Escribe la cuarentena de cada mes reemplazando solo su particion."""
    quarantine_dir = quarantine_dir or QUARANTINE_DIR
    for year, month, periodo in target_months:
        part_dir = quarantine_dir / "yellow" / f"year={year}" / f"month={month}"
        if part_dir.exists():
            shutil.rmtree(part_dir)
        part_dir.mkdir(parents=True)
        destino = (part_dir / f"cuarentena_{periodo}.parquet").as_posix()
        con.execute(f"""
            COPY (SELECT * FROM stg_cuarentena WHERE source_period = '{periodo}' ORDER BY nro_fila_archivo)
            TO '{destino}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)


def guardar_reportes(con, conteos, reglas=None, reports_dir=None) -> None:
    """Tablas reporte_calidad y reporte_calidad_reglas + un .md por mes.

    Las tablas acumulan meses: se borran solo las filas de los meses que se
    reprocesan, asi correr varios meses no pisa los anteriores.
    """
    reglas = reglas or load_rules()
    reports_dir = reports_dir or REPORTS_DIR
    reports_dir.mkdir(parents=True, exist_ok=True)
    con.execute("""
        CREATE TABLE IF NOT EXISTS reporte_calidad (
            periodo VARCHAR, filas_leidas BIGINT, filas_aceptadas BIGINT,
            filas_cuarentena BIGINT, filas_con_advertencia BIGINT, pct_cuarentena DOUBLE
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS reporte_calidad_reglas (
            periodo VARCHAR, regla VARCHAR, descripcion VARCHAR, severidad VARCHAR,
            filas BIGINT, pct DOUBLE
        )""")

    for periodo, leidas, aceptadas, cuarentena, con_advertencia in conteos:
        con.execute("DELETE FROM reporte_calidad WHERE periodo = ?", [periodo])
        con.execute("DELETE FROM reporte_calidad_reglas WHERE periodo = ?", [periodo])
        pct = round(cuarentena / leidas * 100, 3) if leidas else 0.0
        con.execute("INSERT INTO reporte_calidad VALUES (?, ?, ?, ?, ?, ?)",
                    [periodo, leidas, aceptadas, cuarentena, con_advertencia, pct])

        por_regla = dict(con.execute(
            "SELECT regla, filas FROM conteo_reglas WHERE source_period = ?", [periodo]).fetchall())
        filas_reglas = []
        for r in reglas:  # todas las reglas, aunque hayan dado 0
            n = por_regla.get(r["id"], 0)
            filas_reglas.append((r["id"], r["descripcion"], r["severidad"], n,
                                 round(n / leidas * 100, 3) if leidas else 0.0))
        con.executemany("INSERT INTO reporte_calidad_reglas VALUES (?, ?, ?, ?, ?, ?)",
                        [(periodo, *f) for f in filas_reglas])

        md = [
            f"# Reporte de calidad {periodo}", "",
            "| filas leidas | aceptadas | en cuarentena | % cuarentena | aceptadas con advertencia |",
            "|---:|---:|---:|---:|---:|",
            f"| {leidas:,} | {aceptadas:,} | {cuarentena:,} | {pct} | {con_advertencia:,} |", "",
            f"Reconciliacion: {leidas:,} = {aceptadas:,} + {cuarentena:,} -> OK", "",
            "## Cantidad de filas por regla", "",
            "Una fila puede violar varias reglas, por eso la suma puede superar las filas en cuarentena.", "",
            "| regla | descripcion | severidad | filas | % |", "|---|---|---|---:|---:|",
        ]
        md += [f"| {i} | {d} | {s} | {n:,} | {p} |" for i, d, s, n, p in filas_reglas]
        md += ["", "## Un ejemplo de fila en cuarentena por regla", ""]
        for r in reglas:
            if r["severidad"] != "rechazo":
                continue
            ej = con.execute("""
                SELECT archivo_origen, nro_fila_archivo, reglas_violadas, pickup_datetime, dropoff_datetime,
                       trip_distance, total_amount, pu_location_id, do_location_id
                FROM stg_cuarentena
                WHERE source_period = ? AND list_contains(string_split(reglas_violadas, '|'), ?)
                ORDER BY nro_fila_archivo LIMIT 1""", [periodo, r["id"]]).fetchone()
            md.append(f"- **{r['id']}**: " + (
                f"fila {ej[1]} de `{ej[0]}` -> reglas `{ej[2]}`; subida {ej[3]}, bajada {ej[4]}, "
                f"distancia {ej[5]}, total {ej[6]}, zonas {ej[7]}->{ej[8]}" if ej else "sin filas este mes"))
        (reports_dir / f"calidad_{periodo}.md").write_text("\n".join(md) + "\n", encoding="utf-8")
        print(f"[CALIDAD] Reporte escrito: {reports_dir / f'calidad_{periodo}.md'}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True)
    args = parser.parse_args()
    target_months = get_target_months(args.mes)

    con = get_db_connection()
    reglas = load_rules()
    cargar_zonas(con)
    evaluar_reglas(con, reglas)
    conteos = reconciliar(con, target_months)
    guardar_cuarentena(con, target_months)
    guardar_reportes(con, conteos, reglas)
    log_memory("FIN CALIDAD")
    con.close()


if __name__ == "__main__":
    main()
"""Parte 2: Contrato de esquema y evolucion entre anios.

Para cada archivo del par de meses:
1. Compara su esquema real contra contracts/yellow_schema.yaml y reporta
   columnas faltantes, columnas nuevas y tipos distintos (tabla
   reporte_esquema + data/reports/esquema_AAAA-MM.md).
2. Arma un SELECT que normaliza los nombres a snake_case, castea al tipo del
   contrato y completa las columnas opcionales que faltan (cbd_congestion_fee
   en 2024 = 0.0).
3. Une los dos meses en la vista v_stg_normalizada, que es la entrada de la
   Parte 3. Cada fila conserva archivo_origen y nro_fila_archivo para poder
   volver a la fila original del Parquet crudo.

Politica ante cambios:
- Falta una columna obligatoria -> error, el pipeline se corta (codigo 1).
- Aparece una columna nueva -> se avisa en el reporte y se ignora (solo se
  procesan las columnas del contrato, para no romper el modelo aguas abajo).
- Cambia el tipo -> se avisa y se castea al tipo del contrato.
"""
import argparse
import sys

from config import (
    BASE_DIR,
    REPORTS_DIR,
    get_db_connection,
    get_target_months,
    load_contract,
    log_memory,
    raw_path,
)


def comparar_esquema(columnas_reales: dict, contract: dict) -> list:
    """Devuelve una fila por columna: (columna, tipo_contrato, tipo_real, estado).

    columnas_reales: {nombre_en_el_archivo: tipo}. El matcheo de nombres no
    distingue mayusculas (la TLC escribe Airport_fee o airport_fee segun el anio).
    Estados: ok, tipo_distinto, faltante_obligatoria, faltante_opcional, nueva.
    """
    reales_lower = {n.lower(): (n, t) for n, t in columnas_reales.items()}
    filas, usadas = [], set()
    for col in contract["columns"]:
        real = reales_lower.get(col["raw_name"].lower())
        if real:
            usadas.add(real[0])
            estado = "ok" if real[1].upper() == col["type"].upper() else "tipo_distinto"
            filas.append((real[0], col["type"], real[1], estado))
        else:
            estado = "faltante_obligatoria" if col.get("required", True) else "faltante_opcional"
            filas.append((col["raw_name"], col["type"], None, estado))
    for nombre, tipo in columnas_reales.items():
        if nombre not in usadas:
            filas.append((nombre, None, tipo, "nueva"))
    return filas


def construir_select(parquet_path, contract: dict, columnas_reales: dict, periodo: str) -> str:
    """SELECT que lleva un archivo crudo al formato del contrato."""
    reales_lower = {n.lower(): n for n in columnas_reales}
    expresiones = []
    for col in contract["columns"]:
        nombre_real = reales_lower.get(col["raw_name"].lower())
        if nombre_real:
            expresiones.append(f'CAST("{nombre_real}" AS {col["type"]}) AS {col["clean_name"]}')
        else:  # solo llega aca si es opcional: las obligatorias ya cortaron antes
            default = col.get("default_when_missing", "NULL")
            expresiones.append(f'CAST({default} AS {col["type"]}) AS {col["clean_name"]}')

    year, month = periodo.split("-")
    try:
        archivo = parquet_path.relative_to(BASE_DIR).as_posix()
    except ValueError:  # en los tests el archivo vive fuera del proyecto
        archivo = parquet_path.name
    expresiones += [
        f"'{periodo}' AS source_period",
        f"{int(year)} AS source_year",
        f"{int(month)} AS source_month",
        f"'{archivo}' AS archivo_origen",
        "file_row_number AS nro_fila_archivo",
    ]
    # hive_partitioning=false: year=/month= son carpetas, no columnas del Parquet.
    # file_row_number=true: numero de fila dentro del archivo crudo (trazabilidad).
    return (
        "SELECT\n  " + ",\n  ".join(expresiones) + "\n"
        f"FROM read_parquet('{parquet_path.as_posix()}', hive_partitioning=false, file_row_number=true)"
    )


def guardar_reporte(con, periodo: str, filas: list, reports_dir=None) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS reporte_esquema (
            periodo VARCHAR, columna VARCHAR, tipo_contrato VARCHAR, tipo_real VARCHAR, estado VARCHAR
        )""")
    con.execute("DELETE FROM reporte_esquema WHERE periodo = ?", [periodo])
    con.executemany("INSERT INTO reporte_esquema VALUES (?, ?, ?, ?, ?)", [(periodo, *f) for f in filas])

    reports_dir = reports_dir or REPORTS_DIR
    reports_dir.mkdir(parents=True, exist_ok=True)
    lineas = [f"# Reporte de esquema {periodo}", "",
              "| columna | tipo contrato | tipo real | estado |", "|---|---|---|---|"]
    lineas += [f"| {c} | {tc or '-'} | {tr or '-'} | {e} |" for c, tc, tr, e in filas]
    (reports_dir / f"esquema_{periodo}.md").write_text("\n".join(lineas) + "\n", encoding="utf-8")


def normalizar(con, target_months, raw_dir=None, contract=None, reports_dir=None) -> None:
    """Valida el esquema de cada mes y crea la vista v_stg_normalizada."""
    contract = contract or load_contract()
    selects = []
    for year, month, periodo in target_months:
        parquet_path = raw_path(year, month, periodo, raw_dir)
        if not parquet_path.exists():
            print(f"[ERROR] No existe {parquet_path}. Corre src/ingesta.py primero.")
            sys.exit(1)

        columnas_reales = dict(con.execute(
            f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM "
            f"read_parquet('{parquet_path.as_posix()}', hive_partitioning=false))"
        ).fetchall())
        filas = comparar_esquema(columnas_reales, contract)
        guardar_reporte(con, periodo, filas, reports_dir)

        for columna, tipo_contrato, tipo_real, estado in filas:
            if estado == "tipo_distinto":
                print(f"[AVISO ESQUEMA] {periodo}: '{columna}' viene como {tipo_real}, el contrato dice {tipo_contrato}. Se castea.")
            elif estado == "nueva":
                print(f"[AVISO ESQUEMA] {periodo}: columna nueva '{columna}' ({tipo_real}) no esta en el contrato. Se ignora.")
            elif estado == "faltante_opcional":
                print(f"[INFO ESQUEMA] {periodo}: falta la columna opcional '{columna}'. Se completa con el valor por defecto.")
        faltantes = [f[0] for f in filas if f[3] == "faltante_obligatoria"]
        if faltantes:
            print(f"[ERROR DE CONTRATO] {periodo}: faltan columnas obligatorias {faltantes}")
            sys.exit(1)

        resumen = ", ".join(f"{e}={sum(1 for f in filas if f[3] == e)}" for e in sorted({f[3] for f in filas}))
        print(f"[ESQUEMA] {periodo}: {resumen}")
        selects.append(construir_select(parquet_path, contract, columnas_reales, periodo))

    con.execute("CREATE OR REPLACE VIEW v_stg_normalizada AS\n" + "\nUNION ALL\n".join(selects))
    print("[ESQUEMA] Vista 'v_stg_normalizada' creada (snake_case, tipos del contrato, meses unificados).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True)
    args = parser.parse_args()

    con = get_db_connection()
    normalizar(con, get_target_months(args.mes))
    log_memory("FIN ESQUEMA")
    con.close()


if __name__ == "__main__":
    main()
"""Parte 1: Ingesta idempotente y trazable.

- Baja el mes pedido y el mismo mes del anio anterior a
  data/raw/yellow/year=AAAA/month=MM/ sin modificarlos.
- La tabla de zonas se baja una sola vez.
- Idempotencia: si el archivo ya esta y su SHA-256 coincide con el ultimo
  registro 'ok' del log, no se vuelve a descargar.
- ingestion_log guarda archivo, URL, filas, bytes, hash, fecha y hora, y estado.
- Ante fallas de red: 3 intentos con espera creciente (2 s y 4 s). Si fallan
  los 3, se registra el error y el programa sale con codigo 1.
"""
import argparse
import hashlib
import os
import sys
import time
from datetime import datetime, timezone

import requests

from config import RAW_DIR, ZONE_LOOKUP_PATH, get_db_connection, get_target_months, log_memory, raw_path

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
PERIODO_ZONAS = "zonas"  # clave de la tabla de zonas en ingestion_log


def init_ingestion_log(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            periodo VARCHAR,
            archivo VARCHAR,
            url VARCHAR,
            sha256 VARCHAR,
            tamano_bytes BIGINT,
            filas_leidas BIGINT,
            fecha_hora_utc TIMESTAMP,
            estado VARCHAR,
            mensaje VARCHAR
        );
    """)


def sha256_de_archivo(path) -> str:
    """Hash del archivo leyendo de a 64 KB, para no cargarlo entero en RAM."""
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            sha256.update(block)
    return sha256.hexdigest()


def descargar_con_reintentos(url: str, destino, max_intentos: int = 3) -> None:
    """Descarga a un archivo temporal '.part' y recien al final lo renombra.

    Asi, si la descarga se corta a la mitad, nunca queda en data/raw/ un
    Parquet incompleto con el nombre del archivo bueno.
    """
    temporal = destino.with_name(destino.name + ".part")
    ultimo_error = None
    for intento in range(1, max_intentos + 1):
        try:
            print(f"  -> Descargando {url} (intento {intento}/{max_intentos})")
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(temporal, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            os.replace(temporal, destino)
            return
        except requests.RequestException as e:
            ultimo_error = e
            if intento < max_intentos:
                espera = 2 ** intento  # 2 s, 4 s: espera creciente
                print(f"  [!] Error en intento {intento}: {e}. Reintentando en {espera}s...")
                time.sleep(espera)
    if temporal.exists():
        temporal.unlink()
    raise RuntimeError(f"Fallaron los {max_intentos} intentos descargando {url}: {ultimo_error}")


def ya_ingerido(con, periodo: str, hash_local: str) -> bool:
    """True si el ultimo registro 'ok' de ese periodo tiene el mismo hash."""
    fila = con.execute(
        "SELECT sha256 FROM ingestion_log WHERE periodo = ? AND estado = 'ok' "
        "ORDER BY fecha_hora_utc DESC LIMIT 1",
        [periodo],
    ).fetchone()
    return fila is not None and fila[0] == hash_local


def registrar_log(con, periodo, archivo, url, sha256, tamano, filas, estado, mensaje):
    """Agrega una fila al log. Nunca borra: el log es la historia de las descargas."""
    con.execute(
        "INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [periodo, str(archivo), url, sha256, tamano, filas,
         datetime.now(timezone.utc).replace(tzinfo=None), estado, mensaje],
    )


def ingerir_archivo(con, periodo, destino, url, contar_filas, descargar=descargar_con_reintentos) -> str:
    """Descarga un archivo si hace falta y lo registra. Devuelve 'omitido' u 'ok'.

    contar_filas: funcion que recibe la ruta y devuelve la cantidad de filas.
    descargar: se puede reemplazar en los tests para no salir a internet.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)
    if destino.exists() and ya_ingerido(con, periodo, sha256_de_archivo(destino)):
        print(f"[INGESTA] {periodo}: ya ingerido (SHA-256 verificado). Salteando descarga.")
        return "omitido"

    try:
        descargar(url, destino)
    except RuntimeError as e:
        registrar_log(con, periodo, destino, url, None, 0, 0, "error", str(e))
        print(f"[ERROR CRITICO] {periodo}: {e}")
        sys.exit(1)

    file_hash = sha256_de_archivo(destino)
    file_size = os.path.getsize(destino)
    filas = contar_filas(destino)
    registrar_log(con, periodo, destino, url, file_hash, file_size, filas, "ok", "ingesta exitosa")
    print(f"[INGESTA] {periodo}: OK ({filas:,} filas, {file_size / 1024 / 1024:.1f} MB)")
    return "ok"


def ingerir_mes(con, year: str, month: str, periodo: str, raw_dir=None, descargar=descargar_con_reintentos) -> str:
    destino = raw_path(year, month, periodo, raw_dir)
    url = f"{TLC_BASE_URL}/{destino.name}"

    def contar(path):
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')").fetchone()[0]

    return ingerir_archivo(con, periodo, destino, url, contar, descargar)


def ingerir_zonas(con, destino=None, descargar=descargar_con_reintentos) -> str:
    """La tabla de zonas se trata igual que los Parquet: hash y registro en el log."""
    destino = destino or ZONE_LOOKUP_PATH

    def contar(path):
        return con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{path.as_posix()}')").fetchone()[0]

    return ingerir_archivo(con, PERIODO_ZONAS, destino, ZONE_LOOKUP_URL, contar, descargar)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True, help="AAAA-MM, ej 2025-03")
    args = parser.parse_args()

    con = get_db_connection()
    init_ingestion_log(con)

    ingerir_zonas(con)
    for year, month, periodo in get_target_months(args.mes):
        ingerir_mes(con, year, month, periodo, RAW_DIR)

    log_memory("FIN INGESTA")
    con.close()


if __name__ == "__main__":
    main()
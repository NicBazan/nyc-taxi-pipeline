"""Tests de las Partes 1, 2 y 3 (los 3 que pide el enunciado y algunos mas).

- Reglas de calidad sobre datos sinteticos: cada fila rota cae en su regla.
- Idempotencia de la ingesta: la segunda corrida no descarga ni duplica el log.
- Reconciliacion de conteos: cierra, y si falta una fila el pipeline se corta.

Todo corre en carpetas temporales: no descarga nada ni toca data/.
"""
from datetime import datetime, timedelta

import pytest

import calidad
import esquema
import ingesta
from config import get_target_months
from conftest import escribir_parquet, fila, parquet_del_mes

MES = "2030-01"  # el par de comparacion es 2029-01
SUBIDA = datetime(2030, 1, 10, 12, 0, 0)

# Fila sintetica -> (reglas de rechazo esperadas, advertencias esperadas)
CASOS = [
    (fila(), set(), set()),                                                     # 0 viaje valido
    (fila(tpep_dropoff_datetime=SUBIDA), {"DQ02"}, set()),                      # 1 bajada = subida
    (fila(trip_distance=0.0), {"DQ03"}, set()),                                 # 2 distancia 0
    (fila(trip_distance=150.0, tpep_dropoff_datetime=SUBIDA + timedelta(hours=3)), {"DQ03"}, set()),  # 3 > 100 millas
    (fila(total_amount=-5.0), {"DQ04"}, set()),                                 # 4 total negativo
    (fila(passenger_count=None), set(), {"DQ05"}),                              # 5 pasajeros nulo
    (fila(DOLocationID=999), {"DQ06"}, set()),                                  # 6 zona inexistente
    (fila(), {"DQ07"}, set()),                                                  # 7 copia exacta de la fila 0
    (fila(tpep_pickup_datetime=datetime(2029, 12, 31, 23, 50),
          tpep_dropoff_datetime=datetime(2029, 12, 31, 23, 59)), {"DQ01"}, set()),  # 8 de otro mes
    (fila(tpep_dropoff_datetime=SUBIDA + timedelta(hours=13)), {"DQ08"}, set()),    # 9 mas de 12 h
    (fila(fare_amount=-3.0, total_amount=5.0), set(), {"DQ09"}),                # 10 tarifa negativa, total positivo
    (fila(PULocationID=264), set(), {"DQ10"}),                                  # 11 zona desconocida
]


def preparar_mes(entorno, filas_2030, filas_2029=None):
    """Escribe el par de Parquet sinteticos y corre Parte 2 y Parte 3 sobre ellos."""
    raw_dir = entorno["raw_dir"]
    escribir_parquet(parquet_del_mes(raw_dir, MES), filas_2030)
    # El mes de comparacion no tiene cbd_congestion_fee, como 2024 en la TLC
    viaje_2029 = fila(tpep_pickup_datetime=datetime(2029, 1, 10, 12, 0),
                      tpep_dropoff_datetime=datetime(2029, 1, 10, 12, 10))
    escribir_parquet(parquet_del_mes(raw_dir, "2029-01"), filas_2029 or [viaje_2029],
                     sin_columnas=("cbd_congestion_fee",))
    con = entorno["con"]
    esquema.normalizar(con, get_target_months(MES), raw_dir=raw_dir, reports_dir=entorno["reports"])
    calidad.cargar_zonas(con, entorno["zonas"])
    calidad.evaluar_reglas(con)
    return con


def test_cada_regla_de_calidad_atrapa_su_fila_sintetica(entorno):
    con = preparar_mes(entorno, [c[0] for c in CASOS])

    cuarentena = dict(con.execute(
        "SELECT nro_fila_archivo, reglas_violadas FROM stg_cuarentena WHERE source_period = ?", [MES]).fetchall())
    aceptadas = dict(con.execute(
        "SELECT nro_fila_archivo, quality_flags FROM stg_viajes_validos WHERE source_period = ?", [MES]).fetchall())

    for nro, (_, rechazos, advertencias) in enumerate(CASOS):
        if rechazos:
            assert nro in cuarentena, f"la fila {nro} tenia que ir a cuarentena"
            assert set(cuarentena[nro].split("|")) >= rechazos, f"fila {nro}: {cuarentena[nro]}"
        else:
            assert nro in aceptadas, f"la fila {nro} tenia que quedar aceptada"
            flags = set() if aceptadas[nro] == "OK" else set(aceptadas[nro].split("|"))
            assert flags == advertencias, f"fila {nro}: flags {aceptadas[nro]}"


def test_cuarentena_guarda_archivo_de_origen_y_numero_de_fila(entorno):
    con = preparar_mes(entorno, [fila(), fila(total_amount=-1.0)])
    archivo, nro, reglas = con.execute(
        "SELECT archivo_origen, nro_fila_archivo, reglas_violadas FROM stg_cuarentena").fetchone()
    assert archivo.endswith("yellow_tripdata_2030-01.parquet")
    assert (nro, reglas) == (1, "DQ04")


# ---------------------------------------------------------------- Parte 2

def test_mes_sin_cbd_congestion_fee_se_completa_con_cero(entorno):
    con = preparar_mes(entorno, [fila()])
    valor = con.execute(
        "SELECT cbd_congestion_fee FROM v_stg_normalizada WHERE source_period = '2029-01'").fetchone()[0]
    assert valor == 0.0
    estado = con.execute(
        "SELECT estado FROM reporte_esquema WHERE periodo = '2029-01' AND columna = 'cbd_congestion_fee'").fetchone()[0]
    assert estado == "faltante_opcional"


def test_comparar_esquema_detecta_faltantes_nuevas_y_tipos_distintos():
    contrato = {"columns": [
        {"raw_name": "VendorID", "clean_name": "vendor_id", "type": "INTEGER", "required": True},
        {"raw_name": "trip_distance", "clean_name": "trip_distance", "type": "DOUBLE", "required": True},
        {"raw_name": "fare_amount", "clean_name": "fare_amount", "type": "DOUBLE", "required": True},
    ]}
    reales = {"vendorid": "INTEGER", "trip_distance": "VARCHAR", "columna_sorpresa": "DOUBLE"}
    estados = {c: e for c, _, _, e in esquema.comparar_esquema(reales, contrato)}
    assert estados == {"vendorid": "ok", "trip_distance": "tipo_distinto",
                       "fare_amount": "faltante_obligatoria", "columna_sorpresa": "nueva"}


# ---------------------------------------------------------------- Reconciliacion

def test_reconciliacion_cierra_por_mes(entorno):
    con = preparar_mes(entorno, [c[0] for c in CASOS])
    conteos = calidad.reconciliar(con, get_target_months(MES), raw_dir=entorno["raw_dir"])
    for periodo, leidas, aceptadas, cuarentena, _ in conteos:
        assert leidas == aceptadas + cuarentena, periodo
    assert conteos[0][1:4] == (len(CASOS), 4, 8)  # 2030-01: 12 leidas = 4 aceptadas + 8 en cuarentena


def test_reconciliacion_corta_el_pipeline_si_se_pierde_una_fila(entorno):
    con = preparar_mes(entorno, [c[0] for c in CASOS])
    con.execute("DELETE FROM stg_viajes_validos WHERE nro_fila_archivo = 0 AND source_period = ?", [MES])
    with pytest.raises(SystemExit) as salida:
        calidad.reconciliar(con, get_target_months(MES), raw_dir=entorno["raw_dir"])
    assert salida.value.code == 1


# ---------------------------------------------------------------- Parte 1

def test_ingesta_es_idempotente(entorno):
    con = entorno["con"]
    ingesta.init_ingestion_log(con)
    origen = escribir_parquet(entorno["tmp"] / "fuente.parquet", [fila(), fila(trip_distance=3.0)])
    descargas = []

    def descarga_falsa(url, destino):  # en vez de internet, copia el archivo local
        descargas.append(url)
        destino.write_bytes(origen.read_bytes())

    primera = ingesta.ingerir_mes(con, "2030", "01", MES, entorno["raw_dir"], descarga_falsa)
    segunda = ingesta.ingerir_mes(con, "2030", "01", MES, entorno["raw_dir"], descarga_falsa)

    assert (primera, segunda) == ("ok", "omitido")
    assert len(descargas) == 1
    filas_log = con.execute("SELECT estado, filas_leidas FROM ingestion_log WHERE periodo = ?", [MES]).fetchall()
    assert filas_log == [("ok", 2)]


def test_ingesta_vuelve_a_bajar_si_el_archivo_cambio(entorno):
    con = entorno["con"]
    ingesta.init_ingestion_log(con)
    origen = escribir_parquet(entorno["tmp"] / "fuente.parquet", [fila()])
    copiar = lambda url, destino: destino.write_bytes(origen.read_bytes())  # noqa: E731
    ingesta.ingerir_mes(con, "2030", "01", MES, entorno["raw_dir"], copiar)

    parquet_del_mes(entorno["raw_dir"], MES).write_bytes(b"archivo corrupto")
    assert ingesta.ingerir_mes(con, "2030", "01", MES, entorno["raw_dir"], copiar) == "ok"


def test_falla_de_red_reintenta_3_veces_con_espera_creciente(monkeypatch, tmp_path):
    esperas = []

    def get_que_falla(*args, **kwargs):
        raise ingesta.requests.ConnectionError("sin red")

    monkeypatch.setattr(ingesta.requests, "get", get_que_falla)
    monkeypatch.setattr(ingesta.time, "sleep", esperas.append)
    with pytest.raises(RuntimeError):
        ingesta.descargar_con_reintentos("http://x/archivo.parquet", tmp_path / "archivo.parquet")
    assert esperas == [2, 4]
    assert not (tmp_path / "archivo.parquet").exists()


def test_falla_definitiva_registra_error_y_sale_con_codigo_1(entorno):
    con = entorno["con"]
    ingesta.init_ingestion_log(con)

    def descarga_rota(url, destino):
        raise RuntimeError("Fallaron los 3 intentos")

    with pytest.raises(SystemExit) as salida:
        ingesta.ingerir_mes(con, "2030", "01", MES, entorno["raw_dir"], descarga_rota)
    assert salida.value.code == 1
    assert con.execute("SELECT estado FROM ingestion_log").fetchall() == [("error",)]
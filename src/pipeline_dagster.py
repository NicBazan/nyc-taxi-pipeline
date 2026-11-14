"""Orquestacion con Dagster: todo el pipeline como un grafo de assets.

Cada etapa es un ASSET (un dato que el pipeline produce). Las flechas del
grafo salen de los 'deps': un asset no corre hasta que corrieron los que
necesita. El mes se toma de la variable de entorno MES (la pone el Makefile).

viajes_crudos -> viajes_normalizados -> viajes_validados -> curado_parquet
-> curado_postgres -> [modelos dbt] -> resultados
"""
import os

from dagster import AssetExecutionContext, Definitions, MaterializeResult, MetadataValue, asset
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

import calidad
import carga_postgres
import esquema
import ingesta
import resultados
import transformacion
from config import DBT_DIR, REPORTS_DIR, get_db_connection, get_target_months


def mes_actual() -> str:
    return os.getenv("MES", "2025-03")


@asset(group_name="parte1_ingesta")
def viajes_crudos(context: AssetExecutionContext) -> MaterializeResult:
    """Parte 1: baja el mes y su comparacion con hash, log y reintentos."""
    con = get_db_connection()
    ingesta.init_ingestion_log(con)
    ingesta.ingerir_zonas(con)
    for year, month, periodo in get_target_months(mes_actual()):
        ingesta.ingerir_mes(con, year, month, periodo)
    log = con.execute(
        "SELECT periodo, estado, filas_leidas, fecha_hora_utc FROM ingestion_log ORDER BY fecha_hora_utc DESC LIMIT 6"
    ).fetchall()
    con.close()
    return MaterializeResult(metadata={"ultimas_descargas": MetadataValue.md(
        "\n".join(f"- {p}: {e}, {f:,} filas ({t:%Y-%m-%d %H:%M})" for p, e, f, t in log))})


@asset(deps=[viajes_crudos], group_name="parte2_esquema")
def viajes_normalizados() -> MaterializeResult:
    """Parte 2: control de esquema contra el contrato y vista unificada."""
    con = get_db_connection()
    esquema.normalizar(con, get_target_months(mes_actual()))
    con.close()
    reporte = (REPORTS_DIR / f"esquema_{get_target_months(mes_actual())[1][2]}.md").read_text(encoding="utf-8")
    return MaterializeResult(metadata={"reporte_mes_anterior": MetadataValue.md(reporte)})


@asset(deps=[viajes_normalizados], group_name="parte3_calidad")
def viajes_validados() -> MaterializeResult:
    """Parte 3: reglas de calidad, cuarentena, reconciliacion y reporte."""
    meses = get_target_months(mes_actual())
    con = get_db_connection()
    reglas = calidad.load_rules()
    calidad.cargar_zonas(con)
    calidad.evaluar_reglas(con, reglas)
    conteos = calidad.reconciliar(con, meses)
    calidad.guardar_cuarentena(con, meses)
    calidad.guardar_reportes(con, conteos, reglas)
    con.close()
    reporte = (REPORTS_DIR / f"calidad_{mes_actual()}.md").read_text(encoding="utf-8")
    return MaterializeResult(metadata={
        "leidas": conteos[0][1], "aceptadas": conteos[0][2], "cuarentena": conteos[0][3],
        "reporte_calidad": MetadataValue.md(reporte),
    })


@asset(deps=[viajes_validados], group_name="parte4_transformacion")
def curado_parquet() -> MaterializeResult:
    """Parte 4: columnas derivadas, zonas y Parquet particionado year/month."""
    antes = transformacion.contar_particiones()
    con = get_db_connection()
    transformacion.transformar(con, get_target_months(mes_actual()))
    con.close()
    despues = transformacion.contar_particiones()
    return MaterializeResult(metadata={"particiones_antes": antes, "particiones_despues": despues})


@asset(deps=[curado_parquet], group_name="parte4_transformacion")
def curado_postgres() -> MaterializeResult:
    """Parte 4: copia la capa curada a Postgres (esquema curado) para dbt."""
    con = get_db_connection()
    carga_postgres.conectar_postgres(con)
    carga_postgres.cargar_zonas(con)
    filas = {p: carga_postgres.cargar_mes(con, y, m, p) for y, m, p in get_target_months(mes_actual())}
    con.close()
    return MaterializeResult(metadata={"filas_por_mes": filas})


dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def modelo_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    """Parte 5: dbt build = crear las tablas del modelo estrella y correr sus tests."""
    yield from dbt.cli(["build"], context=context).stream()


@asset(deps=[modelo_dbt], group_name="parte5_resultados")
def resultados_md() -> MaterializeResult:
    """Parte 5: corre las 6 preguntas de sql/ y escribe RESULTADOS.md."""
    ruta = resultados.generar_resultados(mes_actual())
    return MaterializeResult(metadata={"archivo": ruta})


defs = Definitions(
    assets=[viajes_crudos, viajes_normalizados, viajes_validados, curado_parquet,
            curado_postgres, modelo_dbt, resultados_md],
    resources={"dbt": DbtCliResource(project_dir=dbt_project, profiles_dir=str(DBT_DIR))},
)

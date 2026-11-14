"""Parte 5 (final): correr las 6 preguntas y escribir RESULTADOS.md."""
import argparse

import psycopg2

from config import (
    PG_DB,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    RESULTS_PATH,
    SQL_DIR,
    get_comparison_month,
)


def leer_comentario(sql: str, clave: str) -> str:
    for linea in sql.splitlines():
        if linea.startswith(f"-- {clave}:"):
            return linea.split(":", 1)[1].strip()
    return ""


def tabla_markdown(columnas, filas) -> str:
    lineas = ["| " + " | ".join(columnas) + " |", "|" + "---|" * len(columnas)]
    lineas += ["| " + " | ".join(str(v) for v in fila) + " |" for fila in filas]
    return "\n".join(lineas)


def generar_resultados(mes: str) -> str:
    mes_anterior = get_comparison_month(mes)
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB)
    partes = [
        f"# Resultados: {mes} contra {mes_anterior}",
        "",
        "Generado por `src/resultados.py` sobre el modelo de dbt (esquema `analytics` en Postgres).",
        "",
    ]

    numero = 0
    for numero, archivo in enumerate(sorted(SQL_DIR.glob("q*.sql")), start=1):
        sql = archivo.read_text(encoding="utf-8")
        consulta = sql.replace("{mes}", mes).replace("{mes_anterior}", mes_anterior)
        with conn.cursor() as cur:
            cur.execute(consulta)
            columnas = [d[0] for d in cur.description]
            filas = cur.fetchall()
        partes += [
            f"## {numero}. {leer_comentario(sql, 'pregunta')}",
            "",
            f"Consulta: `sql/{archivo.name}`",
            "",
            "```sql", consulta.strip(), "```",
            "",
            tabla_markdown(columnas, filas),
            "",
            f"**Interpretacion:** {leer_comentario(sql, 'interpretacion')}",
            "",
        ]
    conn.close()

    RESULTS_PATH.write_text("\n".join(partes), encoding="utf-8")
    print(f"[RESULTADOS] {RESULTS_PATH.name} generado con {numero} preguntas.")
    return str(RESULTS_PATH)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True)
    generar_resultados(parser.parse_args().mes)


if __name__ == "__main__":
    main()

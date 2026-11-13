FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "import duckdb; duckdb.sql('INSTALL postgres')"

ENV PYTHONPATH=/app/src \
    DAGSTER_HOME=/app/data/dagster_home

CMD ["sh", "-c", "mkdir -p $DAGSTER_HOME && dagster dev -h 0.0.0.0 -p 3000 -m pipeline_dagster"]
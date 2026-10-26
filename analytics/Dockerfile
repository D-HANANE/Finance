FROM python:3.11-slim
WORKDIR /app
COPY analytics/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install -r /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip python -m venv /opt/soda && /opt/soda/bin/pip install soda-core-duckdb==3.5.6 duckdb==1.0.0
COPY analytics/ /app/
COPY app/serving.py /app/serving.py
COPY dbt/ /app/dbt/
COPY soda/ /app/soda/
RUN mkdir -p /gold /exchange /dagster && useradd -m -u 1001 finance && chown -R finance:finance /app /gold /exchange /dagster
COPY --chown=finance:finance analytics/dagster.yaml /dagster/dagster.yaml
ENV DAGSTER_HOME=/dagster DBT_PROFILES_DIR=/app/dbt DUCKDB_PATH=/gold/finance.duckdb DBT_SEND_ANONYMOUS_USAGE_STATS=false SODA_CORE_TELEMETRY_ENABLED=false
USER finance
CMD ["python", "/app/start.py"]

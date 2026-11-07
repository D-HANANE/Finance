FROM python:3.11-slim
WORKDIR /app
COPY analytics/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install -r /tmp/requirements.txt
COPY analytics/ /app/
COPY app/serving.py app/alerting.py /app/
RUN mkdir -p /dagster && useradd -m -u 1001 finance && chown -R finance:finance /app /dagster
ENV DAGSTER_HOME=/dagster DBT_SEND_ANONYMOUS_USAGE_STATS=false
USER finance
CMD ["python","/app/start.py"]

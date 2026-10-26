FROM python:3.12-slim
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/pip pip install streamlit==1.43.2 plotly==6.0.1 psycopg2-binary==2.9.10 pandas==2.2.3 numpy==2.2.3 pyarrow==18.1.0
COPY ui/app.py /app/app.py
RUN useradd -m finance
USER finance
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--browser.gatherUsageStats=false"]

FROM apache/spark:3.5.5-python3
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
# Resolve at build time; running containers never install dependencies.
RUN mkdir -p /opt/finance /state /exchange && chmod 777 /state /exchange
COPY spark/resolve.py /opt/finance/resolve.py
COPY spark/ivysettings.xml /opt/finance/ivysettings.xml
RUN /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.2,org.apache.iceberg:iceberg-aws-bundle:1.7.2 --conf spark.jars.ivy=/tmp/ivy --conf spark.jars.ivySettings=/opt/finance/ivysettings.xml /opt/finance/resolve.py
COPY app/market.py app/serving.py /opt/finance/
COPY spark/ /opt/finance/
ENV PYTHONPATH=/opt/finance PYSPARK_PYTHON=python3
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1536m", "--conf", "spark.sql.shuffle.partitions=6", "--conf", "spark.driver.extraJavaOptions=-Duser.timezone=UTC", "/opt/finance/streaming.py"]

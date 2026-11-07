FROM apache/spark:3.5.5-python3
USER root
RUN --mount=type=cache,target=/root/.cache/pip pip install psycopg2-binary==2.9.10 boto3==1.35.99
RUN mkdir -p /opt/finance /state && chmod 777 /state
COPY spark/resolve.py spark/ivysettings.xml /opt/finance/
RUN /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.2,org.apache.iceberg:iceberg-aws-bundle:1.7.2,org.postgresql:postgresql:42.7.5,org.apache.hadoop:hadoop-aws:3.3.4 --exclude-packages org.apache.hadoop:hadoop-common --conf spark.jars.ivy=/tmp/ivy --conf spark.jars.ivySettings=/opt/finance/ivysettings.xml /opt/finance/resolve.py
COPY app/market.py app/serving.py /opt/finance/
COPY postgres/002-lakehouse.sql /opt/finance/migration.sql
COPY spark/ /opt/finance/
ENV PYTHONPATH=/opt/finance PYSPARK_PYTHON=python3
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit","--master","local[2]","--driver-memory","1280m","--conf","spark.sql.shuffle.partitions=4","--conf","spark.driver.extraJavaOptions=-Duser.timezone=UTC","/opt/finance/streaming.py"]

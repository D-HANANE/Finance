FROM apache/hive:4.0.0
USER root
ADD https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar /opt/hive/lib/postgres.jar
RUN chmod 644 /opt/hive/lib/postgres.jar
COPY hive/start.sh /opt/finance-start.sh
COPY hive/hive-site.xml /opt/hive/conf/hive-site.xml
RUN sed -i 's/\r$//' /opt/finance-start.sh && chmod 755 /opt/finance-start.sh
USER hive
ENV HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/*
ENTRYPOINT ["/bin/bash", "/opt/finance-start.sh"]

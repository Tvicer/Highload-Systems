FROM spark:3.5.7-python3

USER root

ENV PATH="/opt/spark/bin:${PATH}"

RUN mkdir -p /opt/spark/jars


RUN mkdir -p /opt/spark/jars

RUN apt update && apt install -y curl && \
    curl -sSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.4/postgresql-42.5.4.jar \
    -o /opt/spark/jars/postgresql-42.5.4.jar && \
    curl -sSL https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.4/clickhouse-jdbc-0.9.4-all.jar \
    -o /opt/spark/jars/clickhouse-jdbc-0.9.4-all.jar

# ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.4/postgresql-42.5.4.jar /opt/spark/jars/postgresql-42.5.4.jar
# ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6.jar /opt/spark/jars/clickhouse-jdbc-0.4.6.jar
WORKDIR /app
COPY spark /app/spark
COPY app/data /app/data
RUN chmod +x /app/spark/init.sh


ENV PYTHONPATH="/app:${PYTHONPATH}"

ENV SPARK_HOME=/opt/spark

RUN useradd --create-home --shell /bin/bash sparkuser

USER sparkuser

ENTRYPOINT ["bash", "/app/spark/init.sh"]

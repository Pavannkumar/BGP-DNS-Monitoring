FROM apache/spark:3.5.0
USER root
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2 psycopg2-binary==2.9.9 pyspark==3.5.0
RUN mkdir -p /opt/spark/jars && curl -o /opt/spark/jars/spark-sql-kafka.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && curl -o /opt/spark/jars/kafka-clients.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && curl -o /opt/spark/jars/spark-token-provider.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && curl -o /opt/spark/jars/commons-pool2.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && curl -o /opt/spark/jars/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar
COPY bgp_processor.py .
COPY dns_processor.py .
COPY entrypoint.sh .
ENV PYTHONUNBUFFERED=1
CMD ["python", "bgp_processor.py"]

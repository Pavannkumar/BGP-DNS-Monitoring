#!/bin/bash
SUBMIT="/opt/spark/bin/spark-submit --master local[*] --jars /opt/spark/jars/spark-sql-kafka.jar,/opt/spark/jars/kafka-clients.jar,/opt/spark/jars/spark-token-provider.jar,/opt/spark/jars/commons-pool2.jar,/opt/spark/jars/postgresql.jar"
$SUBMIT /app/bgp_processor.py &
$SUBMIT /app/dns_processor.py &
wait
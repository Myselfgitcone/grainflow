FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir pyyaml==6.0.2 \
 && mkdir -p /opt/spark/ivy /checkpoints \
 && chown -R spark:spark /opt/spark/ivy /checkpoints

COPY spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY spark/jobs /opt/grainflow/jobs
COPY src/grainflow /opt/grainflow/src/grainflow
COPY contracts /opt/grainflow/contracts

ENV PYTHONPATH=/opt/grainflow/src \
    GRAINFLOW_CONTRACTS_DIR=/opt/grainflow/contracts \
    AWS_REGION=us-east-1

USER spark
WORKDIR /opt/grainflow

# Connector jars resolve from Maven on first start and are cached in the ivy volume.
ENV SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.postgresql:postgresql:42.7.4

CMD ["sh", "-c", "/opt/spark/bin/spark-submit --master local[2] --packages $SPARK_PACKAGES jobs/stream_to_bronze.py"]

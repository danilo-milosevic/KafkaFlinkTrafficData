# Dockerfile

# Base image
FROM bitnami/spark:3.2.0

# Install Kafka client for Spark integration
ENV KAFKA_VERSION=2.8.0
ENV SCALA_VERSION=2.12
ENV KAFKA_HOME=/opt/bitnami/kafka

RUN curl -sSL https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz | tar zx -C /opt/bitnami && \
    mv /opt/bitnami/kafka_$SCALA_VERSION-$KAFKA_VERSION $KAFKA_HOME

# Set Kafka dependencies in Spark
ENV PATH=$PATH:$KAFKA_HOME/bin

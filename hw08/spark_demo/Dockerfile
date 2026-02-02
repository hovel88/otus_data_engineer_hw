FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y curl procps openjdk-21-jdk \
 && rm -rf /var/lib/apt/lists/*

# Устанавливаем переменные окружения для Java
ENV JAVA21_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем переменные окружения для Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_VERSION=3.5.7
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Скачиваем и устанавливаем Spark
RUN mkdir -p /opt \
 && cd /opt \
 && curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" | tar -xz \
 && ln -s "spark-${SPARK_VERSION}-bin-hadoop3" spark

# Копируем конфигурацию Spark
COPY conf/spark-defaults.conf conf/spark-env.sh conf/log4j2.properties ${SPARK_HOME}/conf/

WORKDIR /app
EXPOSE 4040
CMD ["/bin/bash"]

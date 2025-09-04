FROM apache/airflow:slim-3.0.1-python3.10
LABEL maintainer="Alisahib Gadimov"

USER root

COPY init-airflow.sh init-airflow.sh
RUN chmod +x init-airflow.sh

RUN apt-get update && \
    mkdir -p /usr/share/man/man1 && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    libzbar-dev \
    bash \
    gcc \
    git \
    libc-dev \
    curl \
    wget \
    vim-tiny \
    nano \
    iputils-ping \
    telnet \
    openssh-client \
    openssh-server \
    net-tools \
    man \
    unzip \
    bc \
    thrift-compiler \
    sudo \
    build-essential && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install Spark 3.5.0
RUN curl -L -o spark-3.5.0-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Add Hadoop-AWS, AWS SDK, and PostgreSQL JDBC driver
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -o /opt/spark/jars/postgresql-42.3.5.jar https://jdbc.postgresql.org/download/postgresql-42.3.5.jar

RUN mkdir -p /opt/spark/history
RUN chmod 777 -R /opt/spark/history

ENV PYSPARK_PYTHON /usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON /usr/local/bin/python

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY delta-core_2.12-2.4.0.jar /opt/spark/jars

USER airflow

COPY requirements.txt requirements.txt

RUN pip install apache-airflow==3.0.1 -r requirements.txt

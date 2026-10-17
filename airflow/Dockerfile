FROM apache/airflow:2.10.5-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        netcat-openbsd \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Arch-independent JAVA_HOME (amd64 / arm64): link the installed JRE to /opt/java
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java && \
    mkdir -p /opt/airflow/.ivy2 && \
    chown -R 50000:0 /opt/airflow/.ivy2

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    apache-airflow-providers-apache-spark \
    pymongo \
    psycopg2-binary \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt && \
    pip install --no-cache-dir pyspark==3.5.9

ENTRYPOINT ["/entrypoint.sh"]

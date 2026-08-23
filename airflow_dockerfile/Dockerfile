FROM apache/airflow:2.10.5-python3.11

# JDK нужен spark-submit: Spark гоняется в режиме local[*] внутри воркера.
# jre-headless вместо полного jdk — компилятор в рантайме не нужен.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Constraints фиксируют версии транзитивных зависимостей Airflow: без них pip
# способен подтянуть несовместимый с 2.10.5 набор провайдеров.
ENV AIRFLOW_VERSION=2.10.5
ENV PYTHON_VERSION=3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /requirements.txt

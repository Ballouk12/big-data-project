FROM apache/zeppelin:0.10.1

USER root

# Variables d'environnement
ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Installation de wget, gcc et outils de compilation pour happybase
RUN apt-get update && \
    apt-get install -y wget procps gcc g++ python3-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

# Créer l'utilisateur zeppelin s'il n'existe pas
RUN id -u zeppelin &>/dev/null || useradd -r -u 1000 -g root zeppelin

# Télécharger et installer Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Installer happybase pour accéder à HBase
RUN pip install happybase

# Créer les répertoires nécessaires
RUN mkdir -p /opt/zeppelin/notebook && \
    mkdir -p /opt/spark-apps && \
    mkdir -p /data

# Configuration des permissions
RUN chown -R 1000:0 /opt/zeppelin && \
    chown -R 1000:0 ${SPARK_HOME} && \
    chown -R 1000:0 /opt/spark-apps && \
    chown -R 1000:0 /data && \
    chmod -R g+rw /opt/zeppelin && \
    chmod -R g+rw ${SPARK_HOME} && \
    chmod -R g+rw /opt/spark-apps && \
    chmod -R g+rw /data

USER 1000

WORKDIR /opt/zeppelin

EXPOSE 8080

CMD ["bin/zeppelin.sh"]
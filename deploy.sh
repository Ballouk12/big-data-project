#!/bin/bash

echo "=========================================="
echo "🚀 DÉPLOIEMENT ZEPPELIN + SPARK + HBASE"
echo "=========================================="

# 1. Arrêter les services existants
echo ""
echo "🛑 Arrêt des services..."
docker-compose down

# 2. Créer la structure de dossiers
echo ""
echo "📁 Création de la structure de dossiers..."
mkdir -p zeppelin/conf
mkdir -p zeppelin/notebooks
mkdir -p spark_apps

# 3. Copier la configuration Zeppelin
echo ""
echo "📋 Configuration Zeppelin..."
cat > zeppelin/conf/interpreter.json <<'EOF'
{
  "interpreterSettings": {
    "spark": {
      "id": "spark",
      "name": "spark",
      "group": "spark",
      "properties": {
        "spark.master": {
          "name": "spark.master",
          "value": "spark://spark-master-bd:7077",
          "type": "string"
        },
        "spark.submit.deployMode": {
          "name": "spark.submit.deployMode",
          "value": "client",
          "type": "string"
        },
        "spark.app.name": {
          "name": "spark.app.name",
          "value": "Zeppelin",
          "type": "string"
        },
        "spark.executor.memory": {
          "name": "spark.executor.memory",
          "value": "1g",
          "type": "string"
        },
        "spark.executor.cores": {
          "name": "spark.executor.cores",
          "value": "1",
          "type": "number"
        },
        "spark.cores.max": {
          "name": "spark.cores.max",
          "value": "2",
          "type": "string"
        },
        "spark.jars.packages": {
          "name": "spark.jars.packages",
          "value": "org.apache.hbase:hbase-client:2.4.9,org.apache.hbase:hbase-common:2.4.9,org.apache.hbase:hbase-server:2.4.9",
          "type": "string"
        },
        "SPARK_HOME": {
          "name": "SPARK_HOME",
          "value": "/opt/spark",
          "type": "string"
        }
      },
      "status": "READY",
      "interpreterGroup": [
        {
          "name": "spark",
          "class": "org.apache.zeppelin.spark.SparkInterpreter",
          "defaultInterpreter": true
        },
        {
          "name": "sql",
          "class": "org.apache.zeppelin.spark.SparkSqlInterpreter",
          "defaultInterpreter": false
        }
      ],
      "option": {
        "remote": true,
        "port": -1,
        "perNote": "shared",
        "perUser": "shared"
      }
    }
  }
}
EOF

# 4. Créer le Dockerfile Zeppelin
echo ""
echo "🐳 Création du Dockerfile Zeppelin..."
cat > zeppelin/Dockerfile <<'EOF'
FROM apache/zeppelin:0.10.1

USER root

ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN mkdir -p /opt/zeppelin/notebook && \
    mkdir -p /opt/spark-apps && \
    mkdir -p /data

RUN chown -R zeppelin:zeppelin /opt/zeppelin && \
    chown -R zeppelin:zeppelin ${SPARK_HOME} && \
    chown -R zeppelin:zeppelin /opt/spark-apps && \
    chown -R zeppelin:zeppelin /data

COPY --chown=zeppelin:zeppelin conf/* /opt/zeppelin/conf/ 2>/dev/null || true

USER zeppelin

WORKDIR /opt/zeppelin

EXPOSE 8080

CMD ["bin/zeppelin.sh"]
EOF

# 5. Build et démarrage
echo ""
echo "🔨 Build de l'image Zeppelin (peut prendre quelques minutes)..."
docker-compose build zeppelin

echo ""
echo "🚀 Démarrage des conteneurs..."
docker-compose up -d --build

echo "⏳ Attente de 15s pour l'initialisation..."
sleep 15

echo "🔌 Démarrage du serveur Thrift HBase..."
docker exec -d hbase-standalone-bd hbase thrift start
echo "✅ Serveur Thrift démarré"

echo "🔄 Redémarrage du producer pour prendre en compte HBase..."
docker restart producer-bd
echo "⏳ Attente du démarrage (60 secondes)..."
sleep 60

echo ""
echo "📊 Statut des conteneurs:"
docker-compose ps

echo ""
echo "=========================================="
echo "✅ DÉPLOIEMENT TERMINÉ!"
echo "=========================================="
echo ""
echo "🌐 URLs d'accès:"
echo "   Zeppelin:     http://localhost:8888"
echo "   Spark Master: http://localhost:8080"
echo "   HBase:        http://localhost:16010"
echo "   Hadoop:       http://localhost:9870"
echo ""
echo "📝 Logs Zeppelin:"
echo "   docker-compose logs -f zeppelin"
echo ""
echo "🔍 Test Spark dans Zeppelin:"
echo "   1. Ouvrir http://localhost:8888"
echo "   2. Créer un nouveau notebook"
echo "   3. Exécuter: %spark"
echo "                val df = spark.range(5)"
echo "                df.show()"
echo ""
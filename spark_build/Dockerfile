FROM bde2020/spark-master:3.1.1-hadoop3.2

ENV SPARK_HOME=/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip

# Install dependencies
RUN apk add --no-cache python3-dev py3-numpy py3-pip build-base && \
    pip3 install --upgrade pip && \
    pip3 install happybase thrift && \
    rm -rf /root/.cache

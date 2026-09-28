# Big Data Weather Pipeline: Real-time Ingestion & Machine Learning

A comprehensive big data project featuring a robust real-time data pipeline, lambda architecture for batch and speed layers, and integrated Machine Learning for weather prediction.

---

## 📋 Overview

This project provides a complete end-to-end data platform :

1.  **Real-time Data Ingestion**: Automated collection of weather data using Kafka and Flume.
2.  **Hybrid Processing Architecture**: Leveraging Apache Spark for both streaming inference and batch model training.
3.  **Scalable Storage**: Distributed storage using Hadoop HDFS and NoSQL capabilities with HBase.
4.  **Interactive Analytics**: Zeppelin notebooks for data visualization and model management.

# Architecture of our project

<!-- Replace with your specific architecture execution flow if different -->
![architecture](https://github.com/user-attachments/assets/c81347bc-33d8-48b2-bc50-f46faa90f602)


## 🏗️ Project Structure

```
big-data/
├── producer/                  # Data Ingestion Layer
│   ├── producer_kafka.py      # Real-time Open-Meteo fetcher
│   ├── extract_final.py       # Batch data extraction script
│   └── Dockerfile             # Custom Producer image
│
├── spark_apps/                # Spark Processing Layer
│   ├── realtime_inference.py  # Structured Streaming & ML Inference
│   
│        
│
├── flume/                     # Log Aggregation
│   └── conf/                  # Flume agent configuration
│
├── zeppelin/                  # Analytics & Interactive
│   └── notebooks/             # Data exploration notebooks
│
├── scripts/                   # Utility Scripts
│   └── init_hbase.txt         # HBase schema initialization
│
├── docker-compose.yml         # Container Orchestration
└── deploy.sh                  # Deployment automation
```

## 📚 Dataset

This project consumes live data from the **Open-Meteo API**:
👉 [Open-Meteo Free Weather API](https://open-meteo.com/)

**Description:**
> The pipeline fetches real-time meteorological data including temperature, wind speed, weather codes, and more. This data is used effectively to simulate a continuous stream of IoT sensor data for Big Data processing.

**Key Features:**
- Real-time updates (Temperature, Wind Speed, Weather Code)
- Geolocation-based forecasting
- High-frequency data simulating sensor streams
- Suitable for Time-series forecasting and Anomaly detection

## 🚀 Quick Start

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 10GB RAM minimum (16GB recommended for full stack)

### Setting Up the Pipeline

1.  **Clone the repository**
    ```bash
    git clone https://github.com/Ballouk12/big-data-project.git
    cd big-data
    ```

2.  **Launch the Services**
    The entire stack is containerized. Start it with one command:
    ```bash
    docker-compose up -d
    ```

3.  **Verify Services**
    - **Namenode**: http://localhost:9870
    - **Spark Master**: http://localhost:8080
    - **Zeppelin**: http://localhost:9999 (mapped from internal 8080)

### Running the Workflows

1.  **Start Data Ingestion (Producer)**
    The producer is configured to restart always, but you can view logs or restart manually:
    ```bash
    docker-compose restart producer-kafka
    # View logs
    docker-compose logs -f kafka-producer
    ```

2.  **Submit Spark Streaming Job**
    Connect to the Spark Master container to submit the inference job:
    ```bash
    docker exec -it spark-master-bd bash
    
    # Submit the Real-time Inference Job
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
      /opt/spark-apps/realtime_inference.py
    ```

3.  **Train/Retrain Model**
    ```bash
    # GO to Zeeplin interface and run the TRAINING_DATASET_ML notebook
    ```

## 📊 Pipeline Components & Performance

The project implements a modern Lambda Architecture:

-   **Speed Layer (Spark Structured Streaming)**:
    -   Consumes immediately from `weather-live` Kafka topic.
    -   Applies pre-trained Random Forest models.
    -   Latency: < 500ms per micro-batch.

-   **Batch Layer (Hadoop/HBase)**:
    -   Persists historical data in HBase `weather_history` table.
    -   Supports complex batch analytics and model retraining.

-   **Orchestration**:
    -   Docker Compose manages dependency graphs (Zookeeper -> Kafka -> Spark).

## 🔧 Features

-   **Fault Tolerance**: Kafka buffering ensures no data loss during processing spikes.
-   **Distributed ML**: Spark MLlib used for distributed training and inference.
-   **NoSQL Integration**: High-throughput writes to HBase for historical archiving.
-   **Dynamic Dashboarding**: Zeppelin notebooks connected to Spark context for live visualization.

## 📦 Data Management

### Supported Formats & Storage
-   **Apache Kafka**: JSON serialized messages (Topics: `weather-live`)
-   **HBase**: Key-Value storage for infinite scaling.
-   **HDFS**: Checkpointing and model artifacts.

### Data Directories (Docker Volumes)
```
volumes/
├── hbase-data/       # Persistent HBase storage
├── namenode/         # HDFS NameNode metadata
├── datanode/         # HDFS Data blocks
└── shared-data/      # Shared artifacts between containers
```

## 🛠️ Development

### Adding a New Feature

1.  **Modify the Producer**:
    Edit `producer/producer_kafka.py` to add new fields from the API.

2.  **Deploy**:
    Restart the specific containers to apply changes:
    ```bash
    docker-compose restart kafka-producer spark-worker
    ```

## 📈 Monitoring & Troubleshooting

### View Logs
```bash
# Kafka Producer logs (Check data flow)
docker-compose logs -f kafka-producer

# Spark Worker logs (Check job execution)
docker-compose logs -f spark-worker
```

### Common Issues

**"Connection Refused" to HBase**:
Ensure the HBase container is fully healthy before starting consumers.
```bash
docker-compose ps hbase-standalone
```

**Spark "Class Not Found"**:
Ensure you include the Kafka package when submitting jobs:
`--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2`

## 📚 Documentation

-   [Apache Spark Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
-   [Open-Meteo API Docs](https://open-meteo.com/en/docs)
-   [HBase Reference Guide](https://hbase.apache.org/book.html)

## 🤝 Contributing

Contributions are welcome! Please:

1.  Fork the repository
2.  Create a feature branch (`git checkout -b feature/amazing-feature`)
3.  Commit your changes (`git commit -m 'Add amazing feature'`)
4.  Push to the branch (`git push origin feature/amazing-feature`)
5.  Open a Pull Request

## 📝 License

This project is licensed under the MIT License.

## 👥 Authors

-   **Ballouk Mohamed** - [GitHub Profile](https://github.com/Ballouk12)

## 🙏 Acknowledgments

-   Apache Software Foundation for open-source big data tools.
-   Docker community for containerization resources.
-   Open-Meteo for the excellent free weather API.

---

**⭐ Star this repository if you find it helpful!**

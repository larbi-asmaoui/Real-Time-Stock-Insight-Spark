# ⚡ Real-Time Stock Insight & Prediction Platform

A scalable, end-to-end data engineering and machine learning platform that ingests real-time stock market data, processes it using Spark Structured Streaming & Delta Lake, and generates AI-driven price predictions using an LSTM neural network. Results are visualized in a live Streamlit dashboard.

---

## 🖼️ Dashboard Preview

<!-- PASTE DASHBOARD SCREENSHOT HERE -->

![Dashboard Preview](screenshots/001.png)

<!-- PASTE ARCHITECTURE DIAGRAM HERE -->

_(Add screenshot: Architecture Diagram)_

---

## 🚀 Features

- **Real-Time Ingestion**: Fetches live stock data (Yahoo Finance/AlphaVantage) and pushes to **Redpanda (Kafka)**.
- **Lakehouse Architecture**: Implements a Medallion Architecture (Bronze used for raw, Silver for clean, Gold for aggregated) using **Delta Lake** on **MinIO (S3)**.
- **Spark Structured Streaming**: Stateful processing, watermark handling, and windowed aggregations.
- **MLOps Integration**:
  - **Feature Engineering**: Real-time feature computation (Volatility, Log Returns).
  - **Deep Learning**: LSTM model training and inference using **PyTorch**.
  - **Model Tracking**: Experiments and models tracked via **MLflow**.
- **Interactive Dashboard**: **Streamlit** app powered by **DuckDB** for low-latency queries on Delta tables.
- **Infrastructure as Code**: Entire stack dockerized with **Docker Compose**.

---

## 🛠️ Tech Stack

- **Ingestion**: Python, Kafka Producer
- **Message Broker**: Redpanda (Kafka compatible)
- **Processing**: Apache Spark 3.4 (Structured Streaming)
- **Storage**: MinIO (S3 compatible object storage)
- **Table Format**: Delta Lake 2.4
- **Machine Learning**: PyTorch (LSTM), MLflow
- **Visualization**: Streamlit, Plotly, DuckDB
- **Containerization**: Docker, Docker Compose

---

## 🏗️ Architecture Overview

The pipeline follows a modern "Medallion" Lakehouse architecture:

1.  **Source**: `kafka_producer` fetches ticks and publishes to `stock_prices` topic.
2.  **Bronze Layer**: Spark reads Kafka, dumps raw bytes/json to Delta table (`s3a://finance-lake/lake/bronze`).
3.  **Silver Layer**: Deduplicates data, enforces schema, and cleans types (`s3a://finance-lake/lake/silver`).
4.  **Gold Layer**: Aggregates data into time windows (e.g., 1-minute candles) and calculates financial metrics (`s3a://finance-lake/lake/gold`).
5.  **Machine Learning**:
    - **Training**: LSTM model trains on historical Gold data.
    - **Inference**: Real-time service consumes Gold data, predicts price direction, and writes to `predictions` table.
6.  **Serving**: Streamlit connects to MinIO via DuckDB to serve the dashboard.

---

## ⚙️ Setup & Installation

### Prerequisites

- Docker & Docker Compose installed.
- (Optional) `git` installed.

### 1. Clone the Repository

```bash
git clone https://github.com/larbi-asmaoui/Real-Time-Stock-Insight-Spark.git
cd Real-Time-Stock-Insight-Spark
```

### 2. Configure Environment

The project comes with reasonable defaults. If you need API keys (e.g., for AlphaVantage), create a `.env` file (optional for Yahoo Finance):

```bash
cp .env.example .env
# Edit .env and add your keys if necessary
```

### 3. Start the Platform

Run the entire stack in detached mode:

```bash
docker-compose up -d
```

_Note: The first run may take a few minutes to build the Spark and Scraper images._

---

## 🖥️ Usage

### Accessing Services

| Service           | URL                     | Description                                 |
| :---------------- | :---------------------- | :------------------------------------------ |
| **Dashboard**     | `http://localhost:8501` | Main Streamlit UI                           |
| **MinIO Console** | `http://localhost:9001` | Object Storage UI (User/Pass: `minioadmin`) |
| **MLflow UI**     | `http://localhost:5000` | Model Registry & Experiment Tracking        |
| **Redpanda**      | `localhost:8081`        | Schema Registry (if enabled)                |

### Triggering ML Training

To retrain the LSTM model on the latest data:

```bash
docker exec -it spark-streaming python3 /app/src/ml/train_lstm.py
```

### Checking Logs

To monitor the streaming process:

```bash
docker-compose logs -f spark-streaming
```

---

## 📂 Project Structure

```plaintext
├── docker-compose.yml       # Infrastructure definition
├── Dockerfile.spark         # Custom Spark image with Jars
├── Dockerfile.dashboard     # Streamlit app image
├── src/
│   ├── ingestion/           # Kafka Producers
│   ├── processing/          # Spark Structured Streaming Jobs
│   ├── ml/                  # PyTorch LSTM Model & Inference
│   ├── dashboard/           # Streamlit App
│   └── debug_*.py           # Utilities for inspecting Delta tables
├── jars/                    # Spark dependencies (Delta, Kafka, Hadoop-AWS)
├── data/                    # Local mount for MLflow/Checkpoints
└── logs/                    # Application logs
```

---

## 📸 Screenshots

### 1. Real-Time Dashboard

<!-- PASTE DASHBOARD IMG HERE -->

_(Add screenshot showing the main view)_

### 2. MLflow Experiment Tracking

<!-- PASTE MLFLOW IMG HERE -->

![MLflow](screenshots/003.png)
_(Add screenshot showing loss curves)_

### 3. MinIO Data Lake Buckets

<!-- PASTE MINIO IMG HERE -->

![MinIO](screenshots/002.png)
_(Add screenshot showing bronze/silver/gold folders)_

---

## 🤝 Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.

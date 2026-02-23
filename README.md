# Crypto Data Pipeline - End-to-End Data Engineering Project

## 📋 Project Overview
This project implements a fully automated ETL pipeline for cryptocurrency market data, ingesting raw data from CoinGecko API, transforming it into structured staging and mart layers, and providing BI-ready datasets for dashboards.

The pipeline is production-ready, containerized with Docker, orchestrated with Apache Airflow, and includes analytics and visualization with Apache Superset.

**Key features:**
- Automatic daily ingestion of cryptocurrency market data.
- Data storage in **MinIO** (S3-compatible) and **PostgreSQL**.
- Incremental transformations using **dbt**.
- Enriched, clean, and BI-ready datasets for analytics.
- Easy deployment via Docker Compose.

---

## 🛠️ Tech Stack
| Layer | Technology |
|-------|------------|
| Orchestration | Apache Airflow (LocalExecutor) |
| Storage | MinIO (S3), PostgreSQL |
| Transformation | dbt (Data Build Tool) |
| BI / Visualization | Apache Superset |
| Infrastructure | Docker, Docker Compose |
| Data Source | CoinGecko API |

---

## 📁 Project Structure

```text
crypto-data-pipeline/
├── airflow/             # Airflow DAGs, configs, logs, plugins
├── dbt/                 # dbt project: models, seeds, snapshots, macros
├── docker/              # Dockerfiles for dbt & Superset
├── minio/               # Minio data
├── scripts/             # Utility scripts (setup, migrations, etc.)
├── tests/               # Unit and integration tests
├── docs/                # Project documentation and images
├── docker-compose.yml   # Multi-service orchestration
├── .env.example         # Environment variables template
├── .gitignore
├── LICENSE
└── README.md
```

## 🔄 ETL Pipeline

The pipeline consists of **3 main layers**:

### 1. Raw Layer
- DAG: `load_coingecko_raw_daily`
- Fetches daily cryptocurrency market data from **CoinGecko API**.
- Stores JSON data as **Parquet files in MinIO**.
- Example DAG in Airflow:  
  ![Airflow Raw Layer DAG](docs/images/airflow_raw_dag.png)

### 2. Staging Layer
- DAG: `load_coingecko_staging_daily`
- Reads Parquet files from MinIO.
- Loads data into **PostgreSQL staging tables** (`stg_coingecko_markets`).
- Performs basic validation and logs row counts.
- Example staging DAG run:  
  ![Airflow Staging DAG](docs/images/airflow_staging_dag.png)

### 3. Mart Layer (Transformation)
- DAG: `transform_crypto_mart_daily`
- Triggers **dbt transformations** in Docker container.
- Incremental models generate:
  - Fact table: `fct_market_daily`
  - Dimension table: `dim_coin`
- Creates enriched BI-ready view: `vw_market_daily_enriched`.
- Example dbt run:  
  ![dbt Run](docs/images/dbt_run.png)

---

## 📊 BI & Visualization

- **Apache Superset** is connected to PostgreSQL DWH.
- Provides dashboards for crypto market analytics:
  - Price trends
  - Market capitalization analysis
  - Volume and supply insights
- Example Superset dashboard:  
  ![Superset Dashboard](docs/images/superset_dashboard.png)

---
## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git
- Recommended system: 4GB RAM, 2 CPUs

### Installation & Run

1. Clone the repository:
```bash
git clone https://github.com/naugtymor/crypto-data-project.git
cd crypto-data-pipeline
```
2. Copy environment variables:
```bash
cp .env.example .env
# Edit .env if needed
```
3. Start the full stack:
```bash
docker-compose up -d
```
4. Initialize Airflow (first time only):
```bash
docker-compose run --rm airflow-init
```

### Access Services

You can access the running services using the following URLs:

| Service     | URL                       |
|------------|---------------------------|
| **Airflow** | [http://localhost:8080](http://localhost:8080) |
| **Superset** | [http://localhost:8088](http://localhost:8088) |
| **MinIO**   | [http://localhost:9001](http://localhost:9001) |

---

### ⚡ Testing & Validation

- dbt models include schema tests (`not_null`, `unique`, `relationships`) for data integrity.  
- Airflow logs provide detailed DAG execution metrics.  
- Data can be queried in Superset dashboards for validation.  

---

### 📂 Data Flow Diagram

*(Placeholder for ETL/Data pipeline diagram image)*

---

### 📌 Notes

- All services are fully containerized for reproducibility.  
- MinIO stores raw Parquet files; PostgreSQL acts as staging + mart layer.  
- Incremental dbt transformations optimize loads and avoid duplicates.  
- Superset dashboards provide real-time analytics for cryptocurrency markets.  

---

### 📖 References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)  
- [Apache Superset Documentation](https://superset.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)  
- [CoinGecko API](https://www.coingecko.com/en/api)
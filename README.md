# Crypto Data Pipeline - Data Engineering Project (END-to-END)

## 📋 Project Overview
A complete ETL pipeline for cryptocurrency market data analysis using data engineering tools.

## 🛠️ Tech Stack
- **Orchestration**: Apache Airflow
- **Storage**: MinIO (S3-compatible), PostgreSQL
- **Processing**: dbt (Data Build Tool)
- **BI/Visualization**: Apache Superset
- **Infrastructure**: Docker, Docker Compose
- **Data Source**: CoinGecko API

## 📁 Project Structure

crypto-data-pipeline/
├── airflow/ # Airflow DAGs and configurations
├── dbt/ # dbt models and transformations
├── docker/ # Docker configurations
├── docs/ # Documentation
├── scripts/ # Utility scripts
├── tests/ # Unit and integration tests
├── docker-compose.yml
├── .env.example # Environment variables template
├── .gitignore
├── LICENSE
└── README.md


## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git

### Installation
1. Clone the repository
```bash
git clone https://github.com/naugtymor/crypto-data-project.git
cd crypto-data-pipeline
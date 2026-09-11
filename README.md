

````markdown
# Dockerized Stock Market Data Pipeline

A Dockerized stock market data pipeline built using **Apache Airflow, Python, PostgreSQL, Docker, and the Alpha Vantage API**.

The pipeline fetches stock market data in JSON format, extracts the required information, and stores or updates it in PostgreSQL. Apache Airflow manages the workflow and runs the pipeline on a daily schedule.

## Tech Stack

- **Python** – Data fetching and processing
- **Apache Airflow** – Workflow orchestration and scheduling
- **PostgreSQL** – Data storage
- **Alpha Vantage API** – Stock market data source
- **Docker & Docker Compose** – Containerization and deployment
- **Requests** – API communication
- **psycopg2** – PostgreSQL connectivity

## Pipeline

```text
Alpha Vantage API
       ↓
fetch_stock_data
       ↓
extract_stock_data
       ↓
update_database
       ↓
PostgreSQL (stock_prices)
````

## Project Structure

```text
docker-airflow-stock-pipeline/
│
├── dags/
│   └── stock_market_dag.py
│
├── scripts/
│   └── stock_pipeline.py
│
├── sql/
│   └── init.sql
│
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Prerequisites

* Docker Desktop / Docker Engine
* Docker Compose
* Git
* Alpha Vantage API key

Get a free Alpha Vantage API key:

[https://www.alphavantage.co/support/#api-key](https://www.alphavantage.co/support/#api-key)

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/abhay9561/docker-airflow-stock-pipeline.git
cd docker-airflow-stock-pipeline
```

### 2. Create the environment file

**Windows PowerShell:**

```powershell
Copy-Item .env.example .env
```

**Linux/macOS:**

```bash
cp .env.example .env
```

### 3. Configure `.env`

Add your Alpha Vantage API key and configuration:

```env
ALPHAVANTAGE_API_KEY=your_api_key_here
ALPHAVANTAGE_BASE_URL=https://www.alphavantage.co/query

STOCK_SYMBOLS=IBM,AAPL,MSFT

POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=stocks

AIRFLOW_DB=airflow

PG_HOST=postgres
PG_PORT=5432

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

Do not commit the `.env` file or expose your API key.

## Run the Pipeline

Build and start the services:

```bash
docker compose up -d --build
```

Check the containers:

```bash
docker compose ps
```

## Airflow

Open the Airflow web interface:

[http://localhost:8082](http://localhost:8082)

The DAG is:

```text
stock_market_pipeline
```

Schedule:

```text
@daily
```

The DAG contains three tasks:

```text
fetch_stock_data
       ↓
extract_stock_data
       ↓
update_database
```

The DAG can also be triggered manually from the Airflow interface.

## PostgreSQL

Database:

```text
stocks
```

Table:

```text
stock_prices
```

To view the stored data:

```bash
docker compose exec postgres psql -U postgres -d stocks -c "SELECT * FROM stock_prices ORDER BY symbol;"
```

The table uses `(symbol, trade_date)` as the primary key, allowing existing records to be updated instead of creating duplicates.

## Error Handling

The pipeline handles API errors, invalid responses, missing data, and database failures.

If a stock symbol does not return usable data, it is handled gracefully and the pipeline continues processing the available data.

## Stop the Pipeline

Stop the containers:

```bash
docker compose down
```

To stop the containers and remove the database volume:

```bash
docker compose down -v
```

> `docker compose down -v` removes the stored PostgreSQL data.

## Repository

GitHub:
[https://github.com/abhay9561/docker-airflow-stock-pipeline](https://github.com/abhay9561/docker-airflow-stock-pipeline)
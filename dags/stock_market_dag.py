"""Airflow DAG orchestrating the stock market pipeline.

    fetch_stock_data -> extract_stock_data -> update_database

The work itself lives in scripts/stock_pipeline.py, which is importable because
docker-compose.yml puts /opt/airflow/scripts on PYTHONPATH.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from stock_pipeline import extract_quotes, fetch_quotes, symbol_list, upsert_quotes

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


@dag(
    dag_id="stock_market_pipeline",
    description="Fetch stock quotes from Alpha Vantage and update PostgreSQL",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["stocks"],
)
def stock_market_pipeline():
    @task
    def fetch_stock_data():
        payloads = fetch_quotes(symbol_list())
        if not payloads:
            raise RuntimeError("No symbol could be fetched from the API")
        return payloads

    @task
    def extract_stock_data(payloads):
        rows = extract_quotes(payloads)
        if not rows:
            raise RuntimeError("API responses contained no usable stock data")
        return rows

    @task
    def update_database(rows):
        return upsert_quotes(rows)

    update_database(extract_stock_data(fetch_stock_data()))


stock_market_pipeline()

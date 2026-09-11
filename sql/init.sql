-- Runs once, on the first start of the postgres container.
-- Executed against POSTGRES_DB (the "stocks" database).

-- Separate database for the Airflow metadata, so pipeline data stays clean.
-- Keep this name in sync with AIRFLOW_DB in .env.
CREATE DATABASE airflow;

-- The table the pipeline updates. It exists before the DAG ever runs, so the
-- pipeline only ever inserts/updates rows - it never creates the schema.
CREATE TABLE IF NOT EXISTS stock_prices (
    symbol          VARCHAR(20)     NOT NULL,
    trade_date      DATE            NOT NULL,
    open_price      NUMERIC(18, 4),
    high_price      NUMERIC(18, 4),
    low_price       NUMERIC(18, 4),
    close_price     NUMERIC(18, 4),
    volume          BIGINT,
    previous_close  NUMERIC(18, 4),
    change_amount   NUMERIC(18, 4),
    change_percent  NUMERIC(10, 4),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_date)
);

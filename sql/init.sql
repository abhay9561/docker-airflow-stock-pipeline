CREATE DATABASE airflow;

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

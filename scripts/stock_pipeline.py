"""Fetch stock market data from Alpha Vantage and update the PostgreSQL table.

The module is used by the Airflow DAG (dags/stock_market_dag.py) and can also
be executed directly:

    python stock_pipeline.py

Pipeline steps:

    fetch_quotes(symbols)    -> raw JSON payloads from the API (requests)
    extract_quotes(payloads) -> flat rows with the relevant data points
    upsert_quotes(rows)      -> UPSERT into the existing stock_prices table

All configuration is read from environment variables - no secrets in code.
"""

import logging
import os

import psycopg2
import requests
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# --- Configuration from environment variables ------------------------------
API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
BASE_URL = os.environ.get("ALPHAVANTAGE_BASE_URL", "https://www.alphavantage.co/query")
SYMBOLS = os.environ.get("STOCK_SYMBOLS", "IBM")

DB_CONFIG = {
    "host": os.environ.get("PG_HOST", "postgres"),
    "port": os.environ.get("PG_PORT", "5432"),
    "dbname": os.environ.get("POSTGRES_DB", "stocks"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
}

TABLE_NAME = "stock_prices"
REQUEST_TIMEOUT = 30

# Column order used when writing rows to the table.
COLUMNS = (
    "symbol",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "previous_close",
    "change_amount",
    "change_percent",
)


def symbol_list():
    """Return the configured symbols as a clean list."""
    return [symbol.strip().upper() for symbol in SYMBOLS.split(",") if symbol.strip()]


# --- Value cleaning --------------------------------------------------------
def _text(value):
    """Return a stripped string, or None when the value is missing or empty."""
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _number(value):
    """Validate a numeric field and return it as a string, or None if unusable.

    Numbers are kept as strings so PostgreSQL parses them into NUMERIC without
    any floating point rounding. Percentages arrive from the API as "0.7042%".
    """
    value = _text(value)
    if value is None:
        return None
    value = value.rstrip("%")
    try:
        float(value)
    except ValueError:
        logger.warning("Ignoring non-numeric value: %r", value)
        return None
    return value


def _integer(value):
    """Validate an integer field, or return None if unusable."""
    value = _number(value)
    if value is None:
        return None
    return int(float(value))


def _redact(message):
    """Keep the API key out of the logs.

    Request errors include the full request URL, which carries the key.
    """
    message = str(message)
    if API_KEY:
        message = message.replace(API_KEY, "***")
    return message


# --- Step 1: fetch from the API --------------------------------------------
def fetch_quote(symbol):
    """Call the Alpha Vantage GLOBAL_QUOTE endpoint for one symbol.

    Returns the parsed JSON body, or None if the request could not be made.
    """
    if not API_KEY:
        raise RuntimeError("ALPHAVANTAGE_API_KEY environment variable is not set")

    params = {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": API_KEY}
    try:
        response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.JSONDecodeError as error:
        # Checked before RequestException, which it inherits from.
        logger.error(
            "API response for %s was not valid JSON: %s", symbol, _redact(error)
        )
        return None
    except requests.exceptions.RequestException as error:
        logger.error("API request failed for %s: %s", symbol, _redact(error))
        return None


def fetch_quotes(symbols):
    """Fetch every symbol. A symbol that fails is skipped, not fatal."""
    payloads = {}
    for symbol in symbols:
        payload = fetch_quote(symbol)
        if payload is None:
            continue
        payloads[symbol] = payload
    logger.info("Fetched %d of %d symbol(s)", len(payloads), len(symbols))
    return payloads


# --- Step 2: parse the JSON and extract the data points --------------------
def extract_quote(payload):
    """Extract the relevant data points from one GLOBAL_QUOTE response.

    Returns a row dict, or None when the response holds no usable quote.
    """
    if not payload:
        return None

    # Alpha Vantage reports rate limits and bad requests with a message field
    # instead of an HTTP error status.
    for key in ("Error Message", "Note", "Information"):
        if key in payload:
            logger.error("API returned a message instead of data: %s", payload[key])
            return None

    quote = payload.get("Global Quote")
    if not quote:
        logger.warning("Response contained no quote data: %s", payload)
        return None

    row = {
        "symbol": _text(quote.get("01. symbol")),
        "trade_date": _text(quote.get("07. latest trading day")),
        "open_price": _number(quote.get("02. open")),
        "high_price": _number(quote.get("03. high")),
        "low_price": _number(quote.get("04. low")),
        "close_price": _number(quote.get("05. price")),
        "volume": _integer(quote.get("06. volume")),
        "previous_close": _number(quote.get("08. previous close")),
        "change_amount": _number(quote.get("09. change")),
        "change_percent": _number(quote.get("10. change percent")),
    }

    # symbol and trade_date are the primary key, so a row without them cannot
    # be stored.
    if not row["symbol"] or not row["trade_date"]:
        logger.warning("Skipping quote without symbol or trading day: %s", quote)
        return None

    missing = [name for name, value in row.items() if value is None]
    if missing:
        logger.warning(
            "Quote for %s is missing field(s): %s", row["symbol"], ", ".join(missing)
        )

    return row


def extract_quotes(payloads):
    """Extract rows from every fetched payload, skipping unusable responses."""
    rows = []
    for symbol, payload in payloads.items():
        row = extract_quote(payload)
        if row is None:
            logger.warning("No usable data extracted for %s", symbol)
            continue
        rows.append(row)
    logger.info("Extracted %d row(s)", len(rows))
    return rows


# --- Step 3: update the existing PostgreSQL table --------------------------
UPSERT_SQL = f"""
    INSERT INTO {TABLE_NAME} ({", ".join(COLUMNS)})
    VALUES %s
    ON CONFLICT (symbol, trade_date) DO UPDATE SET
        open_price     = EXCLUDED.open_price,
        high_price     = EXCLUDED.high_price,
        low_price      = EXCLUDED.low_price,
        close_price    = EXCLUDED.close_price,
        volume         = EXCLUDED.volume,
        previous_close = EXCLUDED.previous_close,
        change_amount  = EXCLUDED.change_amount,
        change_percent = EXCLUDED.change_percent,
        updated_at     = now()
"""


def upsert_quotes(rows):
    """Insert or update the given rows in the stock_prices table.

    Returns the number of rows written. Every row is written in one
    transaction, so a failure leaves the table unchanged.
    """
    if not rows:
        logger.warning("No rows to write, skipping database update")
        return 0

    values = [tuple(row.get(column) for column in COLUMNS) for row in rows]

    connection = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        with connection, connection.cursor() as cursor:
            execute_values(cursor, UPSERT_SQL, values)
        logger.info("Wrote %d row(s) to %s", len(values), TABLE_NAME)
        return len(values)
    except psycopg2.Error as error:
        logger.error("Database update failed: %s", error)
        raise
    finally:
        if connection is not None:
            connection.close()


# --- Full pipeline ---------------------------------------------------------
def run():
    """Run fetch, extract and update in sequence."""
    symbols = symbol_list()
    logger.info("Starting pipeline for symbol(s): %s", ", ".join(symbols))

    payloads = fetch_quotes(symbols)
    rows = extract_quotes(payloads)
    if not rows:
        raise RuntimeError("No usable stock data was returned by the API")

    return upsert_quotes(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run()

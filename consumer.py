"""
consumer.py
-----------
Reads price messages from Kafka and writes them into PostgreSQL.

Think of this as the "sink" end of the pipeline.
Every message the producer sends ends up as a row in the crypto_prices table.
"""

import json
import psycopg2
from kafka import KafkaConsumer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC        = "crypto-prices"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "crypto_db",
    "user":     "crypto",
    "password": "crypto123",
}
# ─────────────────────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO crypto_prices (symbol, price_usd)
    VALUES (%s, %s)
"""


def main():
    print("Starting consumer — connecting to Kafka and Postgres...")

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("PostgreSQL connected.")

    # Connect to Kafka — auto_offset_reset='earliest' means we won't miss
    # messages that arrived while this script was starting up
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    print(f"Kafka connected — listening on topic '{TOPIC}'. Press Ctrl+C to stop.\n")

    for message in consumer:
        data = message.value   # our dict: {"symbol": "BITCOIN", "price_usd": 62000}

        cursor.execute(INSERT_SQL, (data["symbol"], data["price_usd"]))
        conn.commit()

        print(f"  ✓ saved  {data['symbol']:10s}  ${data['price_usd']:,.2f}")


if __name__ == "__main__":
    main()

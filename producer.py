"""
producer.py
-----------
Fetches live crypto prices from CoinGecko (free, no API key needed)
every 5 seconds and pushes each price as a message into a Kafka topic.

Think of this as the "source" end of the pipeline.
"""

import json
import time
import requests
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
TOPIC         = "crypto-prices"
COINS         = ["bitcoin", "ethereum", "solana"]   # add more if you like
INTERVAL_SECS = 30   # CoinGecko free tier allows ~30 req/min — 30s keeps us safe

# CoinGecko free endpoint — no sign-up required
COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids={coins}&vs_currencies=usd"
)
# ─────────────────────────────────────────────────────────────────────────────


def fetch_prices() -> dict:
    """Returns e.g. {'bitcoin': 62000.1, 'ethereum': 3100.5}"""
    url = COINGECKO_URL.format(coins=",".join(COINS))
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    return {coin: data[coin]["usd"] for coin in COINS}


def main():
    print("Starting producer — connecting to Kafka...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        # Serialize our Python dict → JSON string → bytes for Kafka
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Connected! Sending prices every {INTERVAL_SECS}s. Press Ctrl+C to stop.\n")

    while True:
        try:
            prices = fetch_prices()

            for coin, price in prices.items():
                message = {
                    "symbol": coin.upper(),  # "bitcoin" → "BITCOIN"
                    "price_usd": price,
                }
                producer.send(TOPIC, value=message)
                print(f"  → sent  {message['symbol']:10s}  ${price:,.2f}")

            producer.flush()   # make sure messages are actually sent
            print()

        except requests.RequestException as e:
            print(f"API error (will retry): {e}")

        time.sleep(INTERVAL_SECS)


if __name__ == "__main__":
    main()

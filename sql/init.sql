-- Creates the table that stores every price tick we receive
CREATE TABLE IF NOT EXISTS crypto_prices (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,   -- e.g. "BTC", "ETH"
    price_usd   NUMERIC(18, 6) NOT NULL,   -- price at the moment
    captured_at TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- An index so Grafana queries by time stay fast
CREATE INDEX IF NOT EXISTS idx_crypto_prices_time
    ON crypto_prices (captured_at DESC);

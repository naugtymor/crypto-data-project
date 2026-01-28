CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_coingecko_markets (
    load_date DATE NOT NULL,
    id TEXT NOT NULL,                  -- coin id (bitcoin)
    symbol TEXT,
    name TEXT,
    current_price NUMERIC,
    market_cap NUMERIC,
    market_cap_rank INT,
    fully_diluted_valuation NUMERIC,
    total_volume NUMERIC,
    high_24h NUMERIC,
    low_24h NUMERIC,
    price_change_24h NUMERIC,
    price_change_percentage_24h NUMERIC,
    market_cap_change_24h NUMERIC,
    market_cap_change_percentage_24h NUMERIC,
    circulating_supply NUMERIC,
    total_supply NUMERIC,
    max_supply NUMERIC,
    ath NUMERIC,
    ath_change_percentage NUMERIC,
    ath_date TIMESTAMP,
    atl NUMERIC,
    atl_change_percentage NUMERIC,
    atl_date TIMESTAMP,
    roi JSONB,                          -- ⚠️ оставляем как есть
    last_updated TIMESTAMP,
    price_change_percentage_1h NUMERIC,
    price_change_percentage_7d NUMERIC,
    inserted_at TIMESTAMP DEFAULT now(),
    CONSTRAINT pk_stg_coingecko_markets
        PRIMARY KEY (load_date, id)
);

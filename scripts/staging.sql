CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;


drop table staging.stg_coingecko_markets 
CREATE TABLE IF NOT EXISTS staging.stg_coingecko_markets (
    load_date DATE NOT NULL,
    id TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    image TEXT,
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
    roi INT,
    last_updated TIMESTAMP,
    price_change_percentage_1h_in_currency NUMERIC,
    price_change_percentage_24h_in_currency NUMERIC,
    price_change_percentage_7d_in_currency NUMERIC,
    roi_times NUMERIC,
    roi_currency TEXT,
    roi_percentage NUMERIC,
    inserted_at TIMESTAMP DEFAULT now(),
    CONSTRAINT pk_stg_coingecko_markets
        PRIMARY KEY (load_date, id)
);

select * from staging.stg_coingecko_markets scm where id='bitcoin'
with source as (

    select
        -- snapshot grain
        load_date,

        -- coin identity
        id as coin_id,
        symbol,
        name,
        image,

        -- pricing & market
        current_price as price,
        market_cap,
        market_cap_rank,

        total_volume as volume,

        -- intraday range
        high_24h,
        low_24h,

        -- price deltas
        price_change_24h,
        price_change_percentage_24h,

        -- market cap deltas
        market_cap_change_24h,
        market_cap_change_percentage_24h,

        -- supply metrics
        circulating_supply,
        total_supply,
        max_supply,

        -- all-time metadata
        ath,
        ath_change_percentage,
        ath_date,

        atl,
        atl_change_percentage,
        atl_date,

        -- technical metadata
        last_updated,
        inserted_at as ingested_at

    from {{ source('coingecko', 'stg_coingecko_markets') }}

)

select *
from source

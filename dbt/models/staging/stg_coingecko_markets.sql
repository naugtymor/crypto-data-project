with source as (
    select
        load_date,
        id as coin_id,
        symbol,
        name,
        image,
        current_price as price,
        market_cap,
        market_cap_rank,
        total_volume as volume,
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        market_cap_change_24h,
        market_cap_change_percentage_24h,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        ath_change_percentage,
        ath_date,
        atl,
        atl_change_percentage,
        atl_date,
        last_updated,
        inserted_at as ingested_at
    from {{ source('coingecko', 'stg_coingecko_markets') }}
)
select * from source

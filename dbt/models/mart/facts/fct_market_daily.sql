{{ config(
    materialized='incremental',
    unique_key=['load_date', 'coin_id']
) }}

with base as (

    select
        load_date,
        coin_id,
        price,
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        market_cap,
        market_cap_rank,
        market_cap_change_24h,
        market_cap_change_percentage_24h,
        volume,
        circulating_supply,
        total_supply,
        max_supply,
        last_updated,
        ingested_at
    from {{ ref('stg_coingecko_markets') }}

    {% if is_incremental() %}
        where load_date > (
            select max(load_date) from {{ this }}
        )
    {% endif %}

)

select * from base

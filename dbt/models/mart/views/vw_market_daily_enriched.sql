{{ config(
    materialized='view'
) }}

with fact as (
    select * from {{ ref('fct_market_daily') }}
),
dim as (
    select * from {{ ref('dim_coin') }}
)
select
    f.load_date,
    f.coin_id,
    d.symbol,
    d.name,
    f.price,
    f.high_24h,
    f.low_24h,
    f.price_change_24h,
    f.price_change_percentage_24h,
    f.market_cap,
    f.market_cap_rank,
    f.market_cap_change_24h,
    f.market_cap_change_percentage_24h,
    f.volume,
    f.circulating_supply,
    f.total_supply,
    f.max_supply
from fact f
left join dim d on f.coin_id = d.coin_id

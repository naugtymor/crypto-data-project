with ranked as (

    select
        coin_id,
        symbol,
        name,
        ath,
        ath_date,
        atl,
        atl_date,
        load_date,

        row_number() over (
            partition by coin_id
            order by load_date desc
        ) as rn

    from {{ ref('stg_coingecko_markets') }}

)

select
    coin_id,
    symbol,
    name,
    ath,
    ath_date,
    atl,
    atl_date
from ranked
where rn = 1

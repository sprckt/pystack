{{ config(materialized='table') }}

with raw_starships as (

    select * from {{source('pystack', 'starships')}}

)

select
    uid,
    name,
    url,
    _dlt_id as dlt_id
from raw_starships
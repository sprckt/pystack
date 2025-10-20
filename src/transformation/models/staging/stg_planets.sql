{{ config(materialized='table') }}

with raw_planets as (

    select * from {{source('pystack', 'planets')}}

)

select
    uid,
    name,
    url,
    _dlt_id as dlt_id
from raw_planets
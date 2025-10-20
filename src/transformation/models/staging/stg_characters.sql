{{ config(materialized='table') }}

with raw_cast as (

    select * from {{source('pystack', 'people')}}

)

select
    uid,
    name,
    url,
    _dlt_id as dlt_id
from raw_cast
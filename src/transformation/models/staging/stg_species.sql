{{ config(materialized='table') }}

with raw_species as (

    select * from {{source('pystack', 'species')}}

)

select
    uid,
    name,
    url,
    _dlt_id as dlt_id
from raw_species
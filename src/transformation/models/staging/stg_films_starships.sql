{{ config(materialized='table') }}

with raw_starships as (

    select * from {{source('pystack', 'films__properties__starships')}}

)

select
    value as url,
    _dlt_parent_id as parent_id,
    _dlt_id as dlt_id
from 
    raw_starships
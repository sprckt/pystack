{{ config(materialized='table') }}

with raw_species as (

    select * from {{source('pystack', 'films__properties__species')}}

)

select
    value as url,
    _dlt_parent_id as parent_id,
    _dlt_id as dlt_id
from 
    raw_species
{{ config(materialized='table') }}

with raw_film_metadata as (

    select * from {{source('pystack', 'star_wars_film_metadata')}}

)

select
    film,
    run_time,
    budget,
    box_office,
    tomato_rating as tomato_meter,
    popcorn_rating as popcorn_meter,
from 
    raw_film_metadata
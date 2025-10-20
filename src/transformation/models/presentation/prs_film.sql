{{ config(materialized='table') }}

with dmn_films as (

    select * from {{ref('dmn_film_finances')}}

)

select
    episode_number,
    title,
    release_date,
    run_time,
    budget,
    box_office,
    tomato_meter,
    popcorn_meter,
from dmn_films

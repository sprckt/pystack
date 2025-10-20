{{ config(materialized='table') }}

with films as (

    select * from {{ref('stg_films')}}

),

films_planets as (

    select * from {{ref('stg_films_planets')}}

),
planets as (

    select * from {{ref('stg_planets')}}

)

select
    films.episode_number,
    films.title,
    films_planets.url,
    planets.name as planets_name
from films
join films_planets
    on films.dlt_id = films_planets.parent_id
join planets
    on films_planets.url = planets.url

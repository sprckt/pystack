{{ config(materialized='table') }}

with films as (

    select * from {{ref('stg_films')}}

),

films_species as (

    select * from {{ref('stg_films_species')}}

),
species as (

    select * from {{ref('stg_species')}}

)

select
    films.episode_number,
    films.title,
    films_species.url,
    species.name as species_name
from films
join films_species
    on films.dlt_id = films_species.parent_id
join species
    on films_species.url = species.url

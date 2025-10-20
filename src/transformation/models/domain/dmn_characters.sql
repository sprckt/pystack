{{ config(materialized='table') }}

with films as (

    select * from {{ref('stg_films')}}

),

films_characters as (

    select * from {{ref('stg_films_characters')}}

),

characters as (

    select * from {{ref('stg_characters')}}

)

select
    films.episode_number,
    films.title,
    films_characters.url,
    characters.name as characters_name
from films
join films_characters
    on films.dlt_id = films_characters.parent_id
join characters
    on films_characters.url = characters.url

{{ config(materialized='table') }}

with films as (

    select * from {{ref('stg_films')}}

),

films_starships as (

    select * from {{ref('stg_films_starships')}}

),
starships as (

    select * from {{ref('stg_starships')}}

)

select
    films.episode_number,
    films.title,
    films_starships.url,
    starships.name as starships_name
from films
join films_starships
    on films.dlt_id = films_starships.parent_id
join starships
    on films_starships.url = starships.url

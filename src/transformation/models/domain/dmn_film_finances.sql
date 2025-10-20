{{ config(materialized='table') }}

with stg_films as (

    select * from {{ref('stg_films')}}

),

stg_film_metadata as (

    select * from {{ref('stg_film_metadata')}}

),

sequel_films as (
    select 
        7 as episode_number, 
        'The Force Awakens' as title,
        'J.J. Abrams' as director,
        cast('2015-12-18' as date) as release_date,
        'A Star Wars Film' as description, 
    union all
        select
            8 as episode_number, 
            'The Last Jedi' as title,
            'Rian Johnson' as director,
            cast('2017-12-15' as date) as release_date,
            'A Star Wars Film' as description, 
    union all
        select
            9 as episode_number, 
            'The Rise of Skywalker' as title,
            'J.J. Abrams' as director,
            cast('2020-12-20' as date) as release_date,
            'A Star Wars Film' as description, 
),

all_films as(
    select 
        episode_number,
        title,
        director,
        release_date,
        description,    
    from stg_films
    union all
    select * from sequel_films
)

select
    all_films.episode_number,
    all_films.title,
    all_films.director,
    all_films.release_date,
    all_films.description,
    stg_film_metadata.run_time,
    stg_film_metadata.budget,
    stg_film_metadata.box_office,
    stg_film_metadata.tomato_meter,
    stg_film_metadata.popcorn_meter
from all_films
join stg_film_metadata
    on all_films.episode_number = stg_film_metadata.film


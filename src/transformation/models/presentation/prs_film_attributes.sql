{{ config(materialized='table') }}

with films as (
  select * from {{ref('dmn_film_finances')}}
  
),

species as (
  select * from {{ref('dmn_species')}}
  
),

characters as (
  select * from {{ref('dmn_characters')}}
  
),

planets as (
  select * from {{ref('dmn_planets')}}
  
),

starships as (
  select * from {{ref('dmn_starships')}}
  
),

aggregated_counts as (
  select
    films.episode_number,
    films.title,
    films.release_date,
    count(distinct case when species.species_name is not null then species.species_name end) as species_count,
    count(distinct case when characters.characters_name is not null then characters.characters_name end) as             character_count,
    count(distinct case when planets.planets_name is not null then planets.planets_name end) as planet_count,
    count(distinct case when starships.starships_name is not null then starships.starships_name end) as starship_count
  from
    films
    left join species
      on films.episode_number = species.episode_number
    left join characters
      on films.episode_number = characters.episode_number
    left join planets
      on films.episode_number = planets.episode_number
    left join starships
      on films.episode_number = starships.episode_number
  group by 
    films.episode_number, films.title, films.release_date
)


select 
  episode_number, 
  title, 
  release_date, 
  species_count,
  character_count,
  planet_count,
  starship_count
from aggregated_counts
order by release_date
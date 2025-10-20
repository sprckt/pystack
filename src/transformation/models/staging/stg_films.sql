{{ config(materialized='table') }}

with raw_films as (

    select * from {{source('pystack', 'films')}}

)

select
    uid,
    properties__producer as producer,
    properties__title as title,
    properties__episode_id as episode_number,
    properties__director as director,
    properties__release_date as release_date,
    properties__opening_crawl as opening_crawl,
    properties__url as url,
    _id,
    description,
    v,
    _dlt_load_id as dlt_load_id, 
    _dlt_id as dlt_id,
from 
    raw_films



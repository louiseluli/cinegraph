-- analytics/models/intermediate/int_person_filmography.sql
-- Purpose:
--   Unified filmography rows per person-title-role with rich fields for analytics:
--   - person (name, years, lifespan, professions)
--   - title (primary/original/english, start_year, decade, genres, runtime, adult flag)
--   - role (category, characters, is_cast/is_crew)
--   - ratings (avg + votes + buckets)
--   - crew (director nconsts)
--
-- Notes:
--   We ref() the staging models. Staging views expose mixedCase IMDb columns;
--   here we standardize to snake_case.

{{ config(materialized='view') }}

with
b as (
  select
    "tconst"                                    as tconst,
    "primaryTitle"                              as primary_title,
    "originalTitle"                             as original_title,
    "titleType"                                 as title_type,
    is_adult,
    start_year,
    end_year,
    runtime_minutes,
    genres,                                     -- text[]
    decade
  from {{ ref('stg_title_basics') }}
),

p as (
  select
    tconst,
    nconst,
    category,                                   -- lowercased in staging
    characters,                                 -- text[]
    ordering,
    is_cast,
    is_crew
  from {{ ref('stg_title_principals') }}
),

n as (
  select
    "nconst"                                    as nconst,
    primary_name,
    birth_year,
    death_year,
    lifespan_years,
    professions                                 -- text[]
  from {{ ref('stg_name_basics') }}
),

r as (
  select
    "tconst"                                    as tconst,
    average_rating,
    num_votes,
    rating_bucket,
    votes_bucket
  from {{ ref('stg_title_ratings') }}
),

-- Choose the "best" English title per tconst (prefer original if English, else lowest ordering)
a_en as (
  select distinct on ("tconst")
    "tconst"                                    as tconst,
    aka_title                                   as english_title
  from {{ ref('stg_title_akas') }}
  where language = 'en'
  order by "tconst", is_original_title desc, ordering asc
),

c as (
  select
    "tconst"                                    as tconst,
    directors,                                  -- text[] of nconsts
    director_count,
    writers,
    writer_count
  from {{ ref('stg_title_crew') }}
)

select
  -- person
  n.nconst,
  n.primary_name,
  n.birth_year,
  n.death_year,
  n.lifespan_years,
  n.professions,

  -- title
  b.tconst,
  b.primary_title,
  b.original_title,
  a_en.english_title,
  b.title_type,
  b.is_adult,
  b.start_year,
  b.end_year,
  b.runtime_minutes,
  b.genres,
  (case when array_length(b.genres, 1) >= 1 then b.genres[1] end) as primary_genre,
  b.decade,
  (case when b.decade is not null then (b.decade::text || 's') end) as decade_label,

  -- role
  p.category,
  p.is_cast,
  p.is_crew,
  p.characters,

  -- ratings
  r.average_rating,
  r.num_votes,
  r.rating_bucket,
  r.votes_bucket,

  -- crew (for director-based filters / “Willi Forst style”)
  c.directors,
  c.director_count,
  c.writers,
  c.writer_count
from p
join n   on n.nconst  = p.nconst
join b   on b.tconst  = p.tconst
left join r   on r.tconst  = b.tconst
left join a_en on a_en.tconst = b.tconst
left join c   on c.tconst  = b.tconst

-- analytics/models/intermediate/int_titles.sql
-- Purpose:
--   One row per title with rich fields for analytics and ML:
--   - canonical titles (primary/original/english)
--   - time features (start_year, decade)
--   - content features (genres[], primary_genre, runtime, is_adult)
--   - popularity/quality (average_rating, num_votes + buckets)
--   - creators (director_ids[], writer_ids[])
--   - cast (ordered cast_nconsts[], cast_count)
--   - convenient genre flags (for quick filters / simple ML baselines)
--
-- Notes:
--   - We keep arrays for easy joins and embeddings later.
--   - Genre flags are intentionally sparse; we can add more on demand.

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
    genres,
    (case when array_length(genres, 1) >= 1 then genres[1] end) as primary_genre,
    decade,
    (case when decade is not null then (decade::text || 's') end) as decade_label
  from {{ ref('stg_title_basics') }}
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

a_en as (
  -- pick "best" English title per tconst: prefer original-if-English, then lowest ordering
  select distinct on ("tconst")
    "tconst"                                    as tconst,
    aka_title                                   as english_title
  from {{ ref('stg_title_akas') }}
  where language = 'en'
  order by "tconst", is_original_title desc, ordering asc
),

crew as (
  select
    "tconst"                                    as tconst,
    directors,
    director_count,
    writers,
    writer_count
  from {{ ref('stg_title_crew') }}
),

cast_rows as (
  select
    p.tconst,
    p.nconst,
    p.ordering
  from {{ ref('stg_title_principals') }} p
  where p.is_cast
),

cast_agg as (
  select
    tconst,
    array_agg(nconst order by ordering) as cast_nconsts,
    count(*)                            as cast_count
  from cast_rows
  group by tconst
),

-- Simple genre flags for filters & baseline ML
genre_flags as (
  select
    tconst,
    -- These are common IMDb genres. Add more as needed.
    (array_position(b.genres, 'Action')       is not null) as g_action,
    (array_position(b.genres, 'Adventure')    is not null) as g_adventure,
    (array_position(b.genres, 'Animation')    is not null) as g_animation,
    (array_position(b.genres, 'Comedy')       is not null) as g_comedy,
    (array_position(b.genres, 'Crime')        is not null) as g_crime,
    (array_position(b.genres, 'Documentary')  is not null) as g_documentary,
    (array_position(b.genres, 'Drama')        is not null) as g_drama,
    (array_position(b.genres, 'Family')       is not null) as g_family,
    (array_position(b.genres, 'Fantasy')      is not null) as g_fantasy,
    (array_position(b.genres, 'History')      is not null) as g_history,
    (array_position(b.genres, 'Horror')       is not null) as g_horror,
    (array_position(b.genres, 'Music')        is not null) as g_music,
    (array_position(b.genres, 'Mystery')      is not null) as g_mystery,
    (array_position(b.genres, 'Romance')      is not null) as g_romance,
    (array_position(b.genres, 'Science Fiction') is not null) as g_scifi,
    (array_position(b.genres, 'TV Movie')     is not null) as g_tvmovie,
    (array_position(b.genres, 'Thriller')     is not null) as g_thriller,
    (array_position(b.genres, 'War')          is not null) as g_war,
    (array_position(b.genres, 'Western')      is not null) as g_western
  from b
)

select
  -- core identity
  b.tconst,
  b.title_type,

  -- titles
  b.primary_title,
  b.original_title,
  a_en.english_title,

  -- time/content
  b.is_adult,
  b.start_year,
  b.end_year,
  b.decade,
  b.decade_label,
  b.runtime_minutes,

  -- genres
  b.genres,
  b.primary_genre,

  -- ratings / popularity
  r.average_rating,
  r.num_votes,
  r.rating_bucket,
  r.votes_bucket,

  -- creators & cast
  crew.directors      as director_ids,
  crew.director_count,
  crew.writers        as writer_ids,
  crew.writer_count,
  cast_agg.cast_nconsts as cast_ids,
  cast_agg.cast_count,

  -- genre flags (good for quick filters & baseline ML)
  gf.g_action, gf.g_adventure, gf.g_animation, gf.g_comedy, gf.g_crime, gf.g_documentary,
  gf.g_drama, gf.g_family, gf.g_fantasy, gf.g_history, gf.g_horror, gf.g_music,
  gf.g_mystery, gf.g_romance, gf.g_scifi, gf.g_tvmovie, gf.g_thriller, gf.g_war, gf.g_western

from b
left join r        on r.tconst = b.tconst
left join a_en     on a_en.tconst = b.tconst
left join crew     on crew.tconst = b.tconst
left join cast_agg on cast_agg.tconst = b.tconst
left join genre_flags gf on gf.tconst = b.tconst

-- analytics/models/intermediate/int_titles_features.sql
-- Purpose:
--   Numeric/boolean *feature view* for ML/recs from analytics.int_titles.
--   Keeps only machine-friendly columns and produces basic normalizations.
--
-- Inputs (from int_titles):
--   - identity: tconst
--   - titles: primary_title, english_title (kept for debugging/inspection)
--   - numerics: start_year, decade, runtime_minutes, average_rating, num_votes,
--               director_count, writer_count, cast_count
--   - booleans: is_adult, genre flags (g_action...g_western)
--   - categories: primary_genre (we one-hot a small stable set)
--
-- Normalizations (per full table):
--   - rating_norm: (average_rating - min) / (max - min)
--   - votes_log   : ln(1 + num_votes)
--   - votes_norm  : (votes_log - min) / (max - min)
--   - runtime_norm: (runtime_minutes - min) / (max - min)
--   - year_norm   : (start_year - min) / (max - min)
--   - crew/cast norms via log1p with the same scheme
--
-- Notes:
--   - This is a VIEW so it always reflects latest data. If you need frozen training
--     snapshots, we’ll materialize a TABLE with persisted stats in a later step.

{{ config(materialized='view') }}

with base as (
  select
    tconst,
    primary_title,
    english_title,
    is_adult,
    start_year,
    decade,
    runtime_minutes,
    average_rating,
    num_votes,
    director_count,
    writer_count,
    cast_count,

    -- genre flags already computed in int_titles (booleans)
    g_action, g_adventure, g_animation, g_comedy, g_crime, g_documentary,
    g_drama, g_family, g_fantasy, g_history, g_horror, g_music,
    g_mystery, g_romance, g_scifi, g_tvmovie, g_thriller, g_war, g_western,

    primary_genre
  from {{ ref('int_titles') }}
),

-- Small one-hot for primary_genre (stable set; extend as needed)
pg as (
  select
    tconst,
    (primary_genre = 'Action')::boolean          as pg_action,
    (primary_genre = 'Adventure')::boolean       as pg_adventure,
    (primary_genre = 'Animation')::boolean       as pg_animation,
    (primary_genre = 'Comedy')::boolean          as pg_comedy,
    (primary_genre = 'Crime')::boolean           as pg_crime,
    (primary_genre = 'Documentary')::boolean     as pg_documentary,
    (primary_genre = 'Drama')::boolean           as pg_drama,
    (primary_genre = 'Family')::boolean          as pg_family,
    (primary_genre = 'Fantasy')::boolean         as pg_fantasy,
    (primary_genre = 'History')::boolean         as pg_history,
    (primary_genre = 'Horror')::boolean          as pg_horror,
    (primary_genre = 'Music')::boolean           as pg_music,
    (primary_genre = 'Mystery')::boolean         as pg_mystery,
    (primary_genre = 'Romance')::boolean         as pg_romance,
    (primary_genre = 'Science Fiction')::boolean as pg_scifi,
    (primary_genre = 'TV Movie')::boolean        as pg_tvmovie,
    (primary_genre = 'Thriller')::boolean        as pg_thriller,
    (primary_genre = 'War')::boolean             as pg_war,
    (primary_genre = 'Western')::boolean         as pg_western
  from base
),

-- Precompute table-level stats for min-max scaling (NULL-safe)
stats as (
  select
    min(average_rating)           as min_rating,
    max(average_rating)           as max_rating,
    min(runtime_minutes)          as min_runtime,
    max(runtime_minutes)          as max_runtime,
    min(start_year)               as min_year,
    max(start_year)               as max_year,
    min(ln(1 + coalesce(num_votes,0)))  as min_votes_log,
    max(ln(1 + coalesce(num_votes,0)))  as max_votes_log,
    min(ln(1 + coalesce(director_count,0))) as min_dir_log,
    max(ln(1 + coalesce(director_count,0))) as max_dir_log,
    min(ln(1 + coalesce(writer_count,0)))   as min_wri_log,
    max(ln(1 + coalesce(writer_count,0)))   as max_wri_log,
    min(ln(1 + coalesce(cast_count,0)))     as min_cast_log,
    max(ln(1 + coalesce(cast_count,0)))     as max_cast_log
  from base
),

-- Build features
features as (
  select
    b.tconst,

    -- Keep human-readable labels for inspection (not used by models directly)
    b.primary_title,
    b.english_title,

    -- Booleans
    coalesce(b.is_adult, false) as f_is_adult,

    -- Raw numerics (nullable)
    b.start_year,
    b.decade,
    b.runtime_minutes,
    b.average_rating,
    b.num_votes,
    b.director_count,
    b.writer_count,
    b.cast_count,

    -- Log transforms (0 for nulls via coalesce)
    ln(1 + coalesce(b.num_votes,0))      as f_votes_log,
    ln(1 + coalesce(b.director_count,0)) as f_dir_log,
    ln(1 + coalesce(b.writer_count,0))   as f_wri_log,
    ln(1 + coalesce(b.cast_count,0))     as f_cast_log,

    -- Min-max normalizations (guard denominators)
    case when s.max_rating  > s.min_rating
         then (b.average_rating - s.min_rating) / nullif(s.max_rating - s.min_rating, 0)
         else null end as f_rating_norm,

    case when s.max_runtime > s.min_runtime and b.runtime_minutes is not null
         then (b.runtime_minutes - s.min_runtime) / nullif(s.max_runtime - s.min_runtime, 0)
         else null end as f_runtime_norm,

    case when s.max_year > s.min_year and b.start_year is not null
         then (b.start_year - s.min_year) / nullif(s.max_year - s.min_year, 0)
         else null end as f_year_norm,

    case when s.max_votes_log > s.min_votes_log
         then (ln(1 + coalesce(b.num_votes,0)) - s.min_votes_log) / nullif(s.max_votes_log - s.min_votes_log, 0)
         else null end as f_votes_norm,

    case when s.max_dir_log > s.min_dir_log
         then (ln(1 + coalesce(b.director_count,0)) - s.min_dir_log) / nullif(s.max_dir_log - s.min_dir_log, 0)
         else null end as f_dir_norm,

    case when s.max_wri_log > s.min_wri_log
         then (ln(1 + coalesce(b.writer_count,0)) - s.min_wri_log) / nullif(s.max_wri_log - s.min_wri_log, 0)
         else null end as f_wri_norm,

    case when s.max_cast_log > s.min_cast_log
         then (ln(1 + coalesce(b.cast_count,0)) - s.min_cast_log) / nullif(s.max_cast_log - s.min_cast_log, 0)
         else null end as f_cast_norm,

    -- Genre booleans (from int_titles)
    b.g_action, b.g_adventure, b.g_animation, b.g_comedy, b.g_crime, b.g_documentary,
    b.g_drama, b.g_family, b.g_fantasy, b.g_history, b.g_horror, b.g_music,
    b.g_mystery, b.g_romance, b.g_scifi, b.g_tvmovie, b.g_thriller, b.g_war, b.g_western

  from base b
  cross join stats s
)

select
  f.*,
  -- Primary-genre one-hot for a compact categorical signal
  pg.pg_action, pg.pg_adventure, pg.pg_animation, pg.pg_comedy, pg.pg_crime, pg.pg_documentary,
  pg.pg_drama, pg.pg_family, pg.pg_fantasy, pg.pg_history, pg.pg_horror, pg.pg_music,
  pg.pg_mystery, pg.pg_romance, pg.pg_scifi, pg.pg_tvmovie, pg.pg_thriller, pg.pg_war, pg.pg_western
from features f
join pg on pg.tconst = f.tconst

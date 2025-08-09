-- analytics/models/staging/stg_title_basics.sql
-- Purpose: Clean IMDb title.basics staging into typed fields + decade
-- Materialization: view (inherits from dbt_project.yml for 'staging')

with raw as (
    select
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        case
            when "isAdult" ~ '^[01]$' then ("isAdult"::int)::boolean
            else null
        end as is_adult,
        case when "startYear" ~ '^-?\d+$' then "startYear"::int end as start_year,
        case when "endYear"   ~ '^-?\d+$' then "endYear"::int   end as end_year,
        case when "runtimeMinutes" ~ '^-?\d+$' then "runtimeMinutes"::int end as runtime_minutes,
        nullif("genres", '') as genres_raw,
        case
            when "genres" is null or "genres" = '' then array[]::text[]
            else string_to_array("genres", ',')
        end as genres
    from "stg_title_basics"
),
with_decade as (
    select
        *,
        case when start_year is not null then (start_year / 10) * 10 end as decade
    from raw
)
select * from with_decade;

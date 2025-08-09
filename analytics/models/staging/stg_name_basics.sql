-- analytics/models/staging/stg_name_basics.sql
-- Purpose: Clean IMDb name.basics into typed fields + lifespan
-- Notes:
--   - Raw staging has mixedCase columns; we reference them quoted.
--   - We'll keep professions as a text[] for later filtering.

with raw as (
    select
        "nconst",
        nullif("primaryName", '')       as primary_name,
        case when "birthYear" ~ '^-?\d+$' then "birthYear"::int end as birth_year,
        case when "deathYear" ~ '^-?\d+$' then "deathYear"::int end as death_year,
        -- professions: "actor,producer" or empty
        nullif("primaryProfession", '') as professions_raw,
        case
            when "primaryProfession" is null or "primaryProfession" = '' then array[]::text[]
            else string_to_array("primaryProfession", ',')
        end as professions,
        nullif("knownForTitles", '')    as known_for_titles_raw,
        case
            when "knownForTitles" is null or "knownForTitles" = '' then array[]::text[]
            else string_to_array("knownForTitles", ',')
        end as known_for_titles
    from "stg_name_basics"
),
with_lifespan as (
    select
        *,
        case
            when birth_year is not null and death_year is not null then (death_year - birth_year)
            when birth_year is not null and death_year is null then null -- alive or unknown
        end as lifespan_years
    from raw
)

select * from with_lifespan

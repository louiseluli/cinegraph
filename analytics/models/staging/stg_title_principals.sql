-- analytics/models/staging/stg_title_principals.sql
-- Purpose: Clean IMDb title.principals staging into typed fields and normalized categories
-- Notes:
--   - Raw source table has mixedCase columns; we reference them quoted.
--   - "characters" values look like ["Foo","Bar"]. We keep the raw field and also a parsed array.

with raw as (
    select
        "tconst",
        case when "ordering" ~ '^-?\d+$' then "ordering"::int end as ordering,
        "nconst",
        lower(nullif("category", '')) as category,         -- normalize to lowercase
        nullif("job", '')        as job_raw,
        nullif("characters", '') as characters_raw
    from "stg_title_principals"
),

parsed as (
    select
        r.*,
        -- strip leading/trailing brackets, then turn "," into |, then strip quotes, then split to array
        case
            when characters_raw is null then array[]::text[]
            else string_to_array(replace(replace(regexp_replace(characters_raw, '^\[|\]$', '', 'g'), '","', '|'), '"', ''), '|')
        end as characters
    from raw r
),

labeled as (
    select
        *,
        -- quick role flags (handy for actor-centric queries)
        case when category in ('actor', 'actress') then true else false end as is_cast,
        case when category not in ('actor', 'actress') and category is not null then true else false end as is_crew
    from parsed
)

select * from labeled

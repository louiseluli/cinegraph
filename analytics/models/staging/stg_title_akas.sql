-- analytics/models/staging/stg_title_akas.sql
-- Purpose: Clean IMDb title.akas into typed fields + arrays
-- Raw columns (mixedCase from TSV): "titleId","ordering","title","region","language","types","attributes","isOriginalTitle"
-- Notes:
--   - We cast ordering to int
--   - We normalize region/language to lowercase text (or null)
--   - We split "types" and "attributes" into text[] arrays
--   - We convert isOriginalTitle from '0'/'1' to boolean

with raw as (
    select
        "titleId" as tconst,
        case when "ordering" ~ '^-?\d+$' then "ordering"::int end as ordering,
        nullif("title", '') as aka_title,
        lower(nullif("region", ''))   as region,    -- e.g., 'us', 'gb', 'at'
        lower(nullif("language", '')) as language,  -- e.g., 'en', 'de'
        nullif("types", '')      as types_raw,      -- e.g., 'imdbDisplay,working'
        nullif("attributes", '') as attributes_raw, -- e.g., 'literal translation'
        case
            when "isOriginalTitle" ~ '^[01]$' then ("isOriginalTitle"::int)::boolean
            else null
        end as is_original_title
    from "stg_title_akas"
),

arrays as (
    select
        *,
        case
            when types_raw is null or types_raw = '' then array[]::text[]
            else string_to_array(types_raw, ',')
        end as types,
        case
            when attributes_raw is null or attributes_raw = '' then array[]::text[]
            else string_to_array(attributes_raw, ',')
        end as attributes
    from raw
)

select
    tconst,
    ordering,
    aka_title,
    region,
    language,
    types_raw,
    attributes_raw,
    types,
    attributes,
    is_original_title
from arrays

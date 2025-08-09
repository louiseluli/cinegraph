-- analytics/models/staging/stg_title_crew.sql
-- Purpose: Clean IMDb title.crew into typed arrays of nconsts for directors and writers.
-- Notes:
--   - Raw columns (mixedCase): "tconst","directors","writers"
--   - Values are comma-separated nconsts (e.g. 'nm0000001,nm0000002' or '\N')
--   - We normalize empty -> NULL and split to text[] arrays.

with raw as (
    select
        "tconst",
        nullif("directors", '') as directors_raw,
        nullif("writers",   '') as writers_raw
    from "stg_title_crew"
),

arrays as (
    select
        "tconst",
        case
            when directors_raw is null or directors_raw = '\N' then array[]::text[]
            else string_to_array(directors_raw, ',')
        end as directors,
        case
            when writers_raw is null or writers_raw = '\N' then array[]::text[]
            else string_to_array(writers_raw, ',')
        end as writers
    from raw
),

with_counts as (
    select
        tconst,
        directors,
        writers,
        cardinality(directors) as director_count,
        cardinality(writers)   as writer_count
    from arrays
)

select * from with_counts

-- analytics/models/staging/stg_title_ratings.sql
-- Purpose: Clean IMDb title.ratings with typed columns + simple quality buckets
-- Notes:
--   - Raw staging table has mixedCase column names; we reference them quoted.
--   - Keep average_rating as numeric(3,1) for nice display; num_votes as integer.

with raw as (
    select
        "tconst",
        case when "averageRating" ~ '^[0-9]+(\.[0-9])?$'
             then ("averageRating")::numeric(3,1)
        end as average_rating,
        case when "numVotes" ~ '^[0-9]+$'
             then "numVotes"::int
        end as num_votes
    from "stg_title_ratings"
),

with_buckets as (
    select
        *,
        case
            when average_rating is null then null
            when average_rating >= 8.5 then 'masterpiece'
            when average_rating >= 7.5 then 'great'
            when average_rating >= 6.5 then 'good'
            when average_rating >= 5.5 then 'okay'
            else 'low'
        end as rating_bucket,
        case
            when num_votes is null then null
            when num_votes >= 500000 then 'massive'
            when num_votes >= 100000 then 'very_high'
            when num_votes >= 20000  then 'high'
            when num_votes >= 5000   then 'medium'
            else 'low'
        end as votes_bucket
    from raw
)

select * from with_buckets

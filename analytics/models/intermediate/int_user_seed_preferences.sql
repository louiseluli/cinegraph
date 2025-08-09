-- analytics/models/intermediate/int_user_seed_preferences.sql
-- Purpose:
--   App-facing table to store a user's seed preferences (likes / boosts) by title.
--   Columns:
--     - user_id  : text
--     - tconst   : text (IMDb title id)
--     - weight   : double precision (default 1.0)
--     - source   : text  (e.g., 'thumbs_up','watched','import','explicit')
--     - created_at : timestamp (UTC)
--
-- Notes:
--   - Materialized as a TABLE so the backend can INSERT rows.
--   - Table starts EMPTY; you populate via API / manual inserts.
--   - Jinja-safe comments only inside config block.

{# dbt model configuration #}
{{ config(
    materialized='table',
    constraints={
      "primary_key": ["user_id", "tconst"]
    },
    indexes=[
      {"columns": ["user_id"]},
      {"columns": ["tconst"]}
    ]
) }}

-- Create an EMPTY table with the correct schema. The WHERE false keeps it empty.
select
  cast(null as text)              as user_id,
  cast(null as text)              as tconst,
  cast(1.0  as double precision)  as weight,
  cast(null as text)              as source,
  (now() at time zone 'utc')      as created_at
where false

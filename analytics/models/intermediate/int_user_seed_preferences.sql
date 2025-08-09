-- analytics/models/intermediate/int_user_seed_preferences.sql
-- Purpose:
--   App-facing table to store a user's seed preferences (likes / boosts) by title.
--   This is intentionally minimal and stable so the backend can INSERT into it.
--
--   Columns:
--     - user_id  : text (use UUID string or any stable user handle)
--     - tconst   : text (IMDb title id)
--     - weight   : double precision (default 1.0, suggested range [0.1, 5.0])
--     - source   : text  (e.g., 'thumbs_up','watched','import','explicit')
--     - created_at : timestamp (server time when inserted)
--
-- Notes:
--   - Materialized as a TABLE so we can persist inserts.
--   - We create it EMPTY here; your API will insert rows.
--   - We add PK (user_id, tconst) + indexes via post-hooks (safe IF EXISTS wrapper).
--
--   Example insert (after dbt run):
--     insert into analytics.int_user_seed_preferences (user_id, tconst, weight, source)
--     values ('user:louise', 'tt0034583', 2.0, 'explicit'); -- Casablanca
--
--   Example join to features:
--     select s.user_id, s.tconst, f.*
--     from analytics.int_user_seed_preferences s
--     join analytics.int_titles_features f using (tconst);

{{ config(
    materialized='table',
    post_hook=[
      -- primary key (user_id, tconst) with a safety wrapper
      "do $$ begin
          if not exists (
            select 1 from pg_constraint
            where conname = 'int_user_seed_preferences_pk'
              and conrelid = '{{ this }}'::regclass
          ) then
            alter table {{ this }} add constraint int_user_seed_preferences_pk primary key (user_id, tconst);
          end if;
        end $$;",
      -- index on user_id
      "do $$ begin
          if not exists (
            select 1 from pg_indexes where schemaname=current_schema() and indexname='ix_user_seed_user'
          ) then
            create index ix_user_seed_user on {{ this }} (user_id);
          end if;
        end $$;",
      -- index on tconst
      "do $$ begin
          if not exists (
            select 1 from pg_indexes where schemaname=current_schema() and indexname='ix_user_seed_tconst'
          ) then
            create index ix_user_seed_tconst on {{ this }} (tconst);
          end if;
        end $$;"
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

# CineGraph ETL — IMDb Ingestion (v0.1)

This doc explains exactly how we’ll **download, validate, and load** IMDb’s Non-Commercial Datasets into Postgres for CineGraph. It’s designed for reproducibility and safety (respecting IMDb’s terms), and to be expanded in small, reviewable steps.

> **Scope of this doc:** Planning + commands for the ETL.  
> **What exists right now:** Only this README.  
> **Next step:** `etl/requirements.txt` and `etl/imdb_fetch.py` (a polite downloader).  
> **We will NOT** fetch anything until those are added and reviewed.

---

## 1) Data source & license

- **IMDb Non-Commercial Datasets** (daily refresh): https://datasets.imdbws.com/
- **Use:** Personal/non-commercial only. You may hold local copies.
- **Files we’ll use:**
  - `name.basics.tsv.gz` — people (birth/death years, professions)
  - `title.basics.tsv.gz` — titles (type, year, runtime, genres)
  - `title.akas.tsv.gz` — alternative titles (original & localized)
  - `title.crew.tsv.gz` — directors/writers
  - `title.principals.tsv.gz` — principal cast/crew
  - `title.ratings.tsv.gz` — IMDb ratings and votes
  - `title.episode.tsv.gz` — series episode linkage

We will preserve IMDb’s schema and **never** redistribute the raw TSVs. Aggregates and derived stats are OK inside CineGraph.

---

## 2) ETL overview (end state)

1. **Fetch**: Download `.tsv.gz` files with a polite user agent, retries, and checksums.
2. **Stage**: Load into **Postgres** staging tables (1:1 with IMDb columns). Convert `\N` → `NULL`. Use `TEXT` for raw fields; we’ll cast in dbt.
3. **Model**: Use **dbt** to create clean views/tables (typed columns, derived fields like **decade**, normalized genres, person/title crosswalks).
4. **Serve**: Our API queries the modeled schema for stats, filmographies, AKAs, etc.
5. **Schedule**: Prefect (later) to refresh daily or on-demand.

---

## 3) Target tables (staging)

We’ll create one staging table per IMDb file (names TBC during implementation):

- `stg_name_basics`
- `stg_title_basics`
- `stg_title_akas`
- `stg_title_crew`
- `stg_title_principals`
- `stg_title_ratings`
- `stg_title_episode`

> Indexes will focus on `nconst`, `tconst`, `startYear`, and `primaryName/primaryTitle` trigram search (for actor/title lookup speed).

---

## 4) Safety & “polite” rules

- Use a **descriptive User-Agent**, e.g. `CineGraph/0.1 (your-email@example.com)`.
- **Backoff & retries** on network hiccups.
- **No parallel hammering** — we’ll download files sequentially.
- **Checksum** verification (if provided; otherwise size/last-modified checks).
- Store files under `./data/imdb/` (git-ignored).

---

## 5) Local paths (planned)

When implemented, downloads will live here:
cinegraph/
data/
imdb/
name.basics.tsv.gz
title.basics.tsv.gz
title.akas.tsv.gz
title.crew.tsv.gz
title.episode.tsv.gz
title.principals.tsv.gz
title.ratings.tsv.gz
`.gitignore` will ensure `data/` is **not** committed.

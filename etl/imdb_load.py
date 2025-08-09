#!/usr/bin/env python3
"""
IMDb -> Postgres staging loader (v0.1)

What this script does:
- Reads local IMDb *.tsv.gz files from --from (default: ./data/imdb)
- Loads INTO Postgres staging tables (one per IMDb file), columns as TEXT
- Converts '\N' -> NULL
- Uses a replace-on-load strategy: load into <table>_new then swap
- Adds minimal indexes on tconst/nconst for speed

Usage:
    export DATABASE_URL="postgresql://cinegraph_user:changeme_strong_password@localhost:5432/cinegraph"
    python etl/imdb_load.py --dsn "$DATABASE_URL" --from ./data/imdb --tables title.basics,title.akas

Notes:
- We keep staging columns TEXT; dbt will cast/normalize later.
- Script is intentionally strict: it fails fast if a required file is missing.
"""

from __future__ import annotations

import argparse
import gzip
import io
import os
from pathlib import Path
from typing import Dict, List, Tuple

import psycopg
from psycopg import sql

# --------- IMDb file -> staging table schema (TEXT columns) ----------

# Column lists are straight from IMDb docs. All TEXT in staging; dbt will cast later.
SCHEMAS: Dict[str, Tuple[str, List[Tuple[str, str]]]] = {
    # table_name, columns[(name, type)]
    "stg_name_basics": (
        "name.basics.tsv.gz",
        [
            ("nconst", "TEXT"),
            ("primaryName", "TEXT"),
            ("birthYear", "TEXT"),
            ("deathYear", "TEXT"),
            ("primaryProfession", "TEXT"),
            ("knownForTitles", "TEXT"),
        ],
    ),
    "stg_title_basics": (
        "title.basics.tsv.gz",
        [
            ("tconst", "TEXT"),
            ("titleType", "TEXT"),
            ("primaryTitle", "TEXT"),
            ("originalTitle", "TEXT"),
            ("isAdult", "TEXT"),
            ("startYear", "TEXT"),
            ("endYear", "TEXT"),
            ("runtimeMinutes", "TEXT"),
            ("genres", "TEXT"),
        ],
    ),
    "stg_title_akas": (
        "title.akas.tsv.gz",
        [
            ("titleId", "TEXT"),
            ("ordering", "TEXT"),
            ("title", "TEXT"),
            ("region", "TEXT"),
            ("language", "TEXT"),
            ("types", "TEXT"),
            ("attributes", "TEXT"),
            ("isOriginalTitle", "TEXT"),
        ],
    ),
    "stg_title_crew": (
        "title.crew.tsv.gz",
        [
            ("tconst", "TEXT"),
            ("directors", "TEXT"),
            ("writers", "TEXT"),
        ],
    ),
    "stg_title_episode": (
        "title.episode.tsv.gz",
        [
            ("tconst", "TEXT"),
            ("parentTconst", "TEXT"),
            ("seasonNumber", "TEXT"),
            ("episodeNumber", "TEXT"),
        ],
    ),
    "stg_title_principals": (
        "title.principals.tsv.gz",
        [
            ("tconst", "TEXT"),
            ("ordering", "TEXT"),
            ("nconst", "TEXT"),
            ("category", "TEXT"),
            ("job", "TEXT"),
            ("characters", "TEXT"),
        ],
    ),
    "stg_title_ratings": (
        "title.ratings.tsv.gz",
        [
            ("tconst", "TEXT"),
            ("averageRating", "TEXT"),
            ("numVotes", "TEXT"),
        ],
    ),
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load IMDb TSV.GZ files into Postgres staging tables")
    p.add_argument("--dsn", type=str, default=os.getenv("DATABASE_URL"), help="Postgres DSN (or set DATABASE_URL)")
    p.add_argument("--from", dest="src", type=str, default="data/imdb", help="Directory containing *.tsv.gz")
    p.add_argument(
        "--tables",
        type=str,
        default=",".join([k for k in SCHEMAS.keys()]),
        help=f"Comma-separated staging tables to load. Default: all ({','.join(SCHEMAS.keys())})",
    )
    p.add_argument("--drop-first", action="store_true", help="Drop target tables before load (rarely needed)")
    return p.parse_args()


def must_exist(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"File not found: {path}")


def create_table_sql(table: str, cols: List[Tuple[str, str]]) -> str:
    cols_sql = ", ".join([f"{name} {typ}" for name, typ in cols])
    return f"CREATE TABLE IF NOT EXISTS {table} ({cols_sql});"


def create_indexes_sql(table: str) -> List[str]:
    # minimal useful indexes
    idx = []
    if table in ("stg_title_basics", "stg_title_crew", "stg_title_episode", "stg_title_principals", "stg_title_ratings"):
        idx.append(f"CREATE INDEX IF NOT EXISTS {table}_tconst_idx ON {table}(tconst);")
    if table in ("stg_name_basics", "stg_title_principals"):
        idx.append(f"CREATE INDEX IF NOT EXISTS {table}_nconst_idx ON {table}(nconst);")
    if table == "stg_title_akas":
        idx.append(f"CREATE INDEX IF NOT EXISTS {table}_titleid_idx ON {table}(titleId);")
    return idx


def copy_file(conn: psycopg.Connection, table: str, file_path: Path, cols: List[Tuple[str, str]]) -> int:
    """
    COPY data from gzip TSV to table_new, converting \N -> NULL.
    Returns number of rows loaded.
    """
    target_new = f"{table}_new"
    col_names = [c[0] for c in cols]
    with conn.cursor() as cur:
        # Drop & create temp _new table
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(target_new)))
        cur.execute(sql.SQL(create_table_sql(target_new, cols)))

        # Prepare COPY
        col_list = sql.SQL(', ').join(sql.Identifier(c) for c in col_names)
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', HEADER TRUE, NULL '\N')").format(
            sql.Identifier(target_new),
            col_list,
        )

        # Stream gzip -> COPY
        with gzip.open(file_path, "rb") as gz:
            # psycopg 3 copy expects text or bytes; IMDb TSV are UTF-8
            # We can pass bytes directly using binary mode.
            with cur.copy(copy_sql) as cp:
                # Read in chunks to avoid loading the whole file in memory
                for chunk in iter(lambda: gz.read(1024 * 1024), b""):
                    cp.write(chunk)

        # Swap: drop old, rename new
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table)))
        cur.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(sql.Identifier(target_new), sql.Identifier(table)))

        # Indexes
        for stmt in create_indexes_sql(table):
            cur.execute(stmt)

        # Count rows
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        (count,) = cur.fetchone()
        return int(count)


def main() -> None:
    args = parse_args()
    if not args.dsn:
        raise SystemExit("No DSN provided. Set --dsn or export DATABASE_URL")

    src = Path(args.src).resolve()
    if not src.exists():
        raise SystemExit(f"Source directory not found: {src}")

    targets = [t.strip() for t in args.tables.split(",") if t.strip()]
    for t in targets:
        if t not in SCHEMAS:
            raise SystemExit(f"Unknown table: {t}. Valid: {', '.join(SCHEMAS)}")

    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET lock_timeout = '5s';")
        if args.drop_first:
            for table in targets:
                conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table)))
        for table in targets:
            imdb_file, cols = SCHEMAS[table]
            path = src / imdb_file
            must_exist(path)
            print(f"→ Loading {imdb_file} into {table} ...")
            count = copy_file(conn, table, path, cols)
            print(f"✔ Loaded {count:,} rows into {table}")

    print("All loads complete.")


if __name__ == "__main__":
    main()

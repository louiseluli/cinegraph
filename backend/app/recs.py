# backend/app/recs.py
# Content-based recommendations from analytics.int_titles_features.
# No new dependencies: psycopg + FastAPI + tiny pure-Python cosine.
#
# Design notes:
# - Feature columns are explicitly listed to keep a fixed order (important for cosine).
# - We build a weighted centroid from the user's seed titles (likes).
# - We filter candidate space (min_votes, since_year, optional genres, no adult)
#   to keep it fast until we switch to ANN.
# - Safe SQL: dynamic WHERE uses a whitelist of column names; values are parameterized.

from fastapi import APIRouter, HTTPException, Query
import os
import psycopg
from typing import List, Dict, Any, Tuple, Optional
import math

router = APIRouter(tags=["recs"])

# ------------- configuration -------------

# Read DSN from env. Inside Docker this should already be set to db:5432.
DB_DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://cinegraph_user:changeme_strong_password@localhost:5433/cinegraph",
)

# Order matters! Keep in sync if you add features in analytics.int_titles_features.
FEATURE_COLS: List[str] = [
    # normalized numerics
    "f_rating_norm",
    "f_votes_norm",
    "f_runtime_norm",
    "f_year_norm",
    # raw(ish) logs (add signal even if norms are null)
    "f_votes_log",
    "f_dir_log",
    "f_wri_log",
    "f_cast_log",
    # genre flags
    "g_action","g_adventure","g_animation","g_comedy","g_crime","g_documentary",
    "g_drama","g_family","g_fantasy","g_history","g_horror","g_music",
    "g_mystery","g_romance","g_scifi","g_tvmovie","g_thriller","g_war","g_western",
    # primary-genre one-hot
    "pg_action","pg_adventure","pg_animation","pg_comedy","pg_crime","pg_documentary",
    "pg_drama","pg_family","pg_fantasy","pg_history","pg_horror","pg_music",
    "pg_mystery","pg_romance","pg_scifi","pg_tvmovie","pg_thriller","pg_war","pg_western",
]

# Allowable genre names from query -> (flag column)
GENRE_FLAG_MAP: Dict[str, str] = {
    # main genres
    "Action": "g_action",
    "Adventure": "g_adventure",
    "Animation": "g_animation",
    "Comedy": "g_comedy",
    "Crime": "g_crime",
    "Documentary": "g_documentary",
    "Drama": "g_drama",
    "Family": "g_family",
    "Fantasy": "g_fantasy",
    "History": "g_history",
    "Horror": "g_horror",
    "Music": "g_music",
    "Mystery": "g_mystery",
    "Romance": "g_romance",
    "Science Fiction": "g_scifi",  # IMDb label
    "Sci-Fi": "g_scifi",           # alias
    "TV Movie": "g_tvmovie",
    "Thriller": "g_thriller",
    "War": "g_war",
    "Western": "g_western",
}

# ------------- tiny math helpers -------------

def _dot(a: List[float], b: List[float]) -> float:
    return sum(x*y for x, y in zip(a, b))

def _norm(a: List[float]) -> float:
    return math.sqrt(sum(x*x for x in a)) or 1e-9

def _cosine(a: List[float], b: List[float]) -> float:
    return _dot(a, b) / (_norm(a) * _norm(b))

def _row_to_vec(row: Dict[str, Any]) -> List[float]:
    # Preserve FEATURE_COLS order; coalesce None->0.0
    return [float(row.get(col) or 0.0) for col in FEATURE_COLS]

# ------------- SQL helpers -------------

SEED_SQL = f"""
select
  s.tconst,
  s.weight,
  f.primary_title,
  f.english_title,
  f.start_year,
  f.average_rating,
  f.num_votes,
  {", ".join(FEATURE_COLS)}
from analytics.int_user_seed_preferences s
join analytics.int_titles_features f using (tconst)
where s.user_id = %s
"""

# Base candidate SELECT; WHERE is appended safely based on whitelisted columns.
CANDIDATE_BASE_SQL = f"""
select
  f.tconst,
  f.primary_title,
  f.english_title,
  f.start_year,
  f.average_rating,
  f.num_votes,
  {", ".join(FEATURE_COLS)}
from analytics.int_titles_features f
"""

def _build_candidate_where(
    allow_adult: bool,
    min_votes: int,
    since_year: int,
    genre_any: Optional[List[str]],
    exclude_tconst: Optional[List[str]],
    seed_tconsts: List[str],
) -> Tuple[str, List[Any]]:
    where_clauses: List[str] = []
    params: List[Any] = []

    if not allow_adult:
        where_clauses.append("coalesce(f.f_is_adult, false) = false")

    if min_votes is not None and min_votes > 0:
        where_clauses.append("coalesce(f.num_votes, 0) >= %s")
        params.append(min_votes)

    if since_year is not None and since_year > 0:
        where_clauses.append("coalesce(f.start_year, 0) >= %s")
        params.append(since_year)

    # Genre OR: any of the specified genres
    if genre_any:
        mapped_cols = [GENRE_FLAG_MAP[g] for g in genre_any if g in GENRE_FLAG_MAP]
        if mapped_cols:
            ors = " OR ".join([f"f.{c} = true" for c in mapped_cols])
            where_clauses.append(f"({ors})")

    # Exclude user seeds & optionally explicit excludes
    all_excludes = set(seed_tconsts)
    if exclude_tconst:
        all_excludes.update(exclude_tconst)
    if all_excludes:
        where_clauses.append(f"f.tconst <> ALL(%s)")
        params.append(list(all_excludes))

    where_sql = (" where " + " and ".join(where_clauses)) if where_clauses else ""
    return where_sql, params

# ------------- endpoint -------------

@router.get("/for-me")
def recs_for_me(
    user_id: str = Query(..., description="Your user id (e.g., 'user:louise')"),
    k: int = Query(20, ge=1, le=100, description="How many recommendations to return"),
    min_votes: int = Query(50, ge=0, description="Minimum vote count for candidates"),
    since_year: int = Query(1900, ge=0, description="Only titles with start_year >= since_year"),
    allow_adult: bool = Query(False, description="If true, adult titles may appear"),
    genre_any: Optional[str] = Query(None, description="Comma-separated list, e.g., 'Comedy,Horror,Sci-Fi'"),
    exclude_tconst: Optional[str] = Query(None, description="Comma-separated tconst list to exclude"),
):
    # Parse CSV params
    genre_list = [g.strip() for g in genre_any.split(",")] if genre_any else []
    exclude_list = [t.strip() for t in exclude_tconst.split(",")] if exclude_tconst else []

    # 1) Fetch seeds for this user
    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(SEED_SQL, (user_id,))
            seed_rows = cur.fetchall()

    if not seed_rows:
        raise HTTPException(status_code=404, detail=f"No seeds found for user_id={user_id}. Insert into analytics.int_user_seed_preferences first.")

    # 2) Build weighted centroid vector
    total_w = 0.0
    centroid = [0.0] * len(FEATURE_COLS)
    seed_tconsts: List[str] = []
    for r in seed_rows:
        vec = _row_to_vec(r)
        w = float(r.get("weight") or 1.0)
        total_w += w
        centroid = [c + w * x for c, x in zip(centroid, vec)]
        seed_tconsts.append(r["tconst"])
    if total_w > 0:
        centroid = [c / total_w for c in centroid]

    # 3) Stream candidates with filters, score, keep top-k
    where_sql, params = _build_candidate_where(
        allow_adult=allow_adult,
        min_votes=min_votes,
        since_year=since_year,
        genre_any=genre_list,
        exclude_tconst=exclude_list,
        seed_tconsts=seed_tconsts,
    )
    sql = CANDIDATE_BASE_SQL + where_sql
    results: List[Dict[str, Any]] = []

    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor(name="cand_cur", row_factory=psycopg.rows.dict_row) as cur:
            # Server-side cursor to avoid loading everything at once
            cur.execute(sql, params)
            fetch_size = 2000
            while True:
                batch = cur.fetchmany(fetch_size)
                if not batch:
                    break
                for row in batch:
                    vec = _row_to_vec(row)
                    score = _cosine(centroid, vec)
                    results.append({
                        "tconst": row["tconst"],
                        "primary_title": row["primary_title"],
                        "english_title": row["english_title"],
                        "start_year": row["start_year"],
                        "average_rating": row["average_rating"],
                        "num_votes": row["num_votes"],
                        "score": score,
                    })

    # 4) Sort on score (desc) then rating (desc) then votes (desc)
    results.sort(key=lambda r: (r["score"], r.get("average_rating") or 0.0, r.get("num_votes") or 0), reverse=True)
    return {
        "user_id": user_id,
        "k": k,
        "used_seeds": [
            {"tconst": r["tconst"], "title": r["primary_title"], "weight": float(r.get("weight") or 1.0)}
            for r in seed_rows
        ],
        "filters": {
            "min_votes": min_votes, "since_year": since_year, "allow_adult": allow_adult,
            "genre_any": genre_list, "exclude_tconst": exclude_list
        },
        "results": results[:k],
    }

# backend/app/recs.py
# Purpose: Content-based recommendations over analytics.int_titles_features
# Notes:
# - Reads user seed titles from analytics.int_user_seed_preferences
# - Builds a single preference vector (weighted average) from seed rows
# - Computes cosine similarity to candidate titles and returns top-k
# - Supports filters: since_year, min_votes, genre_any (comma-separated), title_types
# - Uses container-safe DSN (db:5432) by default

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
import os
import math
import json
import psycopg

def _ensure_vec(v):
    # psycopg usually returns JSON as a Python list already.
    if isinstance(v, (list, tuple)):
        return [float(x) for x in v]
    if isinstance(v, memoryview):
        v = v.tobytes()
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8")
    if isinstance(v, str):
        return [float(x) for x in json.loads(v)]
    # Fallback (shouldn't happen)
    return []
  
def _as_vec(value):
    # psycopg3 usually returns JSON/JSONB as native Python (list/dict).
    # If we ever get a string, fall back to json.loads.
    if isinstance(value, (list, tuple)):
        return list(value)
    return json.loads(value)
  



router = APIRouter(prefix="/recs", tags=["recs"])

def _dsn() -> str:
    # Prefer ANALYTICS_DSN or fall back to DATABASE_URL
    dsn = os.getenv("ANALYTICS_DSN") or os.getenv("DATABASE_URL") or ""

    # If someone put a host-only DSN, keep it.
    # If it uses localhost/127.0.0.1, rewrite to Docker service "db" on the correct internal port 5432.
    # We normalize both host and port robustly.
    def _normalize_local_to_db(s: str) -> str:
        # Common cases:
        #   postgresql://user:pass@localhost:5433/db -> ...@db:5432/db
        #   postgresql://user:pass@127.0.0.1:5433/db -> ...@db:5432/db
        #   postgresql://user:pass@localhost/db      -> ...@db:5432/db
        for needle in ("@localhost:", "@127.0.0.1:"):
            if needle in s:
                return s.replace(needle, "@db:5432:")
        # handle no explicit port
        for needle in ("@localhost/", "@127.0.0.1/"):
            if needle in s:
                return s.replace(needle, "@db:5432/")
        return s

    dsn = _normalize_local_to_db(dsn)

    # Final fallback if empty
    return dsn or "postgresql://cinegraph_user:changeme_strong_password@db:5432/cinegraph"


DB_DSN = _dsn()

# Which columns become our feature vector (documentation only; we now build it in SQL)
# These names match analytics.int_titles_features (see \d+ in your logs)
FEATURE_DOC = [
    "f_year_norm", "f_runtime_norm", "f_rating_norm",
    # one-hots for genres present in the title (multi-hot)
    "g_action","g_adventure","g_animation","g_comedy","g_crime","g_documentary",
    "g_drama","g_family","g_fantasy","g_history","g_horror","g_music","g_mystery",
    "g_romance","g_scifi","g_tvmovie","g_thriller","g_war","g_western"
]

# Map user-friendly genre tokens -> warehouse boolean columns
GENRE_TO_COL = {
    "action":"g_action",
    "adventure":"g_adventure",
    "animation":"g_animation",
    "comedy":"g_comedy",
    "crime":"g_crime",
    "documentary":"g_documentary",
    "drama":"g_drama",
    "family":"g_family",
    "fantasy":"g_fantasy",
    "history":"g_history",
    "horror":"g_horror",
    "music":"g_music",
    "mystery":"g_mystery",
    "romance":"g_romance",
    "sci-fi":"g_scifi",  # allow sci-fi
    "scifi":"g_scifi",
    "science fiction":"g_scifi",
    "tv movie":"g_tvmovie",
    "tvmovie":"g_tvmovie",
    "thriller":"g_thriller",
    "war":"g_war",
    "western":"g_western",
}

def _cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x*y for x, y in zip(a, b))
    na = math.sqrt(sum(x*x for x in a))
    nb = math.sqrt(sum(y*y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)

# --- Helper: same JSON feature vector expression used for seeds & candidates ---
# We COALESCE to 0.0 and cast booleans to int so Python gets a clean list[float].
FEATURE_JSON_SQL = """
json_build_array(
  COALESCE(f.f_year_norm::double precision, 0.0),
  COALESCE(f.f_runtime_norm::double precision, 0.0),
  COALESCE(f.f_rating_norm::double precision, 0.0),

  (CASE WHEN f.g_action THEN 1 ELSE 0 END),
  (CASE WHEN f.g_adventure THEN 1 ELSE 0 END),
  (CASE WHEN f.g_animation THEN 1 ELSE 0 END),
  (CASE WHEN f.g_comedy THEN 1 ELSE 0 END),
  (CASE WHEN f.g_crime THEN 1 ELSE 0 END),
  (CASE WHEN f.g_documentary THEN 1 ELSE 0 END),
  (CASE WHEN f.g_drama THEN 1 ELSE 0 END),
  (CASE WHEN f.g_family THEN 1 ELSE 0 END),
  (CASE WHEN f.g_fantasy THEN 1 ELSE 0 END),
  (CASE WHEN f.g_history THEN 1 ELSE 0 END),
  (CASE WHEN f.g_horror THEN 1 ELSE 0 END),
  (CASE WHEN f.g_music THEN 1 ELSE 0 END),
  (CASE WHEN f.g_mystery THEN 1 ELSE 0 END),
  (CASE WHEN f.g_romance THEN 1 ELSE 0 END),
  (CASE WHEN f.g_scifi THEN 1 ELSE 0 END),
  (CASE WHEN f.g_tvmovie THEN 1 ELSE 0 END),
  (CASE WHEN f.g_thriller THEN 1 ELSE 0 END),
  (CASE WHEN f.g_war THEN 1 ELSE 0 END),
  (CASE WHEN f.g_western THEN 1 ELSE 0 END)
) AS feature_vec
"""

def _fetch_seeds(conn, user_id: str):
    # CHANGED: We no longer read f.feature_vec (doesn't exist).
    # Instead, compute it via FEATURE_JSON_SQL.
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT s.tconst, s.weight, {FEATURE_JSON_SQL}
            FROM analytics.int_user_seed_preferences s
            JOIN analytics.int_titles_features f USING (tconst)
            WHERE s.user_id = %s
        """, (user_id,))
        rows = cur.fetchall()
        return [{"tconst": r[0], "weight": float(r[1]), "vec": _as_vec(r[2])} for r in rows]



def _build_pref_vector(seeds: List[dict]) -> Optional[List[float]]:
    if not seeds:
        return None
    # weighted average
    total_w = sum(s["weight"] for s in seeds)
    if total_w <= 0:
        return None
    k = len(seeds[0]["vec"])
    acc = [0.0] * k
    for s in seeds:
        w = s["weight"] / total_w
        v = s["vec"]
        for i in range(k):
            acc[i] += w * v[i]
    return acc

def _fetch_candidates(
    conn,
    since_year: Optional[int],
    min_votes: int,
    genre_any: Optional[List[str]],
    exclude_tconsts: List[str],
    title_types: Optional[str] = None,
    lang_any: Optional[List[str]] = None,
    region_any: Optional[List[str]] = None,
    include_adult: bool = True,
):
    # CHANGED: align names with view: f.start_year, f.num_votes
    where = ["f.num_votes >= %s", "f.start_year >= %s"]
    params: List[object] = [min_votes, since_year or 0]

    # exclude adult unless explicitly allowed
    if not include_adult:
        where.append("COALESCE(f.f_is_adult, false) = false")

    # genre filter: any match among provided
    if genre_any:
        ors = []
        for g in genre_any:
            token = g.strip().lower().replace("-", " ")
            token = token if token in GENRE_TO_COL else g.strip().lower()
            col = GENRE_TO_COL.get(token)
            if col:
                ors.append(f"f.{col} = TRUE")
        if ors:
            where.append("(" + " OR ".join(ors) + ")")

    if exclude_tconsts:
        where.append("f.tconst <> ALL(%s)")
        params.append(exclude_tconsts)

    # titleType filter (defaults to 'movie' at the endpoint layer)
    types = tuple(t.strip() for t in title_types.split(",")) if title_types else tuple()
    title_filter_sql = ""
    if types:
        if len(types) == 1:
            title_filter_sql = ' AND b."titleType" = %s'
            params.append(types[0])
        else:
            placeholders = ",".join(["%s"] * len(types))
            title_filter_sql = f' AND b."titleType" IN ({placeholders})'
            params.extend(types)

    # AKA-based language/region filters via EXISTS
    aka_filters = []
    if lang_any:
        aka_filters.append("""
            EXISTS (
              SELECT 1 FROM stg_title_akas a
               WHERE a."titleId" = f.tconst
                 AND a.language = ANY(%s)
            )
        """)
        params.append(lang_any)
    if region_any:
        aka_filters.append("""
            EXISTS (
              SELECT 1 FROM stg_title_akas a
               WHERE a."titleId" = f.tconst
                 AND a.region = ANY(%s)
            )
        """)
        params.append(region_any)
    if aka_filters:
        where.append("(" + " AND ".join(aka_filters) + ")")

    sql = f"""
        SELECT
          f.tconst,
          f.primary_title,
          f.english_title,
          f.start_year,
          f.average_rating,
          f.num_votes,
          {FEATURE_JSON_SQL}
        FROM analytics.int_titles_features f
        JOIN stg_title_basics b ON b.tconst = f.tconst
        WHERE {" AND ".join(where)}
        {title_filter_sql}
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [
            {
                "tconst": r[0],
                "primary_title": r[1],
                "english_title": r[2],
                "start_year": r[3],
                "average_rating": float(r[4]) if r[4] is not None else None,
                "num_votes": r[5],
                "vec": _ensure_vec(r[6]),
            }
            for r in rows
        ]

@router.get("/for-me")
def recs_for_me(
    user_id: str,
    k: int = 25,
    min_votes: int = 100,
    since_year: int = 1900,
    genre_any: Optional[str] = None,
    title_types: Optional[str] = Query(
        "movie",
        description="Comma-separated IMDb titleType filter, e.g. 'movie', 'movie,short', 'tvMovie'. Default: movie"
    ),
    orig_lang_any: Optional[str] = Query(
        None, description="Comma-separated ISO 639-1 language codes (from AKAs), e.g. 'en,fr'"
    ),
    aka_region_any: Optional[str] = Query(
        None, description="Comma-separated regions in AKAs (e.g. 'US,GB,XWW')"
    ),
    include_adult: bool = Query(
        False, description="Include adult titles (default: false)"
    ),
):
    # parse genres if provided (comma-separated; case-insensitive)
    genre_list = [g.strip() for g in genre_any.split(",")] if genre_any else None
    lang_list  = [x.strip() for x in orig_lang_any.split(",")] if orig_lang_any else None
    region_list= [x.strip() for x in aka_region_any.split(",")] if aka_region_any else None

    with psycopg.connect(DB_DSN) as conn:
        seeds = _fetch_seeds(conn, user_id)
        if not seeds:
            raise HTTPException(
                status_code=404,
                detail=f"No seeds found for user_id={user_id}. Insert into analytics.int_user_seed_preferences first."
            )

        pref = _build_pref_vector(seeds)
        if not pref:
            raise HTTPException(status_code=400, detail="Invalid seed weights or vectors")

        exclude = [s["tconst"] for s in seeds]
        cands = _fetch_candidates(
            conn,
            since_year,
            min_votes,
            genre_list,
            exclude,
            title_types,
            lang_list,
            region_list,
            include_adult,
        )

    # score candidates in Python via cosine
    scored = []
    for c in cands:
        score = _cosine(pref, c["vec"])
        if score > 0:
            scored.append({
                "tconst": c["tconst"],
                "primary_title": c["primary_title"],
                "english_title": c["english_title"],
                "start_year": c["start_year"],
                "average_rating": c["average_rating"],
                "num_votes": c["num_votes"],
                "score": score,
            })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return {"user_id": user_id, "count": len(scored[:k]), "results": scored[:k]}
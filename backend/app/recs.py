# backend/app/recs.py
# Purpose: Content-based recommendations over analytics.int_titles_features
# Notes:
# - Reads user seed titles from analytics.int_user_seed_preferences
# - Builds a single preference vector (weighted average) from seed rows
# - Computes cosine similarity to candidate titles and returns top-k
# - Supports filters: since_year, min_votes, genre_any (comma-separated)
# - Uses container-safe DSN (db:5432) by default

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
import os
import math
import json
import psycopg

router = APIRouter(prefix="/recs", tags=["recs"])

def _dsn() -> str:
    dsn = os.getenv("ANALYTICS_DSN") or os.getenv("DATABASE_URL") or ""
    # guard: if someone set localhost/127.0.0.1 for CLI, rewrite for container
    dsn = dsn.replace("@localhost:", "@db:").replace("@127.0.0.1:", "@db:")
    return dsn or "postgresql://cinegraph_user:changeme_strong_password@db:5432/cinegraph"

DB_DSN = _dsn()

# Columns used for features — must match analytics.int_titles_features
FEATURE_COLS = [
    "year_norm", "runtime_norm", "rating_norm",
    "is_action", "is_comedy", "is_drama", "is_romance", "is_horror",
    "is_war", "is_thriller", "is_crime", "is_fantasy", "is_scifi",
    "is_family", "is_western", "is_animation", "is_documentary"
]

def _cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x*y for x, y in zip(a, b))
    na = math.sqrt(sum(x*x for x in a))
    nb = math.sqrt(sum(y*y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)

def _fetch_seeds(conn, user_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            select s.tconst, s.weight, f.feature_vec
            from analytics.int_user_seed_preferences s
            join analytics.int_titles_features f using (tconst)
            where s.user_id = %s
        """, (user_id,))
        rows = cur.fetchall()
        return [{"tconst": r[0], "weight": float(r[1]), "vec": json.loads(r[2])} for r in rows]

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

def _fetch_candidates(conn, since_year: Optional[int], min_votes: int,
                      genre_any: Optional[List[str]], exclude_tconsts: List[str]):

    where = ["f.vote_count >= %s", "f.year >= %s"]
    params: List[object] = [min_votes, since_year or 0]

    # genre filter: any match
    genre_sql = []
    if genre_any:
        for g in genre_any:
            g = g.strip().lower()
            # map input “comedy” -> column is_comedy
            genre_col = f"is_{g}"
            genre_sql.append(f"f.{genre_col} = true")
        if genre_sql:
            where.append("(" + " OR ".join(genre_sql) + ")")

    if exclude_tconsts:
        where.append("f.tconst <> ALL(%s)")
        params.append(exclude_tconsts)

    sql = f"""
        select f.tconst, f.title, f.original_title, f.year, f.vote_count, f.feature_vec
        from analytics.int_titles_features f
        where {" AND ".join(where)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [
            {
                "tconst": r[0],
                "title": r[1],
                "original_title": r[2],
                "year": r[3],
                "vote_count": r[4],
                "vec": json.loads(r[5]),
            }
            for r in rows
        ]

@router.get("/for-me")
def recs_for_me(
    user_id: str,
    k: int = 25,
    min_votes: int = 100,
    since_year: int = 1900,
    genre_any: Optional[str] = Query(None, description="comma-separated, e.g. Comedy,Horror")
):
    # parse genres if provided
    genre_list = [g.strip() for g in genre_any.split(",")] if genre_any else None

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
        cands = _fetch_candidates(conn, since_year, min_votes, genre_list, exclude)

    # score candidates
    scored = []
    for c in cands:
        score = _cosine(pref, c["vec"])
        if score > 0:
            scored.append({
                "tconst": c["tconst"],
                "title": c["title"],
                "original_title": c["original_title"],
                "year": c["year"],
                "vote_count": c["vote_count"],
                "score": score,
            })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return {"user_id": user_id, "count": len(scored[:k]), "results": scored[:k]}

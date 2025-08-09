"""
Statistics routes.

GET /stats/film-count?person=Jean-Claude%20Van%20Damme&genre=Action

Returns:
{
  "person": "Jean-Claude Van Damme",
  "genre": "Action",
  "count": 37,
  "titles": [
    {"id": 85, "title": "Bloodsport", "original_title": "Bloodsport", "year": 1988},
    ...
  ]
}

Notes:
- Uses TMDB as a first pass. We'll switch to IMDb-backed warehouse counts later.
- Person resolution picks the best match by popularity, preferring exact/near-exact names.
"""

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query
from .tmdb import tmdb

router = APIRouter(prefix="/stats", tags=["stats"])


def _normalize(s: str) -> str:
    return "".join(ch.lower() for ch in s if ch.isalnum() or ch.isspace()).strip()


async def _resolve_person_id(name: str) -> Optional[int]:
    # Search person
    res = await tmdb.search_person(query=name, language="en-US", page=1)
    results: List[Dict[str, Any]] = res.get("results", [])
    if not results:
        return None

    norm = _normalize(name)
    # Try to find an exact-ish match, else take the most popular
    exacts = [p for p in results if _normalize(p.get("name", "")) == norm]
    chosen = (exacts or results)[0]
    return int(chosen["id"])


async def _genre_name_to_id(genre_name: str) -> Optional[int]:
    data = await tmdb.genre_movie_list(language="en-US")
    genres = data.get("genres", [])
    by_name = {g["name"].lower(): int(g["id"]) for g in genres}
    return by_name.get(genre_name.lower())


def _extract_year(date_str: Optional[str]) -> Optional[int]:
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except Exception:
        return None


@router.get("/film-count")
async def film_count(
    person: str = Query(..., description="Person full name, e.g. 'Jean-Claude Van Damme'"),
    genre: str = Query(..., description="Genre name, e.g. 'Action'"),
):
    # Resolve person
    person_id = await _resolve_person_id(person)
    if not person_id:
        raise HTTPException(status_code=404, detail=f"Person not found: {person}")

    # Resolve genre name -> id
    genre_id = await _genre_name_to_id(genre)
    if genre_id is None:
        raise HTTPException(status_code=400, detail=f"Unknown genre: {genre}")

    # Fetch person movie credits
    credits = await tmdb.person_movie_credits(person_id, language="en-US")
    cast_movies: List[Dict[str, Any]] = credits.get("cast", [])
    crew_movies: List[Dict[str, Any]] = credits.get("crew", [])

    def _matches(m: Dict[str, Any]) -> bool:
        gids = m.get("genre_ids") or []
        return genre_id in gids

    # Filter both cast and crew (many actors also produce/direct)
    filtered: List[Dict[str, Any]] = [m for m in cast_movies if _matches(m)] + [m for m in crew_movies if _matches(m)]

    # Deduplicate by movie id
    seen = set()
    unique: List[Dict[str, Any]] = []
    for m in filtered:
        mid = m.get("id")
        if mid in seen:
            continue
        seen.add(mid)
        unique.append(m)

    # Build response list (sorted by year)
    titles = [
        {
            "id": int(m["id"]),
            "title": m.get("title") or m.get("original_title") or "",
            "original_title": m.get("original_title") or m.get("title") or "",
            "year": _extract_year(m.get("release_date")),
        }
        for m in unique
    ]
    titles.sort(key=lambda x: (x["year"] or 0, x["title"]))

    return {
        "person": person,
        "genre": genre,
        "count": len(titles),
        "titles": titles,
    }

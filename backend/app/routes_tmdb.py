"""
TMDB utility routes.

- GET /tmdb/ping
  Calls TMDB /genre/movie/list to verify credentials and connectivity.

Return example:
{
  "ok": true,
  "genres_count": 19,
  "sample": [{"id": 35, "name": "Comedy"}, {"id": 28, "name": "Action"}]
}
"""

from fastapi import APIRouter, HTTPException
from .tmdb import tmdb

router = APIRouter(prefix="/tmdb", tags=["tmdb"])


@router.get("/ping")
async def tmdb_ping():
    """
    Make a lightweight call to TMDB to verify that our Bearer/Key works.
    """
    try:
        data = await tmdb.genre_movie_list(language="en-US")
        genres = data.get("genres", [])
        return {
            "ok": True,
            "genres_count": len(genres),
            "sample": genres[:2],
        }
    except Exception as exc:
        # Bubble up a clear error so we can debug tokens/network quickly.
        raise HTTPException(status_code=502, detail=f"TMDB ping failed: {exc}")

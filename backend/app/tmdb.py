"""
Lightweight TMDB client for CineGraph.

Endpoints covered:
- GET /movie/{movie_id}
- GET /movie/{movie_id}/credits
- GET /movie/{movie_id}/keywords
- GET /genre/movie/list
- GET /discover/movie
- GET /credit/{credit_id}

Auth:
- Prefer TMDB v4 "Read Access Token" via Authorization: Bearer <token>.
- Fallback to TMDB v3 "API Key" as ?api_key=... (rarely needed if v4 present).

Notes:
- We keep this tiny and robust: timeouts, retries, friendly error messages.
- Returns raw dicts. We can layer Pydantic models if/when you want type safety.

Usage:
    from .tmdb import tmdb

    data = await tmdb.movie_details(289)  # Casablanca example
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx
from loguru import logger

TMDB_BASE_URL = "https://api.themoviedb.org/3"


def _auth_headers() -> Dict[str, str]:
    """
    Prefer v4 Bearer token; it's valid for all read endpoints.
    """
    v4 = os.getenv("TMDB_READ_TOKEN")
    if v4:
        return {
            "Authorization": f"Bearer {v4}",
            "Accept": "application/json"
        }
    # fallback: we'll rely on ?api_key query if only v3 is available
    return {"Accept": "application/json"}


def _api_key() -> Optional[str]:
    """Return TMDB v3 API key if present (used as a fallback)."""
    return os.getenv("TMDB_API_KEY")


class TMDBClient:
    """
    Small async client with:
    - timeout: 15s total
    - 3 retries on network errors and 429/5xx with backoff
    """

    def __init__(self) -> None:
        self._headers = _auth_headers()
        self._api_key = _api_key()
        self._client = httpx.AsyncClient(
            base_url=TMDB_BASE_URL,
            headers=self._headers,
            timeout=httpx.Timeout(15.0, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        )

    async def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = params.copy() if params else {}
        # If we don't have v4 token, fall back to v3 query key
        if self._api_key:
            params.setdefault("api_key", self._api_key)
        if "Authorization" not in self._headers and self._api_key:
            params.setdefault("api_key", self._api_key)

        attempt = 0
        backoff = 0.5
        while True:
            attempt += 1
            try:
                resp = await self._client.request(method, path, params=params)
            except httpx.RequestError as e:
                if attempt <= 3:
                    logger.warning(f"TMDB network error (attempt {attempt}/3): {e}. Retrying in {backoff:.1f}s")
                    await httpx.AsyncClient().aclose()  # no-op; just yield to loop
                    import asyncio
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                raise

            # Retry on rate-limit or transient 5xx
            if resp.status_code in (429, 500, 502, 503, 504) and attempt <= 3:
                retry_after = float(resp.headers.get("Retry-After", backoff))
                logger.warning(f"TMDB {resp.status_code} (attempt {attempt}/3). Retrying in {retry_after:.1f}s")
                import asyncio
                await asyncio.sleep(retry_after)
                backoff *= 2
                continue

            if resp.status_code >= 400:
                # Raise a helpful error
                try:
                    payload = resp.json()
                except Exception:
                    payload = {"message": resp.text[:300]}
                raise httpx.HTTPStatusError(
                    f"TMDB error {resp.status_code} for {path}: {payload}",
                    request=resp.request,
                    response=resp,
                )

            return resp.json()

    # ---- Public helpers ----

    async def movie_details(self, movie_id: int, *, language: str = "en-US", append_to_response: Optional[str] = None) -> Dict[str, Any]:
        params = {"language": language}
        if append_to_response:
            params["append_to_response"] = append_to_response
        return await self._request("GET", f"/movie/{movie_id}", params)

    async def movie_credits(self, movie_id: int, *, language: str = "en-US") -> Dict[str, Any]:
        return await self._request("GET", f"/movie/{movie_id}/credits", {"language": language})

    async def movie_keywords(self, movie_id: int) -> Dict[str, Any]:
        return await self._request("GET", f"/movie/{movie_id}/keywords")

    async def genre_movie_list(self, *, language: str = "en-US") -> Dict[str, Any]:
        return await self._request("GET", "/genre/movie/list", {"language": language})

    async def discover_movie(self, **filters: Any) -> Dict[str, Any]:
        """
        Direct passthrough for Advanced Filtering / AND-OR logic.
        Example:
            await tmdb.discover_movie(
                with_genres="35",                 # comedy
                with_origin_country="AT",         # Austria
                primary_release_date_gte="1930-01-01",
                primary_release_date_lte="1939-12-31",
                sort_by="popularity.desc",
                include_adult="false",
                language="en-US",
                page=1,
            )
        """
        return await self._request("GET", "/discover/movie", filters)
    async def search_person(self, query: str, *, language: str = "en-US", page: int = 1) -> Dict[str, Any]:
        """
        GET /search/person
        Finds people by name. We'll pick the best match (by popularity) when needed.
        """
        params = {"query": query, "language": language, "page": page, "include_adult": "false"}
        return await self._request("GET", "/search/person", params)

    async def person_movie_credits(self, person_id: int, *, language: str = "en-US") -> Dict[str, Any]:
        """
        GET /person/{person_id}/movie_credits
        Returns cast & crew arrays with genre_ids and titles/years. We’ll filter client-side.
        """
        return await self._request("GET", f"/person/{person_id}/movie_credits", {"language": language})

    async def credit_details(self, credit_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/credit/{credit_id}")

    async def aclose(self) -> None:
        await self._client.aclose()


# Singleton-ish instance for easy imports
tmdb = TMDBClient()

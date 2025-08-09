"""
CineGraph FastAPI entrypoint.

What this file does (v0.3):
- Creates a FastAPI app with CORS.
- Loads settings from environment variables.
- Exposes:
    GET /           -> sanity hello
    GET /healthz    -> returns {"status": "ok"} and checks DB connectivity
    GET /tmdb/ping  -> TMDB connectivity/credentials test
- Uses centralized DB helpers from app.db
- Registers graceful shutdown for the TMDB client.
"""

from typing import List
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Centralized DB + ping
from .db import ping_db

# Routers / services
from .routes_tmdb import router as tmdb_router
from .routes_stats import router as stats_router
from app.recs import router as recs_router
from .tmdb import tmdb


# ---------------------------
# Settings (env-driven config)
# ---------------------------

class Settings(BaseModel):
    app_env: str = Field(default=os.getenv("APP_ENV", "development"))
    cors_allow_origins: str = Field(default=os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:3000"))

    @property
    def cors_origins(self) -> List[str]:
        # Allow comma-separated origins in the env var
        return [o.strip() for o in self.cors_allow_origins.split(",") if o.strip()]

settings = Settings()


# ---------------------------
# Lifespan (startup/shutdown)
# ---------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: nothing special yet
    yield
    # Shutdown: close TMDB client cleanly
    try:
        await tmdb.aclose()
    except Exception:
        pass


# ---------------------------
# FastAPI app
# ---------------------------

app = FastAPI(
    title="CineGraph API",
    description="Movie intelligence & recommender backend (v0.3)",
    version="0.3.0",
    lifespan=lifespan,
)

# CORS: allow local Next.js dev server by default
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------
# Routes
# ---------------------------

@app.get("/", tags=["meta"])
def root():
    return {
        "app": "CineGraph API",
        "env": settings.app_env,
        "docs": "/docs",
        "health": "/healthz"
    }

@app.get("/healthz", tags=["meta"])
def healthz():
    """
    Liveness/readiness check. Also verifies DB connectivity.
    """
    return {
        "status": "ok",
        "database": "ok" if ping_db() else "unreachable"
    }

# TMDB utility routes
app.include_router(tmdb_router)

app.include_router(stats_router)

app.include_router(recs_router)
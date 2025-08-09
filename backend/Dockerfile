# ---- Base image ----
FROM python:3.12-slim

# ---- Environment sanity ----
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# (Optional) system deps — slim image already has most we need thanks to psycopg[binary]
# If we ever need build tools: uncomment the block below
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential curl && \
#     rm -rf /var/lib/apt/lists/*

# ---- Working dir ----
WORKDIR /app

# ---- Install Python deps first (leverages Docker layer caching) ----
# We copy only requirements first so subsequent code changes don't reinstall everything.
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# ---- App code (added incrementally) ----
# We'll add app/main.py in the next step. For now this keeps the dir structure.
COPY app/ /app/app/

# ---- Runtime config ----
EXPOSE 8000
ENV PORT=8000

# ---- Default command ----
# This will run once we add app/main.py (FastAPI entrypoint).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

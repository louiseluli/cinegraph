#!/usr/bin/env python3
"""
IMDb Non-Commercial Datasets — Polite Downloader (v0.1)

What this script does:
- Downloads selected IMDb .tsv.gz files from https://datasets.imdbws.com
- Saves them under ./data/imdb by default (override with --out)
- Polite: descriptive User-Agent, sequential downloads, retries + backoff
- Resumable: writes to .part and supports HTTP Range resume if possible
- Validates: checks gzip magic bytes + attempts a tiny read
- Writes a <filename>.sha256 with the file checksum for your records

Usage:
    # install deps
    pip install -r etl/requirements.txt

    # fetch all core files
    python etl/imdb_fetch.py --all

    # fetch only some files
    python etl/imdb_fetch.py --files title.basics,title.akas

    # custom output dir
    python etl/imdb_fetch.py --all --out ./data/imdb

Notes:
- This script does NOT load to Postgres. The loader is in imdb_load.py (next step).
- We respect IMDb ToS: personal/non-commercial use; store locally; no redistribution.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
import gzip
from pathlib import Path
from typing import Iterable, List, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

IMDB_BASE = "https://datasets.imdbws.com/"

DEFAULT_FILES = [
    "name.basics.tsv.gz",
    "title.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]

# Polite, descriptive UA; can be overridden via env
DEFAULT_UA = os.getenv(
    "IMDB_USER_AGENT",
    "CineGraph-ETL/0.1 (+contact: your-email@example.com)"
)


def make_session() -> requests.Session:
    """Create a requests session with retries and timeouts."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("HEAD", "GET"),
        raise_on_status=False,
    )
    s.headers.update({
        "User-Agent": DEFAULT_UA,
        "Accept": "application/octet-stream",
    })
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Polite downloader for IMDb TSV.GZ datasets")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help="Download all core IMDb files")
    g.add_argument("--files", type=str, help="Comma-separated subset, e.g. title.basics,title.akas")

    p.add_argument("--out", type=str, default="data/imdb", help="Output directory (default: data/imdb)")
    p.add_argument("--resume", action="store_true", help="Resume partial downloads if possible (default: True)", default=True)
    p.add_argument("--overwrite", action="store_true", help="Force re-download even if file exists")
    return p.parse_args()


def ensure_out_dir(path: str) -> Path:
    out = Path(path).resolve()
    out.mkdir(parents=True, exist_ok=True)
    return out


def get_targets(args: argparse.Namespace) -> List[str]:
    if args.all:
        return DEFAULT_FILES
    names = [f.strip() for f in args.files.split(",") if f.strip()]
    # quick validation
    for n in names:
        if not n.endswith(".tsv.gz"):
            raise SystemExit(f"Invalid file name: {n} (must end with .tsv.gz)")
    return names


def sha256_file(p: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_gzip(p: Path) -> Tuple[bool, str]:
    """
    Check gzip magic bytes and try a tiny read to ensure file is not corrupted.
    Returns (ok, err_msg).
    """
    try:
        with p.open("rb") as f:
            magic = f.read(2)
            if magic != b"\x1f\x8b":
                return False, "Missing gzip magic bytes"
        # try small read
        with gzip.open(p, "rb") as gz:
            _ = gz.read(64)  # tiny read
        return True, ""
    except Exception as e:
        return False, str(e)


def download_file(session: requests.Session, url: str, dest: Path, resume: bool = True, overwrite: bool = False) -> None:
    """
    Download with .part file and (if server supports) HTTP Range resume.
    """
    part = dest.with_suffix(dest.suffix + ".part")

    if dest.exists() and not overwrite:
        print(f"✔ Exists, skipping: {dest.name}")
        return

    headers = {}
    mode = "wb"
    existing = 0

    if resume and part.exists():
        existing = part.stat().st_size
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"

    with session.get(url, stream=True, headers=headers, timeout=(5, 30)) as r:
        if r.status_code in (416,):  # range not satisfiable -> start fresh
            part.unlink(missing_ok=True)
            existing = 0
            headers.pop("Range", None)
            r.close()
            time.sleep(0.2)
            r = session.get(url, stream=True, timeout=(5, 30))

        r.raise_for_status()

        # Content length might be total or remaining (if Range)
        total = r.headers.get("Content-Length")
        total = int(total) + existing if total is not None else None

        desc = f"↓ {dest.name}"
        with open(part, mode) as f, tqdm(
            total=total, initial=existing, unit="B", unit_scale=True, desc=desc
        ) as bar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                bar.update(len(chunk))

    # Atomically move .part -> final
    part.rename(dest)

    # Validate gzip and write checksum
    ok, err = validate_gzip(dest)
    if not ok:
        raise RuntimeError(f"Gzip validation failed for {dest.name}: {err}")
    checksum = sha256_file(dest)
    with dest.with_suffix("").with_suffix(dest.suffix + ".sha256").open("w") as c:
        c.write(f"{checksum}  {dest.name}\n")


def main() -> None:
    args = parse_args()
    out_dir = ensure_out_dir(args.out)
    targets = get_targets(args)

    session = make_session()
    print(f"User-Agent: {session.headers.get('User-Agent')}")
    print(f"Output dir: {out_dir}")
    print(f"Files: {', '.join(targets)}")

    for name in targets:
        url = IMDB_BASE + name
        dest = out_dir / name
        try:
            download_file(session, url, dest, resume=args.resume, overwrite=args.overwrite)
            print(f"✔ Done: {name}")
        except Exception as e:
            print(f"✖ Failed: {name} — {e}", file=sys.stderr)
            sys.exit(1)

    print("All downloads completed.")


if __name__ == "__main__":
    main()

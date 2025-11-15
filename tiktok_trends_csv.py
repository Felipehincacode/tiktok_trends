#!/usr/bin/env python3
"""CLI tool to fetch viral TikTok videos for a list of keywords."""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from TikTokApi import TikTokApi

DEFAULT_MIN_LIKES = 25_000
DEFAULT_MAX_PER_KEYWORD = 20
FETCH_COUNT_PER_KEYWORD = 200


def parse_ms_tokens(raw_tokens: Iterable[str]) -> List[str]:
    """Normalize ms_tokens preserving order and removing duplicates."""

    seen = set()
    tokens: List[str] = []
    for raw in raw_tokens:
        trimmed = raw.strip()
        if not trimmed or trimmed in seen:
            continue
        tokens.append(trimmed)
        seen.add(trimmed)
    return tokens


def get_ms_tokens() -> List[str]:
    """Retrieve ms_token values from the environment."""

    collected: List[str] = []
    for env_name in ("ms_token", "ms_tokens", "MS_TOKEN", "MS_TOKENS"):
        value = os.getenv(env_name)
        if not value:
            continue
        collected.extend(re.split(r"[\s,;]+", value))

    tokens = parse_ms_tokens(collected)
    if not tokens:
        raise RuntimeError(
            "No encontré ninguna cookie 'ms_token'. Configura la variable de entorno "
            "ms_token (o ms_tokens) con una o varias cookies separadas por coma o "
            "saltos de línea."
        )
    return tokens


def read_keywords(csv_path: Path) -> List[str]:
    """Read the first column of a CSV file and return cleaned keywords."""
    keywords: List[str] = []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            keyword = row[0].strip()
            if not keyword:
                continue
            if keyword.lower() in {"keyword", "palabra", "hashtag"}:
                continue
            keywords.append(keyword)
    return keywords


async def fetch_for_keyword(
    api: TikTokApi,
    keyword: str,
    min_likes: int,
    max_results: int,
) -> List[Dict[str, Any]]:
    """Fetch and filter videos for a specific hashtag keyword."""
    normalized = keyword.lstrip("#")
    results: List[Dict[str, Any]] = []

    hashtag = api.hashtag(name=normalized)

    async for video in hashtag.videos(count=FETCH_COUNT_PER_KEYWORD):
        data = video.as_dict
        stats = data.get("stats") or {}
        like_count = int(stats.get("diggCount", 0) or 0)

        if like_count < min_likes:
            continue

        author = data.get("author") or {}

        row: Dict[str, Any] = {
            "keyword": keyword,
            "video_id": data.get("id"),
            "description": data.get("desc"),
            "like_count": like_count,
            "comment_count": stats.get("commentCount"),
            "share_count": stats.get("shareCount"),
            "play_count": stats.get("playCount"),
            "author_uniqueId": author.get("uniqueId"),
            "author_nickname": author.get("nickname"),
            "url": video.url,
        }

        results.append(row)
        if len(results) >= max_results:
            break

    return results


def ensure_ms_tokens() -> List[str]:
    """Wrapper around get_ms_tokens for backwards compatibility."""

    return get_ms_tokens()


def read_keywords_safe(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    keywords = read_keywords(path)
    if not keywords:
        raise RuntimeError(f"No keywords found in the first column of {path}")
    return keywords


def write_rows(output_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """Write results to CSV, returning number of rows written."""
    rows_list = list(rows)

    if rows_list:
        fieldnames = list(rows_list[0].keys())
    else:
        fieldnames = [
            "keyword",
            "video_id",
            "description",
            "like_count",
            "comment_count",
            "share_count",
            "play_count",
            "author_uniqueId",
            "author_nickname",
            "url",
        ]

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_list:
            writer.writerow(row)

    return len(rows_list)


async def main_async(args: argparse.Namespace) -> None:
    """Main asynchronous entry point for the CLI tool."""
    input_path = Path(args.input)
    output_path = Path(args.output)
    min_likes = args.min_likes
    max_per_keyword = args.max_per_keyword

    ms_tokens = ensure_ms_tokens()
    keywords = read_keywords_safe(input_path)

    print(f"Voy a buscar TikToks para {len(keywords)} palabras...")
    print(
        "Inicializando sesiones con "
        f"{len(ms_tokens)} cookie(s) ms_token disponible(s)..."
    )

    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=ms_tokens,
            num_sessions=len(ms_tokens),
            sleep_after=3,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
        )

        aggregated: List[Dict[str, Any]] = []

        for keyword in keywords:
            print(f"\n--- Palabra / hashtag: '{keyword}' ---")
            try:
                rows = await fetch_for_keyword(
                    api=api,
                    keyword=keyword,
                    min_likes=min_likes,
                    max_results=max_per_keyword,
                )
            except Exception as exc:  # noqa: BLE001 - we want to log any error and continue
                print(f"  !! Error con '{keyword}': {exc}")
                continue

            print(
                f"  → {len(rows)} videos con ≥ {min_likes} likes."
            )
            aggregated.extend(rows)

    total = write_rows(output_path, aggregated)
    print(f"\n✅ Listo. Guardé {total} filas en: {output_path}")


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Buscar TikToks virales por lista de palabras/hashtags "
            "y exportar los resultados a CSV."
        )
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Ruta del CSV de entrada con las palabras (una por fila, primera columna).",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="tiktoks_virales.csv",
        help="Ruta del CSV de salida (por defecto: tiktoks_virales.csv).",
    )
    parser.add_argument(
        "--min-likes",
        type=int,
        default=DEFAULT_MIN_LIKES,
        help=(
            "Mínimo de likes para considerar un video "
            f"(default: {DEFAULT_MIN_LIKES})."
        ),
    )
    parser.add_argument(
        "--max-per-keyword",
        type=int,
        default=DEFAULT_MAX_PER_KEYWORD,
        help=(
            "Máximo de videos por palabra "
            f"(default: {DEFAULT_MAX_PER_KEYWORD})."
        ),
    )
    return parser


def main() -> None:
    """Parse arguments and execute the asynchronous main function."""
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()

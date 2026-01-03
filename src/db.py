import asyncio
from typing import Iterable, Optional, Sequence

import asyncpg

from .sources import Source


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    etag TEXT,
    last_modified TEXT,
    last_crawled TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS domains (
    domain TEXT PRIMARY KEY,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    notes TEXT
);
"""


async def init_pool(database_url: str) -> asyncpg.pool.Pool:
    return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=10)


async def init_schema(pool: asyncpg.pool.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


async def upsert_sources(pool: asyncpg.pool.Pool, sources: Sequence[Source]) -> None:
    if not sources:
        return
    async with pool.acquire() as conn:
        async with conn.transaction():
            for src in sources:
                await conn.execute(
                    """
                    INSERT INTO sources (name, url)
                    VALUES ($1, $2)
                    ON CONFLICT (name) DO UPDATE SET url = EXCLUDED.url;
                    """,
                    src.name,
                    src.url,
                )


async def get_source_state(pool: asyncpg.pool.Pool, name: str) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT etag, last_modified FROM sources WHERE name = $1",
            name,
        )


async def persist_domains(
    pool: asyncpg.pool.Pool,
    domains: Iterable[str],
    source_name: str,
) -> int:
    to_save = list({d.lower().strip(): None for d in domains}.keys())
    if not to_save:
        return 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO domains (domain, source)
                VALUES ($1, $2)
                ON CONFLICT (domain) DO UPDATE
                SET last_seen = NOW(), source = EXCLUDED.source;
                """,
                [(d, source_name) for d in to_save],
            )
    return len(to_save)


async def update_source_metadata(
    pool: asyncpg.pool.Pool,
    name: str,
    etag: Optional[str],
    last_modified: Optional[str],
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE sources
            SET etag = $2,
                last_modified = $3,
                last_crawled = NOW()
            WHERE name = $1;
            """,
            name,
            etag,
            last_modified,
        )


async def close_pool(pool: asyncpg.pool.Pool) -> None:
    try:
        await pool.close()
    except asyncio.CancelledError:
        pass

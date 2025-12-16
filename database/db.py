# db/db.py
import asyncpg
from core.settings import settings


# Postgres
db_pool: asyncpg.pool.Pool | None = None

async def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(dsn=settings.db.url)
        print("Database pool created")

async def close_db_pool():
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None
        print("Database pool closed")

async def get_db() -> asyncpg.pool.Pool:
    if db_pool is None:
        raise RuntimeError("Database pool is not initialized")
    return db_pool



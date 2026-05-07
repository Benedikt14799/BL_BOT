import asyncio
import os
import time

import asyncpg
from dotenv import load_dotenv

from database import DatabaseManager
from ebay_upload import run_upload_batch


def _extract_counts(result):
    """
    Try to infer counts from whatever run_upload_batch returns.
    Supports dict-like or object-like results. Falls back to zeros.
    """
    def get(key, default=0):
        if result is None:
            return default
        if isinstance(result, dict):
            return int(result.get(key, default) or 0)
        return int(getattr(result, key, default) or 0)

    success = get("success", get("uploaded", get("ok", 0)))
    failed = get("failed", get("errors", 0))
    skipped = get("skipped", 0)
    return success, failed, skipped


async def main():
    start = time.time()
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL missing in .env")

    pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5)
    try:
        await DatabaseManager.create_table(pool)
        result = await run_upload_batch(pool, limit=5000)
        success, failed, skipped = _extract_counts(result)
    finally:
        await pool.close()

    duration_s = int(round(time.time() - start))
    print(f"UPLOAD_SUMMARY success={success} failed={failed} skipped={skipped} duration_s={duration_s}")


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip("'\"")

async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        r_total_active = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id=1")
        r_listed = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id=1 AND ebay_listed=TRUE")
        r_with_price = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id=1 AND start_price>0")
        r_no_price = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id=1 AND (start_price IS NULL OR start_price<=0)")
        r_pending = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id=7")
        r_unprofitable = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id=3")
        r_gefiltert = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id=2")
        r_listed_status4 = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id=4")

        print(f"=== DB Status Uebersicht ===")
        print(f"status=1 (active) GESAMT:        {r_total_active}")
        print(f"  davon ebay_listed=TRUE:         {r_listed}")
        print(f"  davon mit Preis:                {r_with_price}")
        print(f"  davon OHNE Preis:               {r_no_price}")
        print(f"status=2 (gefiltert):             {r_gefiltert}")
        print(f"status=3 (unprofitable):          {r_unprofitable}")
        print(f"status=4 (listed/auf eBay):       {r_listed_status4}")
        print(f"status=7 (pending/re-scrape):     {r_pending}")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

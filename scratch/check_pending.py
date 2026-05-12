import asyncio
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from database import DatabaseManager

async def run():
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
    db_url = os.environ.get('DATABASE_URL')
    pool = await DatabaseManager.create_pool(db_url)
    async with pool.acquire() as conn:
        print("--- PENDING ITEMS SAMPLE ---")
        rows = await conn.fetch("SELECT id, linktobl, isbn, photo, status_id, start_price, ebay_status FROM library WHERE status_id = 7 LIMIT 5")
        for r in rows:
            print(dict(r))
            
        print("\n--- PENDING ITEMS WHERE ISBN IS NULL ---")
        count_null_isbn = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id = 7 AND isbn IS NULL")
        print("Count NULL ISBN:", count_null_isbn)
        
        count_has_isbn = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id = 7 AND isbn IS NOT NULL")
        print("Count HAS ISBN:", count_has_isbn)

    await pool.close()

if __name__ == "__main__":
    asyncio.run(run())

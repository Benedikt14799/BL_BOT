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
        print("--- SITETOSCRAPE ---")
        rows = await conn.fetch("SELECT id, is_scraped, anzahlSeiten, numbersOfBooks FROM sitetoscrape LIMIT 10")
        for r in rows:
            print(dict(r))
            
        print("\n--- LIBRARY STATS ---")
        pending_lib = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id = 7")
        print("status_id = 7 (pending in DB):", pending_lib)
        ebay_pending = await conn.fetchval("SELECT COUNT(*) FROM library WHERE ebay_status = 'pending'")
        print("ebay_status = 'pending' (ready for upload):", ebay_pending)
        
        all_wartend = await conn.fetchval("SELECT count(*) FROM sitetoscrape WHERE (is_scraped IS NULL OR is_scraped = FALSE)")
        print("\nSitetoscrape waiting:", all_wartend)
        
    await pool.close()

if __name__ == "__main__":
    asyncio.run(run())

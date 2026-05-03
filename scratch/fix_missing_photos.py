import asyncio
import os
from dotenv import load_dotenv
import asyncpg

async def fix():
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("DATABASE_URL nicht gefunden.")
        return
        
    pool = await asyncpg.create_pool(dsn=db_url, ssl='require')
    async with pool.acquire() as conn:
        res = await conn.execute("UPDATE library SET status_id = 2, ebay_error = 'missing_photo' WHERE status_id = 1 AND (photo IS NULL OR photo = '')")
        print(f"Bereinigung abgeschlossen: {res}")
    await pool.close()

if __name__ == '__main__':
    asyncio.run(fix())

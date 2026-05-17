import asyncio
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from database import DatabaseManager
from dotenv import load_dotenv

async def check_dnb_success():
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("Keine DATABASE_URL gefunden.")
        return

    pool = await DatabaseManager.create_pool(db_url)
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM library WHERE photo LIKE '%portal.dnb.de%'")
        print(f"Gesamtanzahl Artikel mit DNB-Cover in DB: {count}")
        
        if count > 0:
            rows = await conn.fetch("SELECT id, isbn, photo FROM library WHERE photo LIKE '%portal.dnb.de%' LIMIT 5")
            for r in rows:
                print(f"ID: {r['id']} | ISBN: {r['isbn']} | Foto: {r['photo'][:100]}...")
                
    await pool.close()

if __name__ == "__main__":
    asyncio.run(check_dnb_success())

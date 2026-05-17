
import asyncio
import os
from dotenv import load_dotenv
from database import DatabaseManager
from datetime import datetime

async def check_sync_progress():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    async with pool.acquire() as conn:
        # 1. Gesamtzahl gelisteter Artikel
        total_listed = await conn.fetchval("SELECT COUNT(*) FROM library WHERE ebay_listed = TRUE")
        
        # 2. Heute bereits geprüfte Artikel
        checked_today = await conn.fetchval(
            "SELECT COUNT(*) FROM library WHERE ebay_listed = TRUE AND last_checked::date = $1",
            datetime.now().date()
        )
        
        # 3. Ältestes last_checked Datum bei aktiven Listings
        oldest_check = await conn.fetchval(
            "SELECT MIN(last_checked) FROM library WHERE ebay_listed = TRUE"
        )
        
        print(f"--- SYNC CHECK ({today_str}) ---")
        print(f"Gelistete Artikel gesamt: {total_listed}")
        print(f"Heute bereits geprüft:    {checked_today}")
        if total_listed > 0:
            percent = (checked_today / total_listed) * 100
            print(f"Fortschritt heute:        {percent:.1f}%")
        print(f"Ältester Check (aktiv):   {oldest_check}")

    await pool.close()

if __name__ == "__main__":
    asyncio.run(check_sync_progress())

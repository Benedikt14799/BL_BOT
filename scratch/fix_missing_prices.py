"""
Fix 2: Alle Items mit status_id=1 (active) aber OHNE Preis oder Foto
werden auf status_id=7 (pending) zurückgesetzt, damit der Scraper 
sie beim nächsten Lauf neu verarbeitet.
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip("'\"")


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    
    async with pool.acquire() as conn:
        # Zuerst zählen wie viele betroffen sind
        count_no_price = await conn.fetchval("""
            SELECT COUNT(*) FROM library 
            WHERE status_id = 1 
              AND (start_price IS NULL OR start_price <= 0)
        """)
        count_no_photo = await conn.fetchval("""
            SELECT COUNT(*) FROM library 
            WHERE status_id = 1 
              AND (photo IS NULL OR photo = '')
        """)
        
        print(f"Items mit status=active aber KEIN Preis: {count_no_price}")
        print(f"Items mit status=active aber KEIN Foto:  {count_no_photo}")
        
        # Fix: Items ohne Preis auf 'pending' zurücksetzen (werden re-gescraped)
        result_price = await conn.execute("""
            UPDATE library 
            SET status_id = 7, 
                ebay_error = 'missing_price_reset'
            WHERE status_id = 1 
              AND (start_price IS NULL OR start_price <= 0)
        """)
        print(f"Preis-Cleanup: {result_price}")
        
        # Items ohne Foto auf 'gefiltert' setzen (kein Bild = nicht uploadbar)
        result_photo = await conn.execute("""
            UPDATE library 
            SET status_id = 2, 
                ebay_error = 'missing_photo'
            WHERE status_id = 1 
              AND (photo IS NULL OR photo = '')
        """)
        print(f"Foto-Cleanup: {result_photo}")
        
        # Abschlusskontrolle
        remaining = await conn.fetchval("""
            SELECT COUNT(*) FROM library 
            WHERE status_id = 1 
              AND (start_price IS NULL OR start_price <= 0 
                   OR photo IS NULL OR photo = '')
        """)
        print(f"\n✅ Verbleibende problematische 'active' Items: {remaining}")
        
        ready = await conn.fetchval("""
            SELECT COUNT(*) FROM library 
            WHERE status_id = 1
              AND start_price IS NOT NULL AND start_price > 0
              AND photo IS NOT NULL AND photo != ''
              AND (ebay_listed IS FALSE OR ebay_listed IS NULL)
        """)
        print(f"✅ Upload-ready Items (bereit für eBay): {ready}")
    
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

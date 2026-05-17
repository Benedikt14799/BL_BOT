# scratch/count_overnight.py
import asyncio
import os
from datetime import datetime, timedelta
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip("'\"")

async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        print("--- Datenbank-Analyse seit gestern Abend ---")
        
        # 1. Gesamtanzahl der Einträge pro Status
        statuses = await conn.fetch("SELECT id, label FROM library_statuses ORDER BY id")
        status_map = {row['id']: row['label'] for row in statuses}
        
        print("\nAktueller Stand in der Tabelle 'library' (Gesamtzahlen):")
        for s_id, label in status_map.items():
            count = await conn.fetchval(f"SELECT COUNT(*) FROM library WHERE status_id={s_id}")
            print(f"  Status {s_id} ({label:12}): {count}")
            
        # 2. Neue Artikel gestern/heute
        # Letzte 15 Stunden (seit ca. 18:30 Uhr gestern)
        cut_off = datetime.now() - timedelta(hours=15)
        print(f"\nAktivitäten seit {cut_off.strftime('%Y-%m-%d %H:%M:%S')}:")
        
        # Wieviele wurden neu angelegt (created_at)?
        created_count = await conn.fetchval("SELECT COUNT(*) FROM library WHERE created_at >= $1", cut_off)
        print(f"  Neu importierte Artikel: {created_count}")
        
        # Wieviele wurden verarbeitet (last_checked)?
        # (Da last_checked bei der Detailverarbeitung aktualisiert wird)
        checked_count = await conn.fetchval("SELECT COUNT(*) FROM library WHERE last_checked >= $1", cut_off)
        print(f"  In der Detailverarbeitung bearbeitete Artikel: {checked_count}")
        
        # Wie verteilen sich die seitdem bearbeiteten Artikel?
        by_status = await conn.fetch("""
            SELECT status_id, COUNT(*) as cnt 
            FROM library 
            WHERE last_checked >= $1 
            GROUP BY status_id
        """, cut_off)
        
        print("  Davon Ergebnisse der Bearbeitung:")
        for r in by_status:
            label = status_map.get(r['status_id'], f"Unbekannt ({r['status_id']})")
            print(f"    - {label:12}: {r['cnt']}")
            
        # 3. Wieviele wurden heute (seit Mitternacht) verarbeitet?
        today_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        checked_today = await conn.fetchval("SELECT COUNT(*) FROM library WHERE last_checked >= $1", today_midnight)
        print(f"\nHeute (seit Mitternacht) bearbeitete Artikel: {checked_today}")
        
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

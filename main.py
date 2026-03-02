# main.py
import asyncio
import logging
import time

import openpyxl
import asyncpg
import pandas as pd

from database import DatabaseManager
import scrape

# Logging konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    start_time = time.time()

    import os
    from dotenv import load_dotenv

    load_dotenv("supabase.env.txt")
    
    # Supabase liefert einen regulären Postgres-Connection-String.
    # Da Supabase unter anderem pgbouncer für direktes Polling über port 5432 / 6543 anbietet, 
    # generieren wir hier den Connection-String auf Basis des URL-Formats von supabase oder nutzen ihn direkt, falls als ENV hinterlegt.
    # Für asyncpg nutzen wir idealerweise den connection URI.
    
    # In der supabase.env.txt gibt es supabase_url und supabase_anon_key für die API. Für die Datenbank selbst braucht man:
    # 'postgresql://postgres.[ProjectRef]:[PASSWORD]@aws-0-[Region].pooler.supabase.com:6543/postgres'
    
    # Damit wir flexibel bleiben, erwarten wir hier einen DATABASE_URL Eintrag.
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("Keine DATABASE_URL in der .env-Datei gefunden! Bitte in supabase.env.txt hinzufügen.")
        return

    # 1) Verbindung zur Supabase (PostgreSQL) herstellen
    logger.info("Versuche Verbindung zu Supabase über den Session Pooler (IPv4) herzustellen...")
    
    # Supabase erfordert explizit ssl="require"
    db_pool = await asyncpg.create_pool(
        dsn=db_url,
        ssl="require"
    )

    # 2) Tabellen anlegen (falls noch nicht vorhanden)
    await DatabaseManager.create_table(db_pool)

    # 3) Links aus der Excel-Datei einlesen (Temporär für Test überschrieben)
    try:
        # Zum Testen mit spezifischen Links
        links_to_scrape = [
            "https://www.booklooker.de/B%C3%BCcher/Gef%C3%A4ngnis+Strafvollzug/us/1995",
            "https://www.booklooker.de/B%C3%BCcher/Kanzleif%C3%BChrung/us/866",
            "https://www.booklooker.de/B%C3%BCcher/Karibische+K%C3%BCche/us/2658"
        ]
        logger.info(f"{len(links_to_scrape)} Test-Links werden manuell verwendet.")
    except Exception as e:
        logger.error(f"Fehler: {e}")
        return

    # 4) Datenbank mit neuen Links füllen
    await scrape.insert_links_into_sitetoscrape(links_to_scrape, db_pool)

    # 5) Seiten scrapen und Detail‑Links speichern
    await scrape.scrape_and_save_pages(db_pool)

    # 6) Web‑Scraping Pipeline (prefill + Detailverarbeitung) starten
    #    Default-Category wird in scrape.perform_webscrape_async verwendet
    await scrape.perform_webscrape_async(db_pool)

    # 7) Pool schließen
    await db_pool.close()

    end_time = time.time()
    logger.info("Die Ausführungszeit beträgt: {:.2f} Sekunden".format(end_time - start_time))

if __name__ == "__main__":
    asyncio.run(main())

# main.py
import asyncio
import logging
import time
import os

import asyncpg
from dotenv import load_dotenv

from database import DatabaseManager
import scrape
import ebay_upload

# Logging konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    start_time = time.time()
    load_dotenv(".env")

    links_total = 0
    errors = 0
    db_pool = None

    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            logger.error("Keine DATABASE_URL in der .env-Datei gefunden! Bitte in .env hinzufügen.")
            errors = 1
            return

        logger.info("Versuche Verbindung zu Supabase über den Session Pooler (IPv4) herzustellen...")

        db_pool = await asyncpg.create_pool(
            dsn=db_url,
            ssl="require"
        )

        await DatabaseManager.create_table(db_pool)

        links_to_scrape = []
        links_file_path = "links.txt"
        try:
            if os.path.exists(links_file_path):
                with open(links_file_path, "r", encoding="utf-8") as file:
                    for line in file:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            links_to_scrape.append(line)
                logger.info(f"{len(links_to_scrape)} Zeilen (Links) aus '{links_file_path}' eingelesen.")
            else:
                logger.warning(f"Die Datei '{links_file_path}' wurde nicht gefunden. Es werden keine neuen Links hinzugefügt.")
        except Exception as e:
            logger.error(f"Fehler beim Einlesen von '{links_file_path}': {e}")
            errors = 1
            return

        links_total = len(links_to_scrape)

        if not links_to_scrape:
            logger.info("Es wurden keine neuen Links zum Scrapen übergeben (links.txt ist leer oder fehlt). Alte Einträge werden im nächsten Schritt verarbeitet.")

        await scrape.insert_links_into_sitetoscrape(links_to_scrape, db_pool)
        await scrape.scrape_and_save_pages(db_pool)
        results = await scrape.perform_webscrape_async(db_pool)
        
        items_saved = results.get("ok", 0)
        filtered = results.get("filtered", 0)
        errors = results.get("errors", 0)

        upload_to_ebay = os.environ.get("UPLOAD_TO_EBAY", "").lower() == "true"
        if upload_to_ebay:
            logger.info("UPLOAD_TO_EBAY ist aktiviert. Starte eBay Upload-Prozess...")
            await ebay_upload.run_upload_batch(db_pool)
        else:
            logger.info("UPLOAD_TO_EBAY ist nicht 'true'. eBay Upload wird übersprungen.")

    except Exception:
        logger.exception("Unbehandelter Fehler in main.py")
        errors += 1
        raise
    finally:
        if db_pool is not None:
            await db_pool.close()

        duration_s = int(round(time.time() - start_time))
        logger.info("Die Ausführungszeit beträgt: {:.2f} Sekunden".format(time.time() - start_time))

        print(f"SCRAPE_SUMMARY links_total={links_total} items_saved={items_saved} filtered={filtered} errors={errors} duration_s={duration_s}")


if __name__ == "__main__":
    asyncio.run(main())

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

    # 1) Verbindung zur Datenbank herstellen
    db_pool = await asyncpg.create_pool(
        host="localhost",
        database="postgres",
        user="postgres",
        password="1204",
        port=5432
    )

    # 2) Tabellen anlegen (falls noch nicht vorhanden)
    await DatabaseManager.create_table(db_pool)

    # 3) Links aus der Excel-Datei einlesen
    try:
        df_links = pd.read_excel('links.xlsx', header=None)
        links_to_scrape = df_links.iloc[:, 0].dropna().tolist()
        if not links_to_scrape:
            logger.error("Die Excel-Datei enthält keine Links in der ersten Spalte.")
            return
        logger.info(f"{len(links_to_scrape)} Links wurden aus der Excel-Datei eingelesen.")
    except Exception as e:
        logger.error(f"Fehler beim Einlesen der Excel-Datei: {e}")
        return

    # 4) Datenbank mit neuen Links füllen
    await scrape.insert_links_into_sitetoscrape(links_to_scrape, db_pool)

    await scrape.scrape_and_save_pages(db_pool)

    await scrape.perform_webscrape_async(db_pool)


    # 5) Pool schließen
    await db_pool.close()

    end_time = time.time()
    logger.info("Die Ausführungszeit beträgt: {:.2f} Sekunden".format(end_time - start_time))

if __name__ == "__main__":
    asyncio.run(main())

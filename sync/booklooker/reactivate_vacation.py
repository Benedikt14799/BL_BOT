import asyncio
import os
import sys
import logging
import aiohttp
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Projekt-Root in den Suchpfad legen
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database import DatabaseManager
from sync.booklooker.ebay import fetch_bl_html, is_sold
from price_processing import PriceProcessing
import ebay_upload

# Logging Setup
logger = logging.getLogger("Vacation-Reactivate")
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

async def main():
    logger.info("============================================================")
    logger.info("BookLooker Urlaubs-Reaktivierung gestartet")
    logger.info("============================================================")
    
    db_url = os.getenv("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    
    # 1. Alle pausierten Artikel finden, deren Urlaubs-Datum abgelaufen ist oder heute ist
    query = """
        SELECT id, sku, title, linktobl, vacation_until 
        FROM library 
        WHERE ebay_status = 'VACATION_PAUSED' 
          AND (vacation_until <= CURRENT_DATE OR vacation_until IS NULL)
    """
    
    async with pool.acquire() as conn:
        items = await conn.fetch(query)
    
    if not items:
        logger.info("Keine Artikel zur Reaktivierung gefunden.")
        await pool.close()
        return

    logger.info(f"{len(items)} potenzielle Artikel zur Reaktivierung gefunden.")
    
    reactivated_ids = []
    
    async with aiohttp.ClientSession() as session:
        for record in items:
            item = dict(record)
            sku = item["sku"]
            bl_url = item["linktobl"]
            
            logger.info(f"Prüfe [{sku}] {item['title']} ...")
            
            html = await fetch_bl_html(session, bl_url)
            if not html:
                continue
                
            soup = BeautifulSoup(html, "html.parser")
            ek = PriceProcessing._safe_clean_price(soup)
            
            status, info = is_sold(html, soup, ek)
            
            if status == "OK":
                logger.info(f"✅ [{sku}] ist wieder verfügbar! Markiere für Reaktivierung.")
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE library SET ebay_status = 'pending', vacation_until = NULL WHERE id = $1",
                        item["id"]
                    )
                reactivated_ids.append(item["id"])
            elif status == "VACATION":
                logger.info(f"⏳ [{sku}] weiterhin im Urlaub bis {info}.")
            else:
                logger.info(f"❌ [{sku}] scheint mittlerweile verkauft zu sein.")

    if reactivated_ids:
        logger.info(f"Artikel wurden auf 'pending' gesetzt und werden beim nächsten Upload-Lauf berücksichtigt.")
        
    await pool.close()
    logger.info("Reaktivierungs-Lauf beendet.")

if __name__ == "__main__":
    asyncio.run(main())

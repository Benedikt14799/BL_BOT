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

async def reactivate_vacation(pool):
    """Prüft pausierte Artikel und reaktiviert sie, wenn der Urlaub vorbei ist."""
    logger.info("BookLooker Urlaubs-Reaktivierung gestartet")
    
    # 1. Alle pausierten Artikel finden
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
        return {"found": 0, "reactivated": 0}

    logger.info(f"{len(items)} potenzielle Artikel zur Reaktivierung gefunden.")
    reactivated_count = 0
    
    async with aiohttp.ClientSession() as session:
        for record in items:
            item = dict(record)
            sku = item["sku"]
            bl_url = item["linktobl"]
            
            html = await fetch_bl_html(session, bl_url)
            if not html: continue
                
            soup = BeautifulSoup(html, "html.parser")
            ek = PriceProcessing._safe_clean_price(soup)
            status, info = is_sold(html, soup, ek)
            
            if status == "OK":
                logger.info(f"✅ [{sku}] ist wieder verfügbar!")
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE library SET ebay_status = 'pending', vacation_until = NULL WHERE id = $1",
                        item["id"]
                    )
                reactivated_count += 1
                
    return {"found": len(items), "reactivated": reactivated_count}

async def main():
    db_url = os.getenv("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    try:
        results = await reactivate_vacation(pool)
        logger.info(f"Reaktivierung beendet. {results['reactivated']} von {results['found']} reaktiviert.")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

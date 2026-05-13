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
                # Rentabilitäts-Check hinzufügen
                ship = PriceProcessing._safe_extract_shipping(soup)
                
                # Kostenparameter laden
                def to_dec(val, default):
                    if not val: return Decimal(default)
                    return Decimal(str(val).replace(",", "."))
                
                cost_params = {
                    "fixed_costs": to_dec(os.getenv("FIXKOSTEN_MONATLICH"), "79.95"),
                    "expected_sales": int(os.getenv("ERWARTETE_VERKAEUFE", "200")),
                    "steuer_satz": to_dec(os.getenv("STEUERSATZ"), "7.0"),
                    "addcost_low_mid": to_dec(os.getenv("ZUSATZKOSTEN_LOW_MID"), "0.50"),
                    "addcost_high": to_dec(os.getenv("ZUSATZKOSTEN_HIGH"), "1.75"),
                }
                
                target_ebay_price = PriceProcessing._compute_final_price(
                    ek, ship, cost_params["addcost_low_mid"], cost_params["addcost_high"], 
                    cost_params["steuer_satz"], cost_params["fixed_costs"], cost_params["expected_sales"]
                )
                
                is_rentabel = False
                if target_ebay_price:
                    prof = PriceProcessing.calculate_profitability(
                        ek, ship, target_ebay_price,
                        monthly_fixed_costs=cost_params["fixed_costs"], expected_sales=cost_params["expected_sales"],
                        addcost_low_mid=cost_params["addcost_low_mid"], addcost_high=cost_params["addcost_high"], steuer_satz=cost_params["steuer_satz"]
                    )
                    is_rentabel = prof.get("rentabel", False)
                
                if is_rentabel:
                    logger.info(f"✅ [{sku}] ist wieder verfügbar und rentabel!")
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """UPDATE library 
                               SET ebay_status = 'pending', start_price = $1, 
                                   purchase_price = $2, purchase_shipping = $3, 
                                   vacation_until = NULL, last_checked = NOW() 
                               WHERE id = $4""",
                            target_ebay_price, ek, ship, item["id"]
                        )
                    reactivated_count += 1
                else:
                    logger.warning(f"❌ [{sku}] wieder da, aber UNRENTABEL (EK: {ek}€). Markiere als aussortiert.")
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """UPDATE library 
                               SET ebay_status = 'delisted', ebay_delisted_reason = 'Nach Urlaub unrentabel',
                                   vacation_until = NULL, last_checked = NOW() 
                               WHERE id = $1""",
                            item["id"]
                        )
                
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

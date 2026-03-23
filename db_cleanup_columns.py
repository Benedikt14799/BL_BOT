import asyncio
import os
import logging
from database import DatabaseManager
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DBCleanup")

COLUMNS_TO_DROP = [
    "vat_percent", "buy_it_now_price", 
    "shipping_service_1_cost", "shipping_service_1_priority", 
    "shipping_service_2_cost", "shipping_service_2_priority", 
    "ebay_item_id", "ebay_listed_at", "ebay_last_sync", "next_recheck_date", 
    "returns_accepted_option", "returns_within_option", "refund_option", 
    "return_shipping_cost_paid_by", "productcompliancepolicyid", 
    "regional_productcompliancepolicies", "economicoperator_companyname", 
    "economicoperator_addressline1", "economicoperator_addressline2", 
    "economicoperator_city", "economicoperator_country", "economicoperator_postalcode", 
    "economicoperator_stateorprovince", "economicoperator_phone", "economicoperator_email", 
    "buchreihe", "genre", "originalsprache", "herstellungsland_und_region", 
    "literarische_gattung", "zielgruppe", "relationship", "relationshipdetails", 
    "epid", "literarische_bewegung", "videoid", "shipping_service_1_option", 
    "seitenanzahl", "shipping_service_2_option", "bl_condition", "max_dispatch_time"
]

async def cleanup():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL nicht gefunden.")
        return

    pool = await DatabaseManager.create_pool(db_url)
    
    async with pool.acquire() as conn:
        logger.info(f"Starte Löschen von {len(COLUMNS_TO_DROP)} Spalten aus Tabelle 'library'...")
        
        for col in COLUMNS_TO_DROP:
            try:
                await conn.execute(f"ALTER TABLE library DROP COLUMN IF EXISTS {col};")
                logger.info(f"Spalte gelöscht: {col}")
            except Exception as e:
                logger.error(f"Fehler beim Löschen von {col}: {e}")
                
    await pool.close()
    logger.info("Datenbank-Bereinigung abgeschlossen.")

if __name__ == "__main__":
    asyncio.run(cleanup())

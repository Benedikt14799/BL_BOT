import asyncio
import logging
import os
import sys
from decimal import Decimal
import aiohttp
from dotenv import load_dotenv
import asyncpg
from price_processing import PriceProcessing
from ebay_token_manager import get_token
from ebay_upload import withdraw_offer, update_inventory_price

# .env laden
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    logger.error("DATABASE_URL fehlt in der .env")
    sys.exit(1)

DRY_RUN = True

async def cleanup():
    global DRY_RUN
    if "--execute" in sys.argv:
        DRY_RUN = False
        logger.info("⚠️  AUSFÜHRUNGS-MODUS AKTIVIERT. Änderungen werden in DB vorgenommen.")
    else:
        logger.info("🔒 DRY-RUN MODUS. Keine tatsächlichen Änderungen.")

    pp = PriceProcessing()
    token = get_token()

    limit = 50
    for arg in sys.argv:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
    
    # Verbindung zur DB via asyncpg
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Alle Listings holen, die am längsten nicht geprüft wurden
        rows = await conn.fetch("""
            SELECT * FROM library 
            WHERE Start_price IS NOT NULL 
            ORDER BY last_competitor_check ASC NULLS FIRST 
            LIMIT $1;
        """, limit)
        logger.info(f"Prüfe {len(rows)} Listings (Limit: {limit})...")

        if not rows:
            return

        stats = {
            "total": len(rows),
            "keep": 0,
            "reprice": 0,
            "delist": 0,
            "no_median": 0,
            "errors": 0
        }

        reprice_list = []
        delist_list = []
        keep_list = []

        async with aiohttp.ClientSession() as session:
            for row in rows:
                try:
                    num = row["id"]
                    title = row.get("title") or row.get("Title") or "Unbekannt"
                    isbn = row.get("isbn") or row.get("ISBN")
                    current_p = Decimal(str(row.get("start_price") or row.get("Start_price") or "0"))
                    ek = Decimal(str(row.get("purchase_price") or row.get("Purchase_price") or "0"))
                    bl_shipping = Decimal(str(row.get("purchase_shipping") or row.get("Purchase_shipping") or "0"))
                    
                    if not isbn:
                        stats["no_median"] += 1
                        stats["keep"] += 1
                        keep_list.append(num)
                        continue

                    # 1. Aktuelle Wettbewerber-Daten abrufen
                    comp_data = await pp.get_competitor_prices(
                        session=session,
                        isbn=isbn,
                        token=token,
                        base_url="https://api.ebay.com",
                        condition=str(row.get("bl_condition") or "unbekannt")
                    )

                    median_p = comp_data.get("median_preis")
                    if not median_p or median_p <= 0:
                        stats["no_median"] += 1
                        stats["keep"] += 1
                        keep_list.append(num)
                        continue
                    
                    median_p = Decimal(str(median_p))

                    # 2. Prüfen ob Preis unrealistisch hoch (> 1.5 * Median)
                    if current_p <= median_p * Decimal('1.5'):
                        stats["keep"] += 1
                        keep_list.append(num)
                        continue

                    # 3. Preis ist unrealistisch. Prüfe ob Marktpreis profitabel ist.
                    target_margin = pp._target_margin_for_price(median_p)
                    prof = pp.calculate_profitability(
                        ek=ek,
                        bl_shipping=bl_shipping,
                        ebay_p=median_p,
                        min_margin=target_margin
                    )

                    if prof["rentabel"]:
                        new_p = pp._round_x99_up(median_p * Decimal('0.99'))
                        reprice_list.append({
                            "id": num,
                            "isbn": isbn,
                            "title": title,
                            "old_p": float(current_p),
                            "new_p": float(new_p),
                            "median": float(median_p)
                        })
                        stats["reprice"] += 1
                    else:
                        delist_list.append({
                            "id": num,
                            "isbn": isbn,
                            "title": title,
                            "current_p": float(current_p),
                            "median": float(median_p),
                            "marge_bei_median": prof["marge"]
                        })
                        stats["delist"] += 1

                except Exception as e:
                    logger.error(f"Fehler bei ID {row['id']}: {e}")
                    stats["errors"] += 1

            # Abschlussbericht Analyse
            logger.info("\n" + "="*40)
            logger.info("CLEANUP ANALYSE ERGEBNIS")
            logger.info("="*40)
            logger.info(f"Gesamt geprüft:   {stats['total']}")
            logger.info(f"Behalten:         {stats['keep']}")
            logger.info(f"Kein Median:      {stats['no_median']}")
            logger.info(f"Reprice geplant:  {stats['reprice']}")
            logger.info(f"Delist geplant:   {stats['delist']}")
            logger.info(f"Fehler:           {stats['errors']}")
            logger.info("="*40)

            if not DRY_RUN:
                logger.info("Führe Änderungen in DB und bei eBay durch...")
                
                # Timestamp für Behaltene aktualisieren, damit sie im nächsten Batch nicht wiederkommen
                if keep_list:
                    await conn.execute("UPDATE library SET last_competitor_check = NOW() WHERE id = ANY($1)", keep_list)
                    logger.info(f"{len(keep_list)} Artikel als 'beibehalten' markiert (Timestamp aktualisiert).")

                for item in reprice_list:
                    # 1. eBay Preis-Update
                    target_sku = item["isbn"]
                    if target_sku:
                        ebay_base = os.getenv("EBAY_BASE_URL", "https://api.ebay.com")
                        try:
                            success = await update_inventory_price(session, target_sku, item["new_p"], token, ebay_base)
                            if not success:
                                logger.error(f"ID {item['id']} (ISBN {target_sku}): eBay Reprice fehlgeschlagen.")
                        except Exception as e:
                            logger.error(f"ID {item['id']}: API Fehler beim Reprice: {e}")

                    # 2. DB Update (Inkl. Timestamp)
                    await conn.execute("""
                        UPDATE library 
                        SET Start_price = $1, last_competitor_check = NOW() 
                        WHERE id = $2
                    """, item["new_p"], item["id"])
                    logger.info(f"ID {item['id']}: Preis angepasst auf {item['new_p']}€ (ISBN {target_sku})")
                    
                for item in delist_list:
                    # 1. eBay Listing beenden
                    target_sku = item["isbn"]
                    
                    if target_sku:
                        ebay_base = os.getenv("EBAY_BASE_URL", "https://api.ebay.com")
                        try:
                            # Wir nutzen die ISBN als SKU, da dies die Konvention in ebay_upload.py ist
                            success = await withdraw_offer(session, target_sku, token, ebay_base)
                            if not success:
                                logger.error(f"ID {item['id']} (ISBN {target_sku}): Konnte eBay Angebot nicht beenden. Breche Verschieben ab.")
                                continue
                        except Exception as e:
                            if "404" in str(e):
                                logger.warning(f"ID {item['id']} (ISBN {target_sku}): Angebot nicht gefunden. Wird trotzdem verschoben.")
                            else:
                                logger.error(f"ID {item['id']}: API Fehler: {e}")
                                continue

                    # 2. Verschiebe in unprofitable_listings und lösche aus library
                    async with conn.transaction():
                        # Link aus library holen für den Transfer
                        link_row = await conn.fetchrow("SELECT LinkToBL FROM library WHERE id = $1", item["id"])
                        link = link_row["linktobl"] if link_row else "Link unbekannt"
                        
                        await conn.execute("""
                            INSERT INTO unprofitable_listings (library_id, link, reason, start_price, margin)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (library_id) DO UPDATE SET recorded_at = NOW();
                        """, item["id"], link, "unrealistic_price_vs_market", item["current_p"], item["marge_bei_median"])
                        
                        await conn.execute("DELETE FROM library WHERE id = $1", item["id"])
                        
                    logger.info(f"ID {item['id']}: Bei eBay beendet, in 'unprofitable_listings' verschoben und aus library entfernt.")
            else:
                if reprice_list:
                    logger.info("\nBEISPIELE FÜR REPRICE:")
                    for item in reprice_list[:5]:
                        logger.info(f" - {item['title'][:40]}... : {item['old_p']}€ -> {item['new_p']}€")
                
                if delist_list:
                    logger.info("\nBEISPIELE FÜR DELIST:")
                    for item in delist_list[:5]:
                        logger.info(f" - {item['title'][:40]}... : Preis {item['current_p']}€ vs. Median {item['median']}€")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(cleanup())

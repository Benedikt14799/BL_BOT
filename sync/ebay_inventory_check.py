import asyncio
import os
import sys
import logging
import aiohttp
import json
from datetime import datetime

# Projekt-Root in den Suchpfad legen, damit Imports funktionieren
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database import DatabaseManager
from ebay_token_manager import get_token

logger = logging.getLogger("eBay-Inventory-Sync")

async def fetch_active_ebay_items_trading(token: str):
    """
    Ruft alle aktiven Angebote über die Trading API (GetMyeBaySelling) ab.
    Das ist robuster als die Inventory API für existierende Bestände.
    """
    all_items = []
    page = 1
    url = "https://api.ebay.com/ws/api.dll"
    
    while True:
        headers = {
            "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
            "X-EBAY-API-SITEID": "77", # DE
            "X-EBAY-API-COMPATIBILITY-LEVEL": "1191",
            "X-EBAY-API-IAF-TOKEN": f"Bearer {token}",
            "Content-Type": "text/xml"
        }
        
        xml_payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
          <ActiveList>
            <Include>true</Include>
            <Pagination>
              <EntriesPerPage>200</EntriesPerPage>
              <PageNumber>{page}</PageNumber>
            </Pagination>
          </ActiveList>
        </GetMyeBaySellingRequest>"""
        
        logger.info(f"Rufe eBay ActiveList ab (Trading API), Seite {page}...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=xml_payload) as resp:
                    if resp.status != 200:
                        logger.error(f"Fehler bei GetMyeBaySelling ({resp.status})")
                        break
                    
                    text = await resp.text()
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(text)
                    ns = {"ebay": "urn:ebay:apis:eBLBaseComponents"}
                    
                    batch = root.findall(".//ebay:ActiveList/ebay:ItemArray/ebay:Item", ns)
                    if not batch:
                        break
                        
                    for item in batch:
                        item_id = item.find("ebay:ItemID", ns).text
                        sku = item.find("ebay:SKU", ns)
                        all_items.append({
                            "listingId": item_id,
                            "sku": sku.text if sku is not None else None
                        })
                    
                    total_pages_tag = root.find(".//ebay:ActiveList/ebay:PaginationResult/ebay:TotalNumberOfPages", ns)
                    total_pages = int(total_pages_tag.text) if total_pages_tag is not None else 1
                    
                    if page >= total_pages:
                        break
                    page += 1
        except Exception as e:
            logger.error(f"Fehler beim Abruf der Trading API: {e}")
            break
            
    return all_items

async def run_inventory_sync(db_pool):
    """
    Hauptfunktion für den Bestandsabgleich: 
    eBay Bestand (Trading API) <-> Lokale Datenbank (library)
    """
    logger.info("=" * 60)
    logger.info("Starte eBay Bestandsabgleich (Trading API Check)")
    logger.info("=" * 60)

    token = get_token()
    
    # 1. Alle aktiven Angebote von eBay holen (Trading API)
    ebay_items = await fetch_active_ebay_items_trading(token)
    logger.info(f"Gefundene aktive Angebote auf eBay: {len(ebay_items)}")
    
    # 2. Alle gelisteten Artikel aus der DB holen
    async with db_pool.acquire() as conn:
        query = """
            SELECT id, sku, title, linktobl, ebay_listing_id
            FROM library
            WHERE ebay_listed = TRUE
        """
        db_items = await conn.fetch(query)
    
    logger.info(f"Gelistete Artikel in der lokalen Datenbank: {len(db_items)}")

    # --- SAFETY CHECK ---
    if len(ebay_items) == 0 and len(db_items) > 10:
        logger.error("CRITICAL: eBay hat 0 aktive Angebote zurückgegeben, aber die Datenbank enthält viele gelistete Artikel!")
        logger.error("Abbruch des Synchronisationslaufs zur Vermeidung massenhafter Löschungen.")
        return {
            "total_checked": 0, "removed": 0, "orphans": 0, 
            "error": "Safety shutdown: 0 eBay items found."
        }

    # Mapping für schnellen Zugriff
    ebay_id_map = {str(item["listingId"]): item for item in ebay_items if item["listingId"]}
    ebay_sku_map = {str(item["sku"]): item for item in ebay_items if item["sku"]}

    # 3. Abgleich: DB -> eBay (Was fehlt auf eBay?)
    processed_count = 0
    removed_count = 0
    matched_ebay_listing_ids = set() # Tracke, welche eBay-Items wir in der DB gefunden haben
    
    for item in db_items:
        db_id = item["id"]
        db_sku = item["sku"]
        db_listing_id = item["ebay_listing_id"]
        db_link = item["linktobl"]
        db_title = item["title"]

        # Prüfe ob Listing ID oder SKU auf eBay existiert
        item_on_ebay = None
        if db_listing_id and str(db_listing_id) in ebay_id_map:
            item_on_ebay = ebay_id_map[str(db_listing_id)]
        elif db_sku and str(db_sku) in ebay_sku_map:
            item_on_ebay = ebay_sku_map[str(db_sku)]
        
        if item_on_ebay:
            matched_ebay_listing_ids.add(str(item_on_ebay["listingId"]))
        else:
            logger.warning(f"Listing fehlt auf eBay! DB-ID: {db_id} | SKU: {db_sku} | Titel: {db_title}")
            
            # Grund markieren
            reason = "Listing_not_found_on_ebay_sync"
            
            try:
                await DatabaseManager.record_missing_listing(db_pool, db_id, db_link or "", reason)
                removed_count += 1
            except Exception as e:
                logger.error(f"Fehler beim Archivieren von ID {db_id}: {e}")
        
        processed_count += 1
        if processed_count % 50 == 0:
            logger.info(f"Fortschritt: {processed_count}/{len(db_items)} geprüft.")

    # 4. Abgleich: eBay -> DB (Was ist auf eBay, aber nicht in DB?)
    orphans = []
    for item in ebay_items:
        l_id = str(item["listingId"])
        if l_id not in matched_ebay_listing_ids:
            orphans.append(item.get("sku") or l_id)
    
    if orphans:
        logger.warning(f"Gefundene 'Orphans' auf eBay (aktiv, aber nicht als gelistet in DB markiert): {len(orphans)}")
        for o_sku in orphans[:10]: # Max 10 loggen
            logger.info(f"Orphan SKU: {o_sku}")
        if len(orphans) > 10:
            logger.info("...")

    return {
        "total_checked": len(db_items),
        "removed": removed_count,
        "orphans": len(orphans)
    }

if __name__ == "__main__":
    # Test-Lauf mit Logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    async def test():
        from dotenv import load_dotenv
        load_dotenv()
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            logging.error("DATABASE_URL nicht in .env gefunden!")
            return
        
        pool = await DatabaseManager.create_pool(db_url)
        try:
            await run_inventory_sync(pool)
        finally:
            await pool.close()
    
    asyncio.run(test())

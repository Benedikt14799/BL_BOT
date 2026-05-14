import asyncio
import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal
import aiohttp
from dotenv import load_dotenv
from database import DatabaseManager
from ebay_token_manager import get_token

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("eBayNegotiation")

class eBayNegotiation:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.base_url = os.getenv("EBAY_BASE_URL", "https://api.ebay.com")

    async def get_token(self):
        # Nutzt den Singleton Token Manager
        return get_token()

    async def find_eligible_items(self):
        """
        Findet Listings, die für Preisvorschläge berechtigt sind, 
        und reichert sie mit Datenbank-Infos (Marge) an.
        """
        token = await self.get_token()
        if not token:
            logger.error("Kein eBay Token verfügbar.")
            return []

        # Limit auf 100 setzen
        url = f"{self.base_url}/sell/negotiation/v1/find_eligible_items?limit=100"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_DE"
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        items = data.get("eligibleItems", [])
                        logger.info(f"eBay API hat {len(items)} berechtigte Artikel gemeldet.")
                        return await self._enrich_with_db_data(items)
                    else:
                        err_text = await resp.text()
                        logger.error(f"Fehler bei find_eligible_items: {resp.status} - {err_text}")
                        return []
            except Exception as e:
                logger.error(f"Exception in find_eligible_items: {e}")
                return []

    async def _enrich_with_db_data(self, items):
        """Verknüpft eBay-Items mit der lokalen DB und berechnet die Restmarge."""
        enriched = []
        async with self.db_pool.acquire() as conn:
            for item in items:
                lid = item.get("listingId")
                # Preis robust extrahieren
                ebay_price_data = item.get("price") or item.get("listingPrice") or {}
                val = Decimal(str(ebay_price_data.get("value", "0")))
                curr = ebay_price_data.get("currency", "EUR")

                # DB Lookup
                row = await conn.fetchrow("""
                    SELECT id, sku, purchase_price, purchase_shipping, margin, start_price 
                    FROM library 
                    WHERE ebay_listing_id = $1 OR ebay_item_id::text = $1
                """, lid)

                if row:
                    item["db_data"] = dict(row)
                    item["display_price"] = {"value": str(val), "currency": curr}
                    enriched.append(item)
                else:
                    # Optional: Auch Items ohne DB-Eintrag anzeigen, aber als "unbekannt" markieren
                    item["display_price"] = {"value": str(val), "currency": curr}
                    enriched.append(item)
        return enriched

    async def send_offers_to_watchers(self, profit_share_percent=10):
        """
        Sendet Angebote basierend auf einem Anteil des tatsächlichen Gewinns.
        Beispiel: Gib 10% deines Gewinns als Rabatt weiter.
        """
        eligible_items = await self.find_eligible_items()
        if not eligible_items:
            logger.info("Keine Artikel für Preisvorschläge gefunden.")
            return 0

        token = await self.get_token()
        url = f"{self.base_url}/sell/negotiation/v1/send_offer_to_interested_buyers"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_DE"
        }

        count = 0
        async with aiohttp.ClientSession() as session:
            for item in eligible_items:
                listing_id = item.get("listingId")
                db = item.get("db_data")
                
                if not db or not db.get("gewinn_real"):
                    logger.warning(f"Überspringe Listing {listing_id}: Kein gewinn_real in DB.")
                    continue

                # 1. Daten holen
                current_p = Decimal(str(item["display_price"]["value"]))
                gewinn_real = Decimal(str(db["gewinn_real"]))
                if current_p <= 0 or gewinn_real <= 0: continue
                
                # 2. Wie viel Rabatt (in €) entspricht X% des Gewinns?
                rabatt_euro = gewinn_real * (Decimal(str(profit_share_percent)) / Decimal("100"))
                
                # 3. Welchem Prozentsatz vom Verkaufspreis entspricht das?
                # Formel: (Rabatt_Euro / Verkaufspreis) * 100
                discount_percent_raw = (rabatt_euro / current_p) * Decimal("100")
                discount_percent = int(discount_percent_raw) # Abrunden auf ganze Zahl
                
                # 4. eBay Mindestrabatt-Prüfung (eBay verlangt mind. 5%)
                if discount_percent < 5:
                    logger.info(f"Überspringe {listing_id}: {profit_share_percent}% vom Gewinn ({rabatt_euro:.2f}€) sind nur {discount_percent}% Rabatt. (eBay verlangt mind. 5%)")
                    continue

                # 5. Angebot senden
                payload = {
                    "offers": [
                        {
                            "allowCounterOffer": True,
                            "message": f"Vielen Dank für Ihr Interesse! Hier ist ein exklusives Angebot für Sie.",
                            "offerDuration": {"value": 2, "unit": "DAY"},
                            "offeredItems": [
                                {
                                    "listingId": listing_id,
                                    "discountPercentage": str(discount_percent)
                                }
                            ]
                        }
                    ]
                }

                try:
                    async with session.post(url, headers=headers, json=payload) as resp:
                        if resp.status in [200, 201, 204, 207]:
                            logger.info(f"Angebot für {listing_id} gesendet. Rabatt: {discount_percent}% ({rabatt_euro:.2f}€ vom Gewinn)")
                            count += 1
                        else:
                            err_text = await resp.text()
                            logger.error(f"Fehler bei {listing_id}: {resp.status} - {err_text}")
                except Exception as e:
                    logger.error(f"Exception bei {listing_id}: {e}")

        return count

        return count

        return count

async def test_negotiation():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    
    neg = eBayNegotiation(pool)
    items = await neg.find_eligible_items()
    
    print(f"\n--- EBAY WATCHERS CHECK ---")
    print(f"Berechtigte Artikel für Angebote: {len(items)}")
    for i in items:
        print(f"- Listing: {i.get('listingId')} | Preis: {i.get('price', {}).get('value')} {i.get('price', {}).get('currency')}")
    
    await pool.close()

if __name__ == "__main__":
    asyncio.run(test_negotiation())

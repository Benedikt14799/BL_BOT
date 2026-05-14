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
                # Preis robust extrahieren (eBay nutzt oft 'listingPrice' in dieser API)
                ebay_price_data = item.get("listingPrice") or item.get("price") or {}
                val = Decimal(str(ebay_price_data.get("value", "0")))
                curr = ebay_price_data.get("currency", "EUR")

                # DB Lookup
                row = await conn.fetchrow("""
                    SELECT id, sku, title, purchase_price, purchase_shipping, margin, start_price, gewinn_real
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

    async def send_offers_to_watchers(self, profit_share_percent=35):
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

                # 1. Daten holen (Wir nehmen den NIEDRIGEREN Preis, um Fehler zu vermeiden)
                db_price = Decimal(str(db.get("start_price") or 0))
                
                # Wir schauen, ob eBay uns einen aktuellen Preis liefert
                ebay_price_data = item.get("listingPrice") or item.get("price") or {}
                api_price = Decimal(str(ebay_price_data.get("value", "0")))
                
                if api_price > 0 and db_price > 0:
                    current_p = min(db_price, api_price)
                else:
                    current_p = db_price if db_price > 0 else api_price
                
                gewinn_real = Decimal(str(db.get("gewinn_real") or 0))
                if current_p <= 0 or gewinn_real <= 0:
                    logger.warning(f"Überspringe {listing_id}: Kein Preis ({current_p}) oder Gewinn ({gewinn_real}) vorhanden.")
                    continue
                
                # 2. Wie viel Rabatt (in €) entspricht X% des Gewinns?
                rabatt_euro = gewinn_real * (Decimal(str(profit_share_percent)) / Decimal("100"))
                new_price = current_p - rabatt_euro
                
                # 3. Welchem Prozentsatz vom Verkaufspreis entspricht das?
                # Formel: (Rabatt_Euro / Verkaufspreis) * 100
                discount_percent_raw = (rabatt_euro / current_p) * Decimal("100")
                
                import math
                # Wichtig: Aufrunden auf die nächste ganze Zahl, um eBay's 5% Hürde sicher zu nehmen
                discount_percent = math.ceil(float(discount_percent_raw))
                
                # 4. eBay Mindestrabatt-Prüfung (eBay verlangt mind. 5%)
                if discount_percent < 5:
                    logger.info(f"Überspringe {listing_id}: {profit_share_percent}% vom Gewinn ({rabatt_euro:.2f}€) sind nur {discount_percent}% Rabatt. (eBay verlangt mind. 5%)")
                    continue

                # 5. Angebot senden (Direktes Objekt, keine "offers"-Liste!)
                payload = {
                    "allowCounterOffer": False,
                    "message": "Vielen Dank für Ihr Interesse! Hier ist ein exklusives Angebot für Sie.",
                    "offerDuration": {"value": 2, "unit": "DAY"},
                    "offeredItems": [
                        {
                            "listingId": listing_id,
                            "quantity": 1,
                            "price": {
                                "value": f"{new_price:.2f}",
                                "currency": "EUR"
                            }
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

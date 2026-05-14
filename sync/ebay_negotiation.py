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
        """Findet Listings, die für Preisvorschläge an Käufer berechtigt sind."""
        token = await self.get_token()
        if not token:
            logger.error("Kein eBay Token verfügbar.")
            return []

        url = f"{self.base_url}/sell/negotiation/v1/find_eligible_items"
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
                        return data.get("eligibleItems", [])
                    else:
                        err_text = await resp.text()
                        logger.error(f"Fehler bei find_eligible_items: {resp.status} - {err_text}")
                        return []
            except Exception as e:
                logger.error(f"Exception in find_eligible_items: {e}")
                return []

    async def send_offers_to_watchers(self, discount_percent=5):
        """Sendet Angebote an Beobachter für alle berechtigten Artikel."""
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
                # Hier könnten wir noch die Marge prüfen, wenn wir die SKU hätten
                # Aber für den ersten Test senden wir einfach einen fixen Rabatt
                
                payload = {
                    "offers": [
                        {
                            "allowCounterOffer": True,
                            "message": "Vielen Dank für Ihr Interesse! Hier ist ein exklusives Angebot für Sie.",
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
                            logger.info(f"Angebot für Listing {listing_id} gesendet.")
                            count += 1
                        else:
                            err_text = await resp.text()
                            logger.error(f"Fehler beim Senden für {listing_id}: {resp.status} - {err_text}")
                except Exception as e:
                    logger.error(f"Exception beim Senden für {listing_id}: {e}")

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

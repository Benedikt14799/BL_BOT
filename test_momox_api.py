import aiohttp
import asyncio
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MomoxTest")

class MomoxClient:
    def __init__(self):
        # Bekannte Header für die Momox API (Stand 2024)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "X-API-TOKEN": "22492f5677d6110f0a93068e2834390ad07d4c29", # Öffentlicher Web-Token
            "X-MARKETPLACE-ID": "momox_de",
            "Accept": "application/json",
        }
        self.base_url = "https://api.momox.de/api/v4/offer/"

    async def get_price(self, session: aiohttp.ClientSession, isbn: str):
        # ISBN bereinigen (nur Ziffern)
        clean_isbn = re.sub(r"[^\d]", "", isbn)
        url = f"{self.base_url}?ean={clean_isbn}"
        
        try:
            async with session.get(url, headers=self.headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Momox gibt Preise oft in Cent oder als String zurück
                    price = data.get("purchase_price", 0.0)
                    title = data.get("product", {}).get("title", "Unbekannt")
                    return {"success": True, "price": float(price), "title": title}
                elif resp.status == 404:
                    return {"success": False, "reason": "Nicht im Ankauf"}
                else:
                    text = await resp.text()
                    return {"success": False, "reason": f"API Fehler {resp.status}: {text[:100]}"}
        except Exception as e:
            return {"success": False, "reason": f"Verbindungsfehler: {str(e)}"}

async def main():
    # Beispiel ISBN (kannst du anpassen)
    test_isbn = "9783492026123" 
    
    async with aiohttp.ClientSession() as session:
        client = MomoxClient()
        logger.info(f"Prüfe Momox Preis für ISBN: {test_isbn}...")
        result = await client.get_price(session, test_isbn)
        
        if result["success"]:
            logger.info(f"✅ Erfolg! Titel: {result['title']}")
            logger.info(f"💰 Momox zahlt: {result['price']} €")
        else:
            logger.warning(f"❌ Fehlgeschlagen: {result['reason']}")

if __name__ == "__main__":
    asyncio.run(main())

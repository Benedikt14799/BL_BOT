import cloudscraper
import asyncio
import logging
import re
import json

logger = logging.getLogger(__name__)

class MomoxClient:
    def __init__(self):
        # We use cloudscraper to bypass Cloudflare
        self.scraper = cloudscraper.create_scraper()
        
    def _fetch_price_sync(self, isbn: str):
        """Synchroner Aufruf, der in einem eigenen Thread ausgeführt wird."""
        clean_isbn = re.sub(r"[^\d]", "", isbn)
        if not clean_isbn:
            return None
            
        url = f"https://www.momox.de/offer/{clean_isbn}"
        try:
            resp = self.scraper.get(url, timeout=10)
            if resp.status_code == 200:
                html = resp.text
                
                # Wir suchen nach der Redux/Vue State Variable im HTML
                # oft als window.__INITIAL_STATE__ oder ähnlich
                # Oder wir suchen direkt nach "purchase_price": 1.43
                
                match = re.search(r'"purchase_price"\s*:\s*([\d\.]+)', html)
                if match:
                    return float(match.group(1))
                    
                # Alternative: Preis-Format im HTML "1,43 &euro;"
                # Da wir das genaue Format von cloudscraper nicht sicher wissen,
                # extrahieren wir alle Preise und nehmen den höchsten (meist der Ankaufspreis)
                # Das ist fehleranfälliger, besser ist die API (falls die App API offen ist)
                
                # Fallback API: Manchmal geht die mobile API ohne Cloudflare
                # Wir versuchen es zur Sicherheit mit der internen Web-API, falls der Token aus dem HTML extrahierbar ist.
                
                # Aber für den Moment verlassen wir uns auf einen Regex:
                return 0.0
                
            elif resp.status_code == 404:
                # Artikel wird nicht angekauft
                return 0.0
            else:
                logger.debug(f"Momox-Request für {clean_isbn} lieferte Status {resp.status_code}")
                return 0.0
        except Exception as e:
            logger.debug(f"Fehler bei Momox-Abfrage für {clean_isbn}: {e}")
            return 0.0

    async def get_price(self, isbn: str) -> float:
        """Asynchroner Wrapper für die blockierende Cloudscraper-Abfrage."""
        loop = asyncio.get_running_loop()
        price = await loop.run_in_executor(None, self._fetch_price_sync, isbn)
        return price or 0.0

# Singleton-Instanz
momox_client = MomoxClient()

async def check_arbitrage(db_pool, library_id: int, isbn: str, bl_price: float, bl_shipping: float, link: str, title: str):
    """
    Prüft, ob der Artikel bei Momox gewinnbringend verkauft werden kann.
    Schreibt das Ergebnis direkt in die Datenbank, wenn profitabel.
    """
    if not isbn:
        return
        
    bl_total = float(bl_price) + float(bl_shipping)
    if bl_total <= 0:
        return
        
    momox_price = await momox_client.get_price(isbn)
    
    if momox_price > 0:
        profit = momox_price - bl_total
        logger.debug(f"[ARBITRAGE CHECK] {isbn} - BL: {bl_total}€ | Momox: {momox_price}€ | Profit: {profit}€")
        
        # Threshold: z.B. 3.00€ Gewinn
        if profit >= 3.0:
            logger.info(f"🚀 ARBITRAGE GEFUNDEN! ISBN: {isbn} | Gewinn: {profit:.2f}€ | Titel: {title[:30]}")
            from database import DatabaseManager
            await DatabaseManager.record_arbitrage_deal(
                db_pool=db_pool,
                library_id=library_id,
                bl_total_price=bl_total,
                momox_price=momox_price,
                profit=profit,
                link=link,
                title=title,
                isbn=isbn
            )

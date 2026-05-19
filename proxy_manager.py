
import logging
import os
import asyncio
from datetime import date
from decimal import Decimal
import aiohttp

logger = logging.getLogger(__name__)

class ProxyManager:
    """
    Verwaltet Proxy-Verbindungen, Kosten-Tracking und Sicherheits-Stopps.
    """
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.use_proxies = os.getenv("USE_PROXIES", "False").lower() == "true"
        self.proxy_url = os.getenv("PROXY_URL", "")
        self.price_per_gb = Decimal(os.getenv("PROXY_PRICE_PER_GB", "1.50"))
        self.daily_budget = Decimal(os.getenv("PROXY_DAILY_BUDGET_EUR", "5.00"))
        self.kill_switch = os.getenv("PROXY_KILL_SWITCH", "True").lower() == "true"
        
        self._lock = asyncio.Lock()
        self._budget_exceeded = False

    async def get_current_usage(self):
        """Holt den aktuellen Tagesverbrauch aus der Datenbank."""
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT bytes_transferred, cost_eur, request_count FROM proxy_usage WHERE usage_date = CURRENT_DATE"
            )
            if not row:
                return {"bytes": 0, "cost": Decimal("0.00"), "requests": 0}
            return {
                "bytes": row["bytes_transferred"],
                "cost": row["cost_eur"],
                "requests": row["request_count"]
            }

    async def track_request(self, bytes_count: int):
        """Aktualisiert den Verbrauch und prüft das Budget."""
        async with self._lock:
            cost = (Decimal(bytes_count) / Decimal(1024**3)) * self.price_per_gb
            
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO proxy_usage (usage_date, bytes_transferred, cost_eur, request_count)
                    VALUES (CURRENT_DATE, $1, $2, 1)
                    ON CONFLICT (usage_date) DO UPDATE SET
                        bytes_transferred = proxy_usage.bytes_transferred + $1,
                        cost_eur = proxy_usage.cost_eur + $2,
                        request_count = proxy_usage.request_count + 1
                """, bytes_count, cost)
                
                # Budget-Check
                total_cost = await conn.fetchval("SELECT cost_eur FROM proxy_usage WHERE usage_date = CURRENT_DATE")
                if total_cost and total_cost >= self.daily_budget:
                    if not self._budget_exceeded:
                        logger.critical(f"🛑 BUDGETLIMIT ERREICHT: {total_cost}€ / {self.daily_budget}€. Proxy wird deaktiviert!")
                        self._budget_exceeded = True
                    return False
            return True

    def should_use_proxy(self, url: str) -> bool:
        """Entscheidet, ob für eine URL der Proxy genutzt werden muss (Hybrid-Routing)."""
        if not self.use_proxies or self._budget_exceeded:
            return False
            
        # Nur Booklooker HTML-Seiten gehen über den Proxy
        if "booklooker.de" in url:
            # Bilder-Subdomain explizit ausschließen (spart massiv Traffic)
            if "images.booklooker.de" in url:
                return False
            return True
            
        return False

    async def get_proxy_args(self, url: str) -> dict:
        """Gibt die Proxy-Argumente für aiohttp zurück (mit automatischer IP-Rotation)."""
        if self.should_use_proxy(url):
            if not self.proxy_url:
                if self.kill_switch:
                    raise Exception("PROXY_REQUIRED_BUT_NOT_CONFIGURED")
                return {}
            
            # Automatische IP-Rotation für Data Impulse & gängige Anbieter via Session-ID
            import random
            proxy_str = self.proxy_url
            if "@" in proxy_str:
                try:
                    parts = proxy_str.split("@")
                    auth_part = parts[0] # z.B. "http://username:password"
                    host_part = parts[1] # z.B. "gw.dataimpulse.com:823"
                    
                    auth_subparts = auth_part.split(":")
                    schema = auth_subparts[0] # "http"
                    username = auth_subparts[1].replace("//", "") # "b48f90a42fd8aa1c321b__cr.de"
                    password = auth_subparts[2] # "05840031d7e8fcd2"
                    
                    # Generiere eine einzigartige Session-ID für diesen Request, um eine neue IP zu erzwingen
                    rand_session = random.randint(1000000, 9999999)
                    new_username = f"{username}__session-{rand_session}"
                    
                    rotated_proxy_url = f"{schema}://{new_username}:{password}@{host_part}"
                    return {"proxy": rotated_proxy_url}
                except Exception as e:
                    logger.warning(f"Fehler bei dynamischer Proxy-Rotation: {e}")
                    
            return {"proxy": self.proxy_url}
        return {}

    def is_budget_ok(self) -> bool:
        return not self._budget_exceeded

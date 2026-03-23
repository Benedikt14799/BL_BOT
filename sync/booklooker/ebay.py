"""
BookLooker ↔ eBay Bestandsabgleich
===================================
Eigenständiges Skript für Cron-Jobs.
Prüft alle auf eBay gelisteten Bücher gegen BookLooker und reagiert:
  - Nicht mehr verfügbar → eBay Angebot beenden
  - Preis/Versand geändert → eBay Preis aktualisieren
  - Unrentabel geworden → eBay Angebot beenden

Ausführung:  python -m sync.booklooker.ebay
"""

import asyncio
import os
import sys
import logging
import random
from decimal import Decimal
from datetime import datetime

import aiohttp
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Projekt-Root in den Suchpfad legen, damit Imports funktionieren
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database import DatabaseManager
from price_processing import PriceProcessing
from ebay_token_manager import get_token
import ebay_upload

# ─── Konfiguration ────────────────────────────────────────────────
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

LOG_FILE = os.path.join(PROJECT_ROOT, "sync_inventory.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("BL-eBay-Sync")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

# Delay zwischen Requests (Sekunden) – schont BookLooker
BASE_DELAY = 8.0
JITTER = 0.3  # ±30 %


async def fetch_bl_html(session: aiohttp.ClientSession, url: str) -> str:
    """Holt HTML von BookLooker mit randomisiertem User-Agent."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        async with session.get(url, headers=headers, timeout=15) as resp:
            if resp.status == 200:
                return await resp.text()
            if resp.status == 404:
                logger.info(f"BL 404 (Not Found): {url}")
                return "404_NOT_FOUND"
            if resp.status == 410:
                logger.info(f"BL 410 (Gone): {url}")
                return "410_GONE"
            if resp.status == 429:
                logger.warning("Rate-Limit (429) von BookLooker! Warte 60 s …")
                await asyncio.sleep(60)
                return ""
            logger.error(f"BL HTTP {resp.status} für {url}")
            return ""
    except Exception as e:
        logger.error(f"Fehler beim Abruf von {url}: {e}")
        return ""


def is_sold(html: str, soup: BeautifulSoup, ek: Decimal) -> tuple[str, str | None]:
    """
    Prüft den Status auf BookLooker.
    Gibt (Status, Zusatzinfo) zurück.
    Status: "OK", "SOLD", "VACATION"
    """
    if html in ["404_NOT_FOUND", "410_GONE"] or ek == 0:
        return "SOLD", None
    
    if "Dieses Angebot ist nicht mehr verfügbar" in html or "Artikeldaten nicht gefunden" in html:
        return "SOLD", None

    # Urlaubsmodus erkennen: "Der Anbieter ist bis einschließlich 05.04.2026 nicht erreichbar."
    # Wir suchen im Text nach dem Muster für Datum
    vacation_match = re.search(r"bis einschließlich\s+(\d{2}\.\d{2}\.\d{4})", html)
    if vacation_match:
        return "VACATION", vacation_match.group(1)

    # Case-insensitive Suche nach dem Warenkorb-Button
    if soup:
        cart_button = soup.find("input", value=lambda v: v and "warenkorb" in v.lower())
        if cart_button:
            return "OK", None

    # Falls kein Button da ist ODER keine Soup (404/410) UND kein Urlaub erkannt wurde -> Sold
    return "SOLD", None


# ─── Kern-Logik: Ein einzelnes Item verarbeiten ───────────────────

async def process_item(
    item: dict,
    db_pool,
    session: aiohttp.ClientSession,
    token: str,
    base_url: str,
    cost_params: dict,
) -> dict:
    """
    Prüft ein einzelnes Item gegen BookLooker.
    Gibt ein Status-Dict zurück: {"action": "...", "id": ...}
    """
    internal_id = item["id"]
    bl_url = item.get("linktobl") or item.get("link")
    current_ebay_price = Decimal(str(item.get("start_price") or 0))
    sku = item.get("sku")
    title = item.get("title", "Unknown")

    if not bl_url or not sku:
        logger.warning(f"[{internal_id}] Keine URL oder SKU. Überspringe.")
        return {"action": "skipped", "id": internal_id}

    html = await fetch_bl_html(session, bl_url)
    if not html:
        # Netzwerkfehler – nicht handeln, beim nächsten Lauf erneut prüfen
        await _update_last_checked(db_pool, internal_id)
        return {"action": "network_error", "id": internal_id}

    # Falls BookLooker einen Fehlercode (404/410) geliefert hat, ist html kein echtes HTML mehr
    if html in ["404_NOT_FOUND", "410_GONE"]:
        new_ek = Decimal("0.00")
        new_shipping = Decimal("0.00")
        soup = None
    else:
        soup = BeautifulSoup(html, "html.parser")
        new_ek = PriceProcessing._safe_clean_price(soup)
        new_shipping = PriceProcessing._safe_extract_shipping(soup)
    
    stored_ek = Decimal(str(item.get("purchase_price") or 0))
    stored_shipping = Decimal(str(item.get("purchase_shipping") or 0))
    listing_id = item.get("ebay_listing_id")

    # ── 1. Verfügbarkeit prüfen ──
    status, info = is_sold(html, soup, new_ek)
    
    if status != "OK":
        # Bei "Vielleicht-Verkauf" (oder Urlaub) machen wir eine 3-phasige Verifizierung
        logger.info(f"[{sku}] Buch scheint nicht verfügbar ({status}). Starte Verifizierung …")
        confirmed = True
        for attempt in range(2):
            await asyncio.sleep(3)  # Kurze Pause für Server-Stabilität
            retry_html = await fetch_bl_html(session, bl_url)
            retry_soup = BeautifulSoup(retry_html, "html.parser")
            retry_ek = PriceProcessing._safe_clean_price(retry_soup)
            r_status, r_info = is_sold(retry_html, retry_soup, retry_ek)
            if r_status == "OK":
                logger.info(f"[{sku}] Verifizierung {attempt+1}/2: Fehlalarm!")
                confirmed = False
                break
            logger.info(f"[{sku}] Verifizierung {attempt+1}/2: Status weiterhin {r_status}.")

        if confirmed:
            if status == "VACATION":
                logger.info(f"[{sku}] Anbieter im Urlaub bis {info}! Pausiere eBay Angebot …")
                # Beenden auf eBay (Sicherheit vor Fehlverkäufen)
                if listing_id:
                    success = await ebay_upload.end_item_by_id(session, listing_id, token)
                else:
                    success = await ebay_upload.withdraw_offer(session, sku, token, base_url)
                
                if success:
                    async with db_pool.acquire() as conn:
                        try:
                            # Datum umwandeln (DD.MM.YYYY -> YYYY-MM-DD)
                            db_date = datetime.strptime(info, "%d.%m.%Y").date()
                            await conn.execute(
                                """UPDATE library 
                                   SET ebay_listed = FALSE, ebay_status = 'VACATION_PAUSED', vacation_until = $1, last_checked = NOW()
                                   WHERE id = $2""",
                                db_date, internal_id
                            )
                        except Exception as e:
                            logger.error(f"Fehler beim Speichern des Urlaubsdatums: {e}")
                    return {"action": "vacation_paused", "id": internal_id}
            
            else: # status == "SOLD"
                logger.info(f"[{sku}] Definitiv verkauft auf BL! Beende eBay Angebot …")
                
                # Bevorzugt über listing_id (Trading API), sonst über SKU (Inventory API)
                if listing_id:
                    success = await ebay_upload.end_item_by_id(session, listing_id, token)
                else:
                    success = await ebay_upload.withdraw_offer(session, sku, token, base_url)
                    
                if success:
                    await _mark_sold_on_bl(db_pool, internal_id)
                    await DatabaseManager.record_sold_listing(
                        db_pool, internal_id, bl_url, sku, title, "sold_on_bl"
                    )
                return {"action": "sold", "id": internal_id}

    # ── 2. Preis- / Versandänderung prüfen ──
    # NEU (1): Initial-Lauf abfangen
    if item.get("purchase_price") is None or Decimal(str(item.get("purchase_price") or 0)) == 0:
        logger.info(f"[{sku}] Initialisiere EK/Versand in DB ({new_ek}€ / {new_shipping}€) ohne eBay-Update.")
        async with db_pool.acquire() as conn:
            await conn.execute(
                """UPDATE library
                   SET purchase_price = $1, purchase_shipping = $2, last_checked = NOW()
                   WHERE id = $3""",
                new_ek, new_shipping, internal_id
            )
        return {"action": "db_initialized", "id": internal_id}

    # NEU (2): Nur kalkulieren und updaten, wenn sich auf BookLooker wirklich etwas geändert hat!
    if new_ek == stored_ek and new_shipping == stored_shipping:
        logger.info(f"[{sku}] Unverändert (BL-EK: {new_ek}€, Versand: {new_shipping}€). Überspringe.")
        await _update_last_checked(db_pool, internal_id)
        return {"action": "unchanged", "id": internal_id}

    target_ebay_price = PriceProcessing._compute_final_price(
        new_ek,
        new_shipping,
        cost_params["addcost_low_mid"],
        cost_params["addcost_high"],
        cost_params["steuer_satz"],
        cost_params["fixed_costs"],
        cost_params["expected_sales"],
    )

    if target_ebay_price is None:
        await _update_last_checked(db_pool, internal_id)
        return {"action": "calc_error", "id": internal_id}

    price_diff = abs(Decimal(str(target_ebay_price)) - current_ebay_price)
    if price_diff > Decimal("0.01"):
        logger.info(
            f"[{sku}] Preisänderung auf BL: EK {new_ek}€ (alt {stored_ek}€), Versand {new_shipping}€ (alt {stored_shipping}€) "
            f"→ neuer eBay-Zielpreis {target_ebay_price}€ (bisher {current_ebay_price}€)"
        )

        # Rentabilitäts-Check
        prof = PriceProcessing.recheck_profitability(
            ek=new_ek,
            bl_shipping=new_shipping,
            current_ebay_price=target_ebay_price,
            monthly_fixed_costs=cost_params["fixed_costs"],
            expected_sales=cost_params["expected_sales"],
            addcost_low_mid=cost_params["addcost_low_mid"],
            addcost_high=cost_params["addcost_high"],
            steuer_satz=cost_params["steuer_satz"],
        )

        if not prof["rentabel"]:
            logger.warning(f"[{sku}] Nach Preiserhöhung unrentabel! Beende Angebot …")
            if listing_id:
                await ebay_upload.end_item_by_id(session, listing_id, token)
            else:
                await ebay_upload.withdraw_offer(session, sku, token, base_url)
            
            await _mark_unprofitable(db_pool, internal_id)
            await DatabaseManager.record_sold_listing(
                db_pool, internal_id, bl_url, sku, title, "unprofitable_after_sync"
            )
            return {"action": "unprofitable", "id": internal_id}

        # Preis auf eBay aktualisieren (Bevorzugt über listing_id / Trading API)
        if listing_id:
            success = await ebay_upload.revise_item_price_by_id(session, listing_id, float(target_ebay_price), token)
        else:
            success = await ebay_upload.update_inventory_price(session, sku, float(target_ebay_price), token, base_url)
            
        if success:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """UPDATE library
                       SET start_price = $1, margin = $2,
                           purchase_price = $3, purchase_shipping = $4,
                           last_checked = NOW()
                       WHERE id = $5""",
                    target_ebay_price,
                    prof["marge"],
                    new_ek,
                    new_shipping,
                    internal_id,
                )
            return {"action": "price_updated", "id": internal_id}
        else:
            logger.error(f"[{sku}] eBay-Update fehlgeschlagen. (ID: {listing_id or 'Keine'})")
            return {"action": "ebay_error", "id": internal_id}

    # Keine Änderung – nur Zeitstempel aktualisieren
    await _update_last_checked(db_pool, internal_id)
    return {"action": "unchanged", "id": internal_id}


# ─── DB-Hilfsfunktionen ──────────────────────────────────────────

async def _update_last_checked(db_pool, internal_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE library SET last_checked = NOW() WHERE id = $1", internal_id
        )


async def _mark_sold_on_bl(db_pool, internal_id: int):
    """Setzt ebay_delisted_reason und ebay_status vor dem Archivieren."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            """UPDATE library
               SET ebay_delisted_reason = 'Auf BookLooker verkauft',
                   ebay_status = 'sold',
                   ebay_listed = FALSE,
                   last_checked = NOW()
               WHERE id = $1""",
            internal_id,
        )


async def _mark_unprofitable(db_pool, internal_id: int):
    """Setzt ebay_delisted_reason und ebay_status bei Unrentabilität."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            """UPDATE library
               SET ebay_delisted_reason = 'Nach Sync unrentabel',
                   ebay_status = 'delisted',
                   ebay_listed = FALSE,
                   last_checked = NOW()
               WHERE id = $1""",
            internal_id,
        )


# ─── Hauptlauf ────────────────────────────────────────────────────

async def main():
    """Einmal-Durchlauf: Alle eBay-gelisteten Artikel prüfen."""
    logger.info("=" * 60)
    logger.info("BookLooker ↔ eBay Bestandsabgleich gestartet")
    logger.info("=" * 60)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL fehlt! Abbruch.")
        return

    pool = await DatabaseManager.create_pool(db_url)

    # Tabellen-Migration ausführen (stellt sicher, dass last_checked existiert)
    await DatabaseManager.create_table(pool)

    # Kostenparameter laden
    def to_dec(val, default):
        if not val:
            return Decimal(default)
        return Decimal(str(val).replace(",", "."))

    cost_params = {
        "fixed_costs": to_dec(os.getenv("FIXKOSTEN_MONATLICH"), "79.95"),
        "expected_sales": int(os.getenv("ERWARTETE_VERKAEUFE", "200")),
        "steuer_satz": to_dec(os.getenv("STEUERSATZ"), "7.0"),
        "addcost_low_mid": to_dec(os.getenv("ZUSATZKOSTEN_LOW_MID"), "0.50"),
        "addcost_high": to_dec(os.getenv("ZUSATZKOSTEN_HIGH"), "1.75"),
    }
    logger.info(f"Kostenparameter: {cost_params}")

    EBAY_BASE_URL = os.getenv("EBAY_BASE_URL", "https://api.ebay.com")
    token = None

    async with aiohttp.ClientSession() as session:
        # Alle gelisteten Artikel laden
        async with pool.acquire() as conn:
            query = """
                SELECT id, sku, title, start_price, linktobl, ebay_listing_id,
                       purchase_price, purchase_shipping
                FROM library
                WHERE ebay_listed = TRUE
                ORDER BY last_checked ASC NULLS FIRST
            """
            items = await conn.fetch(query)

        total = len(items)
        logger.info(f"📦 {total} gelistete Artikel gefunden.")

        if total == 0:
            logger.info("Keine Artikel zu prüfen. Beende.")
            await pool.close()
            return

        # Statistiken
        stats = {
            "unchanged": 0,
            "price_updated": 0,
            "sold": 0,
            "unprofitable": 0,
            "skipped": 0,
            "network_error": 0,
            "calc_error": 0,
            "ebay_error": 0,
            "vacation_paused": 0,
            "db_initialized": 0,
        }

        for idx, record in enumerate(items):
            # Token auffrischen, falls er während des langen Laufs abgelaufen ist (>2h)
            token = get_token()

            result = await process_item(
                dict(record), pool, session, token, EBAY_BASE_URL, cost_params
            )
            action = result.get("action", "unknown")
            stats[action] = stats.get(action, 0) + 1

            # Fortschritt loggen
            if (idx + 1) % 25 == 0 or (idx + 1) == total:
                logger.info(f"Fortschritt: {idx+1}/{total}")

            # Anti-Blocking Delay mit Jitter
            jitter_val = random.uniform(-JITTER, JITTER) * BASE_DELAY
            delay = max(5.0, BASE_DELAY + jitter_val)
            await asyncio.sleep(delay)

    await pool.close()

    # Zusammenfassung
    logger.info("=" * 60)
    logger.info("📊 ZUSAMMENFASSUNG")
    logger.info(f"  Geprüft:           {total}")
    logger.info(f"  Unverändert:       {stats['unchanged']}")
    logger.info(f"  Preis aktualisiert:{stats['price_updated']}")
    logger.info(f"  Verkauft (BL):     {stats['sold']}")
    logger.info(f"  Urlaub (Pausiert): {stats['vacation_paused']}")
    logger.info(f"  Unrentabel:        {stats['unprofitable']}")
    logger.info(f"  eBay-Fehler (404): {stats.get('ebay_error', 0)}")
    logger.info(f"  Übersprungen:      {stats['skipped']}")
    logger.info(f"  Netzwerkfehler:    {stats['network_error']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

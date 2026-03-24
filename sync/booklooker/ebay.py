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
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=True)

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
    Status: "OK", "SOLD", "VACATION", "UNKNOWN"
    """
    if html in ["404_NOT_FOUND", "410_GONE"] or ek == 0:
        return "SOLD", None
    
    # Explizite Verkauft-Indikatoren
    if "Dieses Angebot ist nicht mehr verfügbar" in html or "Artikeldaten nicht gefunden" in html:
        return "SOLD", None

    # Urlaubsmodus erkennen
    vacation_match = re.search(r"bis einschließlich\s+(\d{2}\.\d{2}\.\d{4})", html)
    if vacation_match:
        return "VACATION", vacation_match.group(1)

    # Verfügbarkeits-Indikator: Warenkorb-Button
    if soup:
        cart_button = soup.find("input", value=lambda v: v and "warenkorb" in v.lower())
        if cart_button:
            return "OK", None

    # Block-Seiten erkennen (Sicherheit vor Fehl-Löschungen)
    if "zugriff verweigert" in html.lower() or "bot-schutz" in html.lower() or "captcha" in html.lower():
        logger.warning("BookLooker Block-Seite oder Captcha erkannt! Status UNKNOWN.")
        return "UNKNOWN", None

    # Fallback: Wenn kein Button da ist, aber auch kein klarer SOLD-Indikator -> Sicher ist sicher -> UNKNOWN
    # So verhindern wir, dass bei einem Layout-Wechsel oder temporärem Block alles gelöscht wird.
    logger.warning("Weder Warenkorb noch Sold-Indikator gefunden. Status vorsichtshalber UNKNOWN.")
    return "UNKNOWN", None

# ─── Backup-Hilfsfunktionen ───────────────────────────────────────

async def validate_backups(item: dict, session: aiohttp.ClientSession, cost_params: dict, db_pool, worker_id: str) -> bool:
    """Prüft die Backups eines Privat-Angebots. Löscht ungültige aus der DB. Gibt True zurück, wenn noch mind. 1 Backup valid ist."""
    b1_url = item.get("backup1_url")
    b2_url = item.get("backup2_url")
    
    valid_b1 = False
    valid_b2 = False
    
    update_b1 = False
    update_b2 = False
    new_b1_price = item.get("backup1_price")
    new_b1_shipping = item.get("backup1_shipping")
    new_b2_price = item.get("backup2_price")
    new_b2_shipping = item.get("backup2_shipping")
    
    internal_id = item["id"]

    for b_type, b_url, b_is_priv, is_b1 in [
        ("B1", b1_url, item.get("backup1_is_private", True), True),
        ("B2", b2_url, item.get("backup2_is_private", False), False)
    ]:
        if not b_url: continue
        
        html = await fetch_bl_html(session, b_url)
        if not html:
            # Netzwerk-Fehler ignorieren
            if is_b1: valid_b1 = True
            else: valid_b2 = True
            continue
            
        if html in ["404_NOT_FOUND", "410_GONE"]:
            logger.info(f"{worker_id} [Backup {b_type}] {b_url} nicht mehr gefunden (404/410).")
            if is_b1: update_b1 = True; new_b1_price = None; b1_url = None
            else: update_b2 = True; new_b2_price = None; b2_url = None
            continue
            
        soup = BeautifulSoup(html, "html.parser")
        ek = PriceProcessing._safe_clean_price(soup)
        ship = PriceProcessing._safe_extract_shipping(soup)
        status, _ = is_sold(html, soup, ek)
        
        if status != "OK":
            logger.info(f"{worker_id} [Backup {b_type}] Status ist {status}. Entferne aus Backups.")
            if is_b1: update_b1 = True; new_b1_price = None; b1_url = None
            else: update_b2 = True; new_b2_price = None; b2_url = None
            continue
            
        # Margin Check
        new_ebay_p = PriceProcessing._compute_final_price(
            ek, ship, cost_params["addcost_low_mid"], cost_params["addcost_high"], 
            cost_params["steuer_satz"], cost_params["fixed_costs"], cost_params["expected_sales"]
        )
        if not new_ebay_p:
            if is_b1: update_b1 = True; new_b1_price = None; b1_url = None
            else: update_b2 = True; new_b2_price = None; b2_url = None
            continue
            
        prof = PriceProcessing.calculate_profitability(
            ek, ship, new_ebay_p,
            monthly_fixed_costs=cost_params["fixed_costs"], expected_sales=cost_params["expected_sales"],
            min_margin=Decimal("2.50"), # Fiktiv, da compute_final_price die Zielmarge selbst intern aufschlägt
            addcost_low_mid=cost_params["addcost_low_mid"], addcost_high=cost_params["addcost_high"], steuer_satz=cost_params["steuer_satz"]
        )
        
        is_valid = prof["rentabel"] if b_is_priv else prof["marge"] >= 0
        
        if is_valid:
            if is_b1: valid_b1 = True
            else: valid_b2 = True
            
            # Preis-Update?
            old_price = item.get("backup1_price") if is_b1 else item.get("backup2_price")
            old_ship = item.get("backup1_shipping") if is_b1 else item.get("backup2_shipping")
            if float(ek) != float(old_price or 0) or float(ship) != float(old_ship or 0):
                if is_b1: update_b1 = True; new_b1_price = ek; new_b1_shipping = ship
                else: update_b2 = True; new_b2_price = ek; new_b2_shipping = ship
        else:
            logger.info(f"{worker_id} [Backup {b_type}] Nicht mehr rentabel (EK: {ek}€). Entferne.")
            if is_b1: update_b1 = True; b1_url = None; new_b1_price = None
            else: update_b2 = True; b2_url = None; new_b2_price = None

    if update_b1 or update_b2:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE library 
                SET backup1_url = $1, backup1_price = $2, backup1_shipping = $3,
                    backup2_url = $4, backup2_price = $5, backup2_shipping = $6
                WHERE id = $7
            """, b1_url, new_b1_price, new_b1_shipping, b2_url, new_b2_price, new_b2_shipping, internal_id)
            
    # Das Dict aktualisieren für Rotation
    item["backup1_url"] = b1_url
    item["backup1_price"] = new_b1_price
    item["backup1_shipping"] = new_b1_shipping
    item["backup2_url"] = b2_url
    item["backup2_price"] = new_b2_price
    item["backup2_shipping"] = new_b2_shipping
            
    return valid_b1 or valid_b2

async def try_fallback_rotation(item, session, cost_params, db_pool, token, base_url, worker_id) -> bool:
    """Schaltet auf B1 oder B2 um und aktualisiert die DB. Gibt True zurück, wenn Rotation erfolgreich."""
    new_url = None
    new_ek = None
    new_shipping = None
    new_is_private = False
    b_type = ""
    shift_sql = ""
    
    if item.get("backup1_url"):
        new_url = item["backup1_url"]
        new_ek = Decimal(str(item["backup1_price"] or 0))
        new_shipping = Decimal(str(item["backup1_shipping"] or 0))
        new_is_private = item.get("backup1_is_private", True)
        b_type = "B1"
        shift_sql = """
            UPDATE library
            SET linktobl = $1, purchase_price = $2, purchase_shipping = $3,
                is_private = $4, start_price = $5, last_checked = NOW(),
                backup1_url = backup2_url, backup1_price = backup2_price, 
                backup1_shipping = backup2_shipping, backup1_is_private = backup2_is_private,
                backup2_url = NULL, backup2_price = NULL, backup2_shipping = NULL, backup2_is_private = FALSE
            WHERE id = $6
        """
    elif item.get("backup2_url"):
        new_url = item["backup2_url"]
        new_ek = Decimal(str(item["backup2_price"] or 0))
        new_shipping = Decimal(str(item["backup2_shipping"] or 0))
        new_is_private = item.get("backup2_is_private", False)
        b_type = "B2"
        shift_sql = """
            UPDATE library
            SET linktobl = $1, purchase_price = $2, purchase_shipping = $3,
                is_private = $4, start_price = $5, last_checked = NOW(),
                backup1_url = NULL, backup2_url = NULL
            WHERE id = $6
        """
        
    if not new_url:
        return False
        
    # Neues Target Price kalkulieren
    target_ebay_price = PriceProcessing._compute_final_price(
        new_ek, new_shipping, cost_params["addcost_low_mid"], cost_params["addcost_high"], 
        cost_params["steuer_satz"], cost_params["fixed_costs"], cost_params["expected_sales"]
    )
    if not target_ebay_price: return False
    
    listing_id = item.get("ebay_listing_id")
    sku = item["sku"]
    
    if listing_id:
        success = await ebay_upload.revise_item_price_by_id(session, listing_id, float(target_ebay_price), token)
    else:
        success = await ebay_upload.update_inventory_price(session, sku, float(target_ebay_price), token, base_url)
        
    if success:
        logger.info(f"{worker_id} [FALLBACK {b_type}] Erfolgreiche Rotation zu {new_url} (Neuer eBay-Preis: {target_ebay_price}€)")
        async with db_pool.acquire() as conn:
            await conn.execute(shift_sql, new_url, new_ek, new_shipping, new_is_private, target_ebay_price, item["id"])
        return True
    else:
        logger.error(f"{worker_id} [FALLBACK {b_type}] eBay-Preisupdate fehlgeschlagen für Rotation!")
        return False

# ─── Kern-Logik: Ein einzelnes Item verarbeiten ───────────────────

async def process_item(
    item: dict,
    db_pool,
    session: aiohttp.ClientSession,
    token: str,
    base_url: str,
    cost_params: dict,
    worker_id: str = "[W-?]"
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
    listing_id = item.get("ebay_listing_id")
    stored_ek = Decimal(str(item.get("purchase_price") or 0))
    stored_shipping = Decimal(str(item.get("purchase_shipping") or 0))

    if not bl_url or not sku:
        logger.warning(f"{worker_id} [{internal_id}] Keine URL oder SKU. Überspringe.")
        return {"action": "skipped", "id": internal_id}
        
    is_private = item.get("is_private", False)
    
    # ── 0. Backup Validierung für Privat-Angebote ──
    if is_private:
        has_valid_backup = await validate_backups(item, session, cost_params, db_pool, worker_id)
        if not has_valid_backup:
            logger.info(f"{worker_id} [{sku}] Privat-Angebot OHNE valides Backup! Beende eBay Angebot …")
            if listing_id:
                success = await ebay_upload.end_item_by_id(session, listing_id, token)
            else:
                success = await ebay_upload.withdraw_offer(session, sku, token, base_url)
                
            if success:
                await _mark_sold_on_bl(db_pool, internal_id) # Archivieren
                await DatabaseManager.record_sold_listing(db_pool, internal_id, bl_url, sku, title, "no_valid_backup")
            return {"action": "no_backup_ended", "id": internal_id}

    html = await fetch_bl_html(session, bl_url)
    if not html:
        # Netzwerkfehler – nicht handeln, beim nächsten Lauf erneut prüfen
        await _update_last_checked(db_pool, internal_id)
        return {"action": "network_error", "id": internal_id}

    # Falls BookLooker einen Fehlercode (404/410) geliefert hat
    if html in ["404_NOT_FOUND", "410_GONE"]:
        new_ek = Decimal("0.00")
        new_shipping = Decimal("0.00")
        soup = None
    else:
        soup = BeautifulSoup(html, "html.parser")
        new_ek = PriceProcessing._safe_clean_price(soup)
        new_shipping = PriceProcessing._safe_extract_shipping(soup)

    # ── 1. Verfügbarkeit prüfen ──
    status, info = is_sold(html, soup, new_ek)
    
    if status != "OK":
        # Bei "Vielleicht-Verkauf" (oder Urlaub) machen wir eine 3-phasige Verifizierung
        logger.info(f"{worker_id} [{sku}] Buch scheint nicht verfügbar ({status}). URL: {bl_url} | Starte Verifizierung …")
        confirmed = True
        r_status, r_info = status, info
        
        for attempt in range(2):
            await asyncio.sleep(3)  # Kurze Pause für Server-Stabilität
            retry_html = await fetch_bl_html(session, bl_url)
            
            # Bei 410/404 während Retry: Preis-Parsing überspringen für saubere Logs
            if retry_html in ["404_NOT_FOUND", "410_GONE"]:
                retry_soup = None
                retry_ek = Decimal("0.00")
            else:
                retry_soup = BeautifulSoup(retry_html, "html.parser")
                retry_ek = PriceProcessing._safe_clean_price(retry_soup)
                
            r_status, r_info = is_sold(retry_html, retry_soup, retry_ek)
            if r_status == "OK":
                logger.info(f"{worker_id} [{sku}] Verifizierung {attempt+1}/2: Fehlalarm!")
                confirmed = False
                break
            logger.info(f"{worker_id} [{sku}] Verifizierung {attempt+1}/2: Status weiterhin {r_status}.")

        if confirmed:
            if r_status == "VACATION":
                logger.info(f"{worker_id} [{sku}] Anbieter im Urlaub bis {r_info}! URL: {bl_url} | Pausiere eBay Angebot …")
                # Beenden auf eBay (Sicherheit vor Fehlverkäufen)
                if listing_id:
                    success = await ebay_upload.end_item_by_id(session, listing_id, token)
                else:
                    success = await ebay_upload.withdraw_offer(session, sku, token, base_url)
                
                if success:
                    async with db_pool.acquire() as conn:
                        try:
                            # Datum umwandeln (DD.MM.YYYY -> YYYY-MM-DD)
                            db_date = datetime.strptime(r_info, "%d.%m.%Y").date()
                            await conn.execute(
                                """UPDATE library 
                                   SET ebay_listed = FALSE, ebay_status = 'VACATION_PAUSED', vacation_until = $1, last_checked = NOW()
                                   WHERE id = $2""",
                                db_date, internal_id
                            )
                        except Exception as e:
                            logger.error(f"{worker_id} Fehler beim Speichern des Urlaubsdatums: {e}")
                    return {"action": "vacation_paused", "id": internal_id}
            
            elif r_status == "SOLD":
                # Rotation versuchen!
                if item.get("is_private") and (item.get("backup1_url") or item.get("backup2_url")):
                    logger.info(f"{worker_id} [{sku}] Definitiv verkauft auf BL! Versuche Fallback-Rotation...")
                    rotated = await try_fallback_rotation(item, session, cost_params, db_pool, token, base_url, worker_id)
                    if rotated:
                        return {"action": "fallback_rotated", "id": internal_id}

                logger.info(f"{worker_id} [{sku}] Definitiv verkauft auf BL! URL: {bl_url} | Beende eBay Angebot …")
                
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
                
            else:
                # r_status ist "UNKNOWN" (z.B. Blockade) -> Nichts tun, Sicherheit geht vor!
                logger.warning(f"{worker_id} [{sku}] Status unklar ({r_status}). URL: {bl_url} | Überspringe Beenden zur Sicherheit.")
                await _update_last_checked(db_pool, internal_id)
                return {"action": "skipped", "id": internal_id}

    # ── 2. Preis- / Versandänderung prüfen ──
    # NEU (1): Initial-Lauf abfangen
    if item.get("purchase_price") is None or Decimal(str(item.get("purchase_price") or 0)) == 0:
        logger.info(f"{worker_id} [{sku}] Initialisiere EK/Versand in DB ({new_ek}€ / {new_shipping}€) ohne eBay-Update.")
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
        logger.info(f"{worker_id} [{sku}] Unverändert (BL-EK: {new_ek}€, Versand: {new_shipping}€). Überspringe.")
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
            f"{worker_id} [{sku}] Preisänderung auf BL: EK {new_ek}€ (alt {stored_ek}€), Versand {new_shipping}€ (alt {stored_shipping}€) "
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
            logger.warning(f"{worker_id} [{sku}] Nach Preiserhöhung unrentabel! Beende Angebot …")
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
            logger.error(f"{worker_id} [{sku}] eBay-Update fehlgeschlagen. (ID: {listing_id or 'Keine'})")
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
# ─── Kostenparameter Helfer ───────────────────────────────────────
def _get_cost_params() -> dict:
    """Lädt Kostenparameter aus Umgebungsvariablen."""
    def to_dec(val, default):
        if not val:
            return Decimal(default)
        return Decimal(str(val).replace(",", "."))

    return {
        "fixed_costs": to_dec(os.getenv("FIXKOSTEN_MONATLICH"), "79.95"),
        "expected_sales": int(os.getenv("ERWARTETE_VERKAEUFE", "200")),
        "steuer_satz": to_dec(os.getenv("STEUERSATZ"), "7.0"),
        "addcost_low_mid": to_dec(os.getenv("ZUSATZKOSTEN_LOW_MID"), "0.50"),
        "addcost_high": to_dec(os.getenv("ZUSATZKOSTEN_HIGH"), "1.75"),
    }


# ─── Hauptlauf ────────────────────────────────────────────────────

async def worker(
    queue: asyncio.Queue,
    db_pool,
    session: aiohttp.ClientSession,
    base_url: str,
    cost_params: dict,
    stats: dict,
    total_count: int,
    processed_count: list,
    worker_id: str
):
    """Holt Items aus der Queue und verarbeitet sie."""
    while True:
        record = await queue.get()
        try:
            # Token bei jedem Item auffrischen (EbayTokenManager sorgt für Effizienz)
            token = get_token()
            
            result = await process_item(
                dict(record), db_pool, session, token, base_url, cost_params, worker_id
            )
            action = result.get("action", "unknown")
            
            # Statistiken sicher aktualisieren
            stats[action] = stats.get(action, 0) + 1
            processed_count[0] += 1
            
            # Fortschritt alle 25 Artikel loggen
            if processed_count[0] % 25 == 0 or processed_count[0] == total_count:
                logger.info(f"Fortschritt: {processed_count[0]}/{total_count}")

            # Anti-Blocking Delay pro Worker mit Jitter
            jitter_val = random.uniform(-JITTER, JITTER) * BASE_DELAY
            delay = max(5.0, BASE_DELAY + jitter_val)
            await asyncio.sleep(delay)
            
        except Exception as e:
            logger.error(f"Worker Fehler bei Verarbeitung von {record.get('sku')}: {e}")
        finally:
            queue.task_done()


async def main():
    """Einmal-Durchlauf: Alle eBay-gelisteten Artikel prüfen."""
    logger.info("=" * 60)
    logger.info("BookLooker ↔ eBay Bestandsabgleich gestartet (PARALLEL)")
    logger.info("=" * 60)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL fehlt! Abbruch.")
        return

    pool = await DatabaseManager.create_pool(db_url)
    if not pool:
        logger.error("Datenbank-Pool konnte nicht erstellt werden.")
        return

    # Tabellen-Migration ausführen (stellt sicher, dass last_checked existiert)
    await DatabaseManager.create_table(pool)

    # Kostenparameter laden
    cost_params = _get_cost_params()
    logger.info(f"Kostenparameter: {cost_params}")

    EBAY_BASE_URL = os.getenv("EBAY_BASE_URL", "https://api.ebay.com")
    MAX_WORKERS = int(os.getenv("MAX_SYNC_WORKERS", "5"))
    logger.info(f"Parallelisierung: {MAX_WORKERS} Worker.")

    async with aiohttp.ClientSession() as session:
        # Alle gelisteten Artikel laden
        async with pool.acquire() as conn:
            query = """
                SELECT id, sku, title, start_price, linktobl, ebay_listing_id,
                       purchase_price, purchase_shipping, is_private,
                       backup1_url, backup1_price, backup1_shipping, backup1_is_private,
                       backup2_url, backup2_price, backup2_shipping, backup2_is_private
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
            "unchanged": 0, "price_updated": 0, "sold": 0, "unprofitable": 0,
            "skipped": 0, "network_error": 0, "calc_error": 0, "ebay_error": 0,
            "vacation_paused": 0, "db_initialized": 0,
        }
        processed_count = [0] # Liste als Mutable Container für Worker

        # Queue befüllen
        queue = asyncio.Queue()
        for record in items:
            await queue.put(record)

        # Worker starten
        tasks = []
        for i in range(MAX_WORKERS):
            worker_id = f"[W-{i+1}]"
            task = asyncio.create_task(
                worker(queue, pool, session, EBAY_BASE_URL, cost_params, stats, total, processed_count, worker_id)
            )
            tasks.append(task)

        # Warten bis Queue leer ist
        await queue.join()

        # Worker stoppen
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    await pool.close()

    # Zusammenfassung
    logger.info("=" * 60)
    logger.info("📊 ZUSAMMENFASSUNG")
    logger.info(f"  Geprüft:           {total}")
    logger.info(f"  Unverändert:       {stats['unchanged']}")
    logger.info(f"  Preis aktualisiert:{stats['price_updated']}")
    logger.info(f"  Verkauft (BL):     {stats['sold']}")
    logger.info(f"  Rotations-Fallback:{stats.get('fallback_rotated', 0)}")
    logger.info(f"  Backup-Fehlt-Ende: {stats.get('no_backup_ended', 0)}")
    logger.info(f"  Urlaub (Pausiert): {stats['vacation_paused']}")
    logger.info(f"  Unrentabel:        {stats['unprofitable']}")
    logger.info(f"  eBay-Fehler (404): {stats.get('ebay_error', 0)}")
    logger.info(f"  Übersprungen:      {stats['skipped']}")
    logger.info(f"  Netzwerkfehler:    {stats['network_error']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

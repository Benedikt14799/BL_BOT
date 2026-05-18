# sync/booklooker/ready_sync.py
import asyncio
import os
import sys
import logging
import re
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from contextlib import contextmanager

# Projekt-Root in den Suchpfad legen, damit Imports funktionieren
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database import DatabaseManager
from proxy_manager import ProxyManager
from scrape import fetch_html, send_telegram_alert

load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=True)

LOG_FILE = os.path.join(PROJECT_ROOT, "sync_ready.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("BL-Ready-Sync")

LOCK_FILE = os.path.join(PROJECT_ROOT, "sync_ready.lock")

def is_pid_running(pid: int) -> bool:
    if os.name == 'nt':
        import ctypes
        kernel32 = ctypes.windll.kernel32
        PROCESS_QUERY_INFORMATION = 0x0400
        handle = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, pid)
        if handle:
            kernel32.CloseHandle(handle)
            return True
        return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

@contextmanager
def ready_sync_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                content = f.read().strip()
            if content:
                pid = int(content)
                if is_pid_running(pid):
                    logger.warning(f"⚠️ Backlog-Sync läuft bereits mit PID {pid}. Breche ab.")
                    raise RuntimeError(f"Backlog-Sync läuft bereits (PID {pid})")
                else:
                    logger.info(f"Veraltete Sperrdatei gefunden (PID {pid} läuft nicht mehr). Lösche sie.")
                    os.remove(LOCK_FILE)
        except (ValueError, OSError) as e:
            logger.warning(f"Fehler beim Lesen der Sperrdatei: {e}. Lösche sie.")
            try:
                os.remove(LOCK_FILE)
            except OSError:
                pass

    my_pid = os.getpid()
    try:
        with open(LOCK_FILE, "w") as f:
            f.write(str(my_pid))
        logger.info(f"Sperrdatei erstellt für PID {my_pid}.")
        yield
    finally:
        try:
            if os.path.exists(LOCK_FILE):
                with open(LOCK_FILE, "r") as f:
                    content = f.read().strip()
                if content == str(my_pid):
                    os.remove(LOCK_FILE)
                    logger.info(f"Sperrdatei für PID {my_pid} gelöscht.")
        except Exception as e:
            logger.error(f"Fehler beim Löschen der Sperrdatei: {e}")

async def fetch_bl_html(session: aiohttp.ClientSession, url: str, proxy_manager: ProxyManager = None) -> str:
    """Ruft die Booklooker-HTML-Seite mit Proxy-Unterstützung ab."""
    try:
        return await fetch_html(session, url, proxy_manager)
    except Exception as e:
        if "404" in str(e): return "404_NOT_FOUND"
        if "410" in str(e): return "410_GONE"
        logger.error(f"Fehler beim Abruf von {url}: {e}")
        return ""

def is_sold(html: str, soup: BeautifulSoup) -> str:
    """
    Prüft den Status auf Booklooker.
    Gibt "OK", "SOLD", "VACATION" oder "UNKNOWN" zurück.
    """
    if html in ["404_NOT_FOUND", "410_GONE"]:
        return "SOLD"
    
    if "Dieses Angebot ist nicht mehr verfügbar" in html or "Artikeldaten nicht gefunden" in html:
        return "SOLD"

    # Urlaubsmodus erkennen
    vacation_match = re.search(r"bis einschließlich\s+(\d{2}\.\d{2}\.\d{4})", html)
    if vacation_match:
        return "VACATION"

    # Verfügbarkeits-Indikator: Warenkorb-Button
    if soup:
        cart_button = soup.find("input", value=lambda v: v and "warenkorb" in v.lower())
        if cart_button:
            return "OK"

    # Block-Seiten erkennen
    if "zugriff verweigert" in html.lower() or "bot-schutz" in html.lower() or "captcha" in html.lower():
        logger.warning("BookLooker Block-Seite oder Captcha erkannt! Status UNKNOWN.")
        return "UNKNOWN"

    logger.warning("Weder Warenkorb noch Sold-Indikator gefunden. Status vorsichtshalber UNKNOWN.")
    return "UNKNOWN"

async def run_ready_sync(db_pool, progress_callback=None):
    """
    Führt den Abgleich für alle Bücher im Backlog (Status_id = 1) aus.
    """
    logger.info("=== STARTE GEZIELTEN BACKLOG-ABGLEICH ===")
    
    # 1. Alle Einträge im Backlog laden
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, linktobl, sku, title FROM library
            WHERE status_id = 1
              AND (ebay_listed IS FALSE OR ebay_listed IS NULL)
              AND (ebay_status IS NULL OR ebay_status != 'listed')
            ORDER BY id ASC;
        """)

    total_backlog = len(rows)
    logger.info(f"{total_backlog} Angebote im Backlog ('Bereit für eBay') gefunden.")
    
    if total_backlog == 0:
        msg = "ℹ️ *Backlog-Sync abgeschlossen:*\nKeine Angebote im Backlog ('Bereit für eBay') zum Abgleichen vorhanden."
        logger.info(msg)
        if progress_callback:
            await progress_callback(msg)
        else:
            await send_telegram_alert(msg)
        return

    if progress_callback:
        await progress_callback(f"🚀 *Backlog-Sync gestartet:*\nAbgleich von `{total_backlog}` Angeboten im Backlog gestartet...")

    # Zähler
    active_count = 0
    sold_count = 0
    vacation_count = 0
    unknown_count = 0
    processed = 0

    proxy_manager = ProxyManager(db_pool)
    
    async with aiohttp.ClientSession() as session:
        for idx, row in enumerate(rows):
            # Stopp-Flag prüfen
            stop_flag_path = os.path.join(PROJECT_ROOT, "stop_sync.flag")
            if os.path.exists(stop_flag_path):
                logger.warning("Stop-Flag (stop_sync.flag) erkannt. Beende Backlog-Sync vorzeitig...")
                os.remove(stop_flag_path)
                break

            library_id = row["id"]
            url = row["linktobl"]
            sku = row["sku"]
            title = row["title"]

            if not url:
                unknown_count += 1
                processed += 1
                continue

            html = await fetch_bl_html(session, url, proxy_manager)
            soup = None
            if html and html not in ["404_NOT_FOUND", "410_GONE"]:
                soup = BeautifulSoup(html, "html.parser")
            
            status = is_sold(html, soup)
            
            async with db_pool.acquire() as conn:
                if status == "SOLD":
                    sold_count += 1
                    await conn.execute("""
                        UPDATE library 
                        SET status_id = 5, 
                            ebay_listed = FALSE,
                            ebay_delisted_reason = 'sold_on_bl_backlog',
                            last_checked = NOW()
                        WHERE id = $1;
                    """, library_id)
                    logger.info(f"🗑️ [{sku}] Aussortiert (verkauft auf BL): {title}")
                elif status == "VACATION":
                    vacation_count += 1
                    await conn.execute("UPDATE library SET last_checked = NOW() WHERE id = $1;", library_id)
                    logger.info(f"🏖️ [{sku}] Verkäufer im Urlaub (auf BL): {title}")
                elif status == "OK":
                    active_count += 1
                    await conn.execute("UPDATE library SET last_checked = NOW() WHERE id = $1;", library_id)
                    logger.info(f"✅ [{sku}] Bestätigt (noch da): {title}")
                else:
                    unknown_count += 1
                    logger.warning(f"❓ [{sku}] Status unbekannt: {title}")

            processed += 1
            
            # Alle 100 Bücher oder bei Fertigstellung Zwischenstand per Callback senden
            if progress_callback and (processed % 100 == 0 or processed == total_backlog):
                progress_msg = (
                    f"🔄 *Backlog-Sync Fortschritt:* `{processed}/{total_backlog}` geprüft...\n\n"
                    f"✅ Noch verfügbar: `{active_count}`\n"
                    f"🗑️ Auf BL verkauft (aussortiert): `{sold_count}`\n"
                    f"🏖️ Verkäufer im Urlaub: `{vacation_count}`\n"
                    f"❓ Unbekannt/Übersprungen: `{unknown_count}`"
                )
                await progress_callback(progress_msg)
                
            # Kurzer Sleep zum Schutz vor Überlastung
            await asyncio.sleep(0.5)

    # Abschlussbericht senden
    final_msg = (
        f"🏁 *Backlog-Sync abgeschlossen!*\n\n"
        f"📊 *Ergebnis von {processed} geprüften Angeboten:*\n"
        f"✅ Bestätigt (bereit für Upload): `{active_count}`\n"
        f"🗑️ Auf BL verkauft (aussortiert): `{sold_count}`\n"
        f"🏖️ Verkäufer im Urlaub: `{vacation_count}`\n"
        f"❓ Unbekannt/Fehlerhaft: `{unknown_count}`\n\n"
        f"*Hinweis:* Alle auf BL verkauften Bücher wurden auf den Status `sold_on_bl` gesetzt und werden nicht hochgeladen."
    )
    logger.info(final_msg)
    if progress_callback:
        await progress_callback(final_msg)
    else:
        await send_telegram_alert(final_msg)

async def main():
    db_url = os.getenv("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    try:
        with ready_sync_lock():
            await run_ready_sync(pool)
    except RuntimeError as e:
        logger.warning(str(e))
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

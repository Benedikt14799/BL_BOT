import asyncio
import os
import logging
import json
import aiohttp
from datetime import datetime, time as dt_time
from dotenv import load_dotenv

# Importiere die heute optimierten Kern-Module
from sync.booklooker.ebay import run_sync as sync_ebay_run
from sync.booklooker.reactivate_vacation import main as reactivate_vacation_main
from sync import ebay_inventory_check
from sync.ebay_orders import process_orders, generate_daily_report
from database import DatabaseManager
from proxy_manager import ProxyManager
import scrape

load_dotenv()

CONFIG_FILE = "bot_config.json"
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def load_bot_config():
    if not os.path.exists(CONFIG_FILE):
        return {"auto_scrape": True, "auto_sync": True, "scrape_time": "10:00", "sync_time": "03:00", "report_times": ["09:00", "12:00", "18:00", "19:00"]}
    try:
        with open(CONFIG_FILE, "r") as f:
            cfg = json.load(f)
            if "report_times" not in cfg: cfg["report_times"] = ["09:00", "12:00", "18:00", "19:00"]
            return cfg
    except:
        return {"auto_scrape": True, "auto_sync": True, "scrape_time": "10:00", "sync_time": "03:00", "report_times": ["09:00", "12:00", "18:00", "19:00"]}

# Logging-Konfiguration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sync_service.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SyncService")

# ==========================================
# Kern-Aktionen
# ==========================================

async def send_telegram_report(text):
    if not TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"Telegram Report Fehler: {resp.status}")
    except Exception as e:
        logger.error(f"Fehler beim Senden des Reports: {e}")

async def run_status_report(pool):
    """Holt Statistiken und sendet sie an Telegram."""
    logger.info("Sende geplanten Status-Report...")
    try:
        # DB-Statistiken laden
        stats = await DatabaseManager.get_library_stats(pool)

        # Proxy Daten holen
        from proxy_manager import ProxyManager
        pm = ProxyManager(pool)
        proxy = await pm.get_current_usage()
        mb = proxy["bytes"] / (1024*1024)
        cost = float(proxy["cost"])
        
        msg = (
            "📊 *Automatischer System-Status*\n\n"
            f"📥 *Pipeline (Wartend):* {stats['pipeline']:,}\n"
            f"✅ *Bereit für eBay:* {stats['ready']:,}\n"
            f"📦 *Auf eBay gelistet:* {stats['listed']:,}\n"
            f"🗑️ *Aussortiert/Gefiltert:* {stats['filtered']:,}\n\n"
            f"🌐 *Proxy-Verbrauch heute:*\n"
            f"📦 {mb:.1f} MB | 💸 {cost:.2f} €\n"
            f"🔢 {proxy['requests']} Requests\n\n"
            "_Dienst läuft planmäßig._"
        ).replace(",", ".")
        await send_telegram_report(msg)
    except Exception as e:
        logger.error(f"Fehler beim Status-Report: {e}")

async def run_full_sync(pool):
    """Führt Bestandsabgleich und Preis-Sync aus."""
    logger.info("=== STARTE FULL SYNC (eBay, BL) ===")
    try:
        # 1. eBay Bestandsabgleich (Löscht Differenzen)
        logger.info("Starte eBay-Bestandsabgleich...")
        await ebay_inventory_check.run_inventory_sync(pool)

        # 2. Preis- & Status-Sync (Booklooker)
        logger.info("Starte Preis- & Bestands-Sync (BL)...")
        await sync_ebay_run(pool)
        
        logger.info("=== FULL SYNC ABGESCHLOSSEN ===")
    except Exception as e:
        logger.error(f"Fehler im Full Sync: {e}")

async def run_scraping(pool):
    """Führt das Scraping neuer Links aus main.py aus."""
    logger.info("=== STARTE AUTOMATISCHES SCRAPING ===")
    try:
        with scrape.scraping_lock():
            # Wir laden Links aus links.txt (wie in main.py)
            links_to_scrape = []
            links_file_path = "links.txt"
            if os.path.exists(links_file_path):
                with open(links_file_path, "r", encoding="utf-8") as file:
                    for line in file:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            links_to_scrape.append(line)
            
            skip_categories = os.getenv("SKIP_CATEGORY_SEARCH", "False").lower() == "true"
            if skip_categories:
                logger.info("SKIP_CATEGORY_SEARCH ist 'true'. Überspringe Kategorie-Suche im Hintergrund-Dienst...")
            else:
                pm = ProxyManager(pool)
                if links_to_scrape:
                    await scrape.insert_links_into_sitetoscrape(links_to_scrape, pool, pm)
                await scrape.scrape_and_save_pages(pool)
                
            await scrape.perform_webscrape_async(pool)
            logger.info("=== SCRAPING ABGESCHLOSSEN ===")
    except RuntimeError as e:
        logger.warning(str(e))
    except Exception as e:
        logger.error(f"Fehler beim Scraping: {e}")

# ==========================================
# Haupt-Schleife (Schedule)
# ==========================================
async def service_loop(pool):
    logger.info("✅ Sync-Service (Scheduled) gestartet.")
    
    now = datetime.now()
    today = now.date()
    last_tracked_day = today

    # Initialize daily task states based on scheduled times
    config = load_bot_config()

    # Sync state initialization
    sync_parts = [int(x) for x in config.get("sync_time", "03:00").split(":")]
    sync_time_today = datetime.combine(today, dt_time(sync_parts[0], sync_parts[1]))
    last_sync_day = today if now >= sync_time_today else None

    # Scrape state initialization
    scrape_parts = [int(x) for x in config.get("scrape_time", "10:00").split(":")]
    scrape_time_today = datetime.combine(today, dt_time(scrape_parts[0], scrape_parts[1]))
    last_scrape_day = today if now >= scrape_time_today else None

    # Report state initialization
    sent_reports_today = set()
    for r_time in config.get("report_times", ["09:00", "12:00", "18:00", "19:00"]):
        r_parts = [int(x) for x in r_time.split(":")]
        r_dt = datetime.combine(today, dt_time(r_parts[0], r_parts[1]))
        if now >= r_dt:
            sent_reports_today.add(r_time)

    last_order_interval = None
    proxy_alert_sent = False

    # Background task handles
    sync_task = None
    scrape_task = None
    order_task = None

    while True:
        try:
            if os.path.exists("stop_service.flag"):
                logger.warning("Stop-Flag gefunden. Beende Service...")
                os.remove("stop_service.flag")
                break

            now = datetime.now()
            today = now.date()
            config = load_bot_config()

            # Reset daily counters when day changes
            if today != last_tracked_day:
                logger.info(f"📅 Neuer Tag hat begonnen: {today}. Setze Tages-Tracker zurück.")
                sent_reports_today.clear()
                last_tracked_day = today

            # 1. Check Daily Sync (03:00)
            sync_time_parts = [int(x) for x in config.get("sync_time", "03:00").split(":")]
            sync_time_today = datetime.combine(today, dt_time(sync_time_parts[0], sync_time_parts[1]))
            if config.get("auto_sync") and last_sync_day != today and now >= sync_time_today:
                if sync_task is None or sync_task.done():
                    logger.info("⏳ Starte nächtlichen Full Sync im Hintergrund...")
                    sync_task = asyncio.create_task(run_full_sync(pool))
                    last_sync_day = today
                else:
                    logger.warning("⚠️ Sync sollte starten, aber eine vorherige Sync-Aufgabe läuft noch!")

            # 2. Check Daily Scrape (10:00)
            scrape_time_parts = [int(x) for x in config.get("scrape_time", "10:00").split(":")]
            scrape_time_today = datetime.combine(today, dt_time(scrape_time_parts[0], scrape_time_parts[1]))
            if config.get("auto_scrape") and last_scrape_day != today and now >= scrape_time_today:
                if scrape_task is None or scrape_task.done():
                    logger.info("⏳ Starte tägliches Scraping im Hintergrund...")
                    scrape_task = asyncio.create_task(run_scraping(pool))
                    last_scrape_day = today
                else:
                    logger.warning("⚠️ Scraping sollte starten, aber eine vorherige Scraping-Aufgabe läuft noch!")

            # 3. Check Scheduled Reports
            report_times = config.get("report_times", ["09:00", "12:00", "18:00", "19:00"])
            for r_time in report_times:
                r_parts = [int(x) for x in r_time.split(":")]
                r_dt = datetime.combine(today, dt_time(r_parts[0], r_parts[1]))
                if now >= r_dt and r_time not in sent_reports_today:
                    logger.info(f"Sende geplanten Report für {r_time}...")
                    await run_status_report(pool)
                    
                    if r_time in ["12:00", "19:00"]:
                        try:
                            h = 12 if r_time == "12:00" else 24
                            sales_report = await generate_daily_report(pool, h)
                            await send_telegram_report(sales_report)
                        except Exception as e:
                            logger.error(f"Fehler bei Sales Report {r_time}: {e}")
                    
                    sent_reports_today.add(r_time)

            # 4. Periodischer eBay Order Check (alle 30 Minuten, z.B. um :00 und :30)
            current_order_interval = now.replace(minute=now.minute - now.minute % 30, second=0, microsecond=0)
            if last_order_interval is None or last_order_interval != current_order_interval:
                if order_task is None or order_task.done():
                    logger.info(f"⏳ Starte periodischen eBay Order-Check für {current_order_interval.strftime('%H:%M')} im Hintergrund...")
                    
                    async def run_order_check_and_report(p):
                        try:
                            notifications = await process_orders()
                            for note in notifications:
                                await send_telegram_report(note)
                        except Exception as e:
                            logger.error(f"Fehler bei periodischem Order-Check: {e}")

                    order_task = asyncio.create_task(run_order_check_and_report(pool))
                    last_order_interval = current_order_interval
                else:
                    logger.warning("⚠️ Order-Check sollte starten, aber ein vorheriger Order-Check läuft noch!")
            
            # 5. Check Proxy Budget Alert
            pm = ProxyManager(pool)
            if not pm.is_budget_ok() and not proxy_alert_sent:
                await send_telegram_report("🛑 *BUDGET-ALARM*\nDas tägliche Proxy-Budget wurde erreicht. Der Bot hat den Scraper pausiert!")
                proxy_alert_sent = True
            elif pm.is_budget_ok():
                proxy_alert_sent = False

            await asyncio.sleep(30) # Alle 30 Sek prüfen für präzise Reports
            
        except Exception as e:
            logger.error(f"Kritischer Fehler im Service-Loop: {e}")
            await asyncio.sleep(600)


SERVICE_LOCK_FILE = os.path.join(os.path.dirname(__file__), "sync_service.lock")

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

from contextlib import contextmanager

@contextmanager
def service_lock():
    if os.path.exists(SERVICE_LOCK_FILE):
        try:
            with open(SERVICE_LOCK_FILE, "r") as f:
                content = f.read().strip()
            if content:
                pid = int(content)
                if is_pid_running(pid):
                    logger.warning(f"⚠️ Sync-Service läuft bereits mit PID {pid}. Breche ab.")
                    raise RuntimeError(f"Sync-Service läuft bereits (PID {pid})")
                else:
                    logger.info(f"Veraltete Service-Sperrdatei gefunden (PID {pid} läuft nicht mehr). Lösche sie.")
                    os.remove(SERVICE_LOCK_FILE)
        except (ValueError, OSError) as e:
            logger.warning(f"Fehler beim Lesen der Service-Sperrdatei: {e}. Lösche sie.")
            try:
                os.remove(SERVICE_LOCK_FILE)
            except OSError:
                pass

    my_pid = os.getpid()
    try:
        with open(SERVICE_LOCK_FILE, "w") as f:
            f.write(str(my_pid))
        logger.info(f"Service-Sperrdatei erstellt für PID {my_pid}.")
        yield
    finally:
        try:
            if os.path.exists(SERVICE_LOCK_FILE):
                with open(SERVICE_LOCK_FILE, "r") as f:
                    content = f.read().strip()
                if content == str(my_pid):
                    os.remove(SERVICE_LOCK_FILE)
                    logger.info(f"Service-Sperrdatei für PID {my_pid} gelöscht.")
        except Exception as e:
            logger.error(f"Fehler beim Löschen der Service-Sperrdatei: {e}")


async def main():
    db_url = os.getenv("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    try:
        await service_loop(pool)
    finally:
        await pool.close()

if __name__ == "__main__":
    try:
        with service_lock():
            asyncio.run(main())
    except RuntimeError as e:
        logger.warning(f"Dienst-Start verhindert: {e}")
    except KeyboardInterrupt:
        logger.info("Service durch Benutzer beendet.")

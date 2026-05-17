import asyncio
import os
import logging
import json
import aiohttp
from datetime import datetime, time as dt_time
from dotenv import load_dotenv

# Importiere die heute optimierten Kern-Module
from sync.booklooker.ebay import main as sync_ebay_main
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
        logging.FileHandler("sync_service.log"),
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

async def run_status_report():
    """Holt Statistiken und sendet sie an Telegram."""
    logger.info("Sende geplanten Status-Report...")
    db_url = os.getenv("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    try:
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
    finally:
        await pool.close()

async def run_full_sync():
    """Führt Bestandsabgleich und Preis-Sync aus."""
    logger.info("=== STARTE FULL SYNC (eBay, BL) ===")
    try:
        # 1. eBay Bestandsabgleich (Löscht Differenzen)
        logger.info("Starte eBay-Bestandsabgleich...")
        db_url = os.getenv("DATABASE_URL")
        pool = await DatabaseManager.create_pool(db_url)
        try:
            await ebay_inventory_check.run_inventory_sync(pool)
        finally:
            await pool.close()

        # 2. Preis- & Status-Sync (Booklooker)
        logger.info("Starte Preis- & Bestands-Sync (BL)...")
        await sync_ebay_main()
        
        logger.info("=== FULL SYNC ABGESCHLOSSEN ===")
    except Exception as e:
        logger.error(f"Fehler im Full Sync: {e}")

async def run_scraping():
    """Führt das Scraping neuer Links aus main.py aus."""
    logger.info("=== STARTE AUTOMATISCHES SCRAPING ===")
    try:
        db_url = os.getenv("DATABASE_URL")
        pool = await DatabaseManager.create_pool(db_url)
        try:
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
        finally:
            await pool.close()
    except Exception as e:
        logger.error(f"Fehler beim Scraping: {e}")

# ==========================================
# Haupt-Schleife (Schedule)
# ==========================================
async def service_loop():
    logger.info("✅ Sync-Service (Scheduled) gestartet.")
    
    last_sync_day = None
    last_scrape_day = None
    last_report_time = None # Speichert "YYYY-MM-DD HH:MM"
    last_order_sync_time = None
    proxy_alert_sent = False

    while True:
        try:
            if os.path.exists("stop_service.flag"):
                logger.warning("Stop-Flag gefunden. Beende Service...")
                os.remove("stop_service.flag")
                break

            now = datetime.now()
            today = now.date()
            now_str = now.strftime("%H:%M")
            day_hour_str = now.strftime("%Y-%m-%d %H:%M")
            config = load_bot_config()

            # 1. Check Sync (03:00)
            sync_time_parts = [int(x) for x in config.get("sync_time", "03:00").split(":")]
            if config.get("auto_sync") and last_sync_day != today:
                if now.hour == sync_time_parts[0] and now.minute >= sync_time_parts[1]:
                    await run_full_sync()
                    last_sync_day = today

            # 2. Check Scrape (10:00)
            scrape_time_parts = [int(x) for x in config.get("scrape_time", "10:00").split(":")]
            if config.get("auto_scrape") and last_scrape_day != today:
                if now.hour == scrape_time_parts[0] and now.minute >= scrape_time_parts[1]:
                    await run_scraping()
                    last_scrape_day = today

            # 3. Check Reports (09:00, 12:00, 18:00, 19:00)
            report_times = config.get("report_times", ["09:00", "12:00", "18:00", "19:00"])
            if now_str in report_times and last_report_time != day_hour_str:
                # Normaler Status-Report
                await run_status_report()
                
                # Wenn 12:00 oder 19:00, zusätzlich Sales Report senden
                if now_str in ["12:00", "19:00"]:
                    db_url = os.getenv("DATABASE_URL")
                    pool = await DatabaseManager.create_pool(db_url)
                    try:
                        # Mittags: Letzte 12h, Abends: Letzte 24h
                        h = 12 if now_str == "12:00" else 24
                        sales_report = await generate_daily_report(pool, h)
                        await send_telegram_report(sales_report)
                    finally:
                        await pool.close()
                
                last_report_time = day_hour_str

            # 4. Periodischer eBay Order Check (alle 30 Minuten)
            if (now.minute % 30 == 0) and last_order_sync_time != day_hour_str:
                logger.info("Starte periodischen eBay Order-Check...")
                notifications = await process_orders()
                for note in notifications:
                    await send_telegram_report(note)
                last_order_sync_time = day_hour_str
            
            # 5. Check Proxy Budget Alert
            db_url = os.getenv("DATABASE_URL")
            pool = await DatabaseManager.create_pool(db_url)
            try:
                pm = ProxyManager(pool)
                if not pm.is_budget_ok() and not proxy_alert_sent:
                    await send_telegram_report("🛑 *BUDGET-ALARM*\nDas tägliche Proxy-Budget wurde erreicht. Der Bot hat den Scraper pausiert!")
                    proxy_alert_sent = True
                elif pm.is_budget_ok():
                    proxy_alert_sent = False
            finally:
                await pool.close()

            await asyncio.sleep(30) # Alle 30 Sek prüfen für präzise Reports
            
        except Exception as e:
            logger.error(f"Kritischer Fehler im Service-Loop: {e}")
            await asyncio.sleep(600)


async def main():
    await service_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service durch Benutzer beendet.")

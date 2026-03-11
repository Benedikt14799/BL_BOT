import asyncio
import os
import logging
import random
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from datetime import datetime, time, timedelta

# Eigene Module
from database import DatabaseManager
from price_processing import PriceProcessing
import ebay_upload

# Telegram Bot (stubbed for now, will be expanded)
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

load_dotenv()

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
# Globale State-Variablen
# ==========================================
class SyncState:
    is_running = True
    items_processed_today = 0
    price_updates_today = 0
    sold_items_today = 0
    unprofitable_items_today = 0
    total_items = 0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0"
]

# ==========================================
# Kern-Funktionen: Sync
# ==========================================

async def fetch_bl_html(session: aiohttp.ClientSession, url: str) -> str:
    """Holt das HTML von Booklooker mit randomisiertem User-Agent."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        async with session.get(url, headers=headers, timeout=15) as resp:
            if resp.status == 200:
                return await resp.text()
            elif resp.status == 404:
                return "404_NOT_FOUND" 
            elif resp.status == 429:
                logger.error("Rate Limit (429) von Booklooker erreicht!")
                await asyncio.sleep(60) # 1 Min Pause
                return ""
            else:
                logger.error(f"Failed to fetch BL URL {url}: {resp.status}")
                return ""
    except Exception as e:
        logger.error(f"Error fetching BL URL {url}: {e}")
        return ""

async def process_item(item: dict, db_pool, session: aiohttp.ClientSession, token: str, base_url: str, fixed_costs_monthly: Decimal, expected_sales: int, steuer_satz: Decimal):
    """Prüft ein einzelnes Item auf Preisänderungen oder Verkauf."""
    internal_id = item['id']
    bl_url = item.get('linktobl') or item.get('link')
    current_ebay_price = item.get('start_price')
    sku = item.get('sku')
    title = item.get('title', 'Unknown')
    
    if not bl_url or not sku:
        logger.warning(f"Item {internal_id} hat keine URL oder SKU. Überspringe.")
        return

    html = await fetch_bl_html(session, bl_url)
    if not html:
        return

    # 1. Prüfen ob verkauft (404 oder fehlender Preis)
    soup = BeautifulSoup(html, 'html.parser')
    new_ek = PriceProcessing._safe_clean_price(soup)
    
    # Erweiterte Sold-Erkennung (z.B. Button fehlt oder "Angebot nicht verfügbar")
    is_sold = False
    if html == "404_NOT_FOUND" or new_ek == 0:
        is_sold = True
    elif "Dieses Angebot ist nicht mehr verfügbar" in html or soup.find("input", {"value": "In den Warenkorb"}) is None:
        is_sold = True

    if is_sold:
        logger.info(f"[{sku}] Verkauft auf BL! Beende eBay Angebot...")
        success = await ebay_upload.withdraw_offer(session, sku, token, base_url)
        if success or "API" not in str(success): # Assume true if API didn't hard-fail
            await DatabaseManager.record_sold_listing(db_pool, internal_id, bl_url, sku, title, "sold_on_bl")
            SyncState.sold_items_today += 1
        return

    # 2. Preis extrahieren
    new_shipping = PriceProcessing._safe_extract_shipping(soup)
    target_ebay_price = PriceProcessing._compute_final_price(
        new_ek, new_shipping, Decimal('0.50'), Decimal('1.75'), 
        steuer_satz, fixed_costs_monthly, expected_sales
    ) # Hardcoded AddCosts vorerst

    if target_ebay_price is None:
        return

    from decimal import Decimal
    # Wenn sich der Preis um mehr als 0.01€ geändert hat:
    if abs(Decimal(str(target_ebay_price)) - current_ebay_price) > Decimal('0.01'):
        logger.info(f"[{sku}] Preisänderung erkannt: BL {new_ek}€ -> neuer eBay Zielpreis {target_ebay_price}€")
        
        # 3. Wirtschaftlichkeit nach Preisänderung neu berechnen
        prof = PriceProcessing.recheck_profitability(
            ek=new_ek, bl_shipping=new_shipping, current_ebay_price=target_ebay_price,
            monthly_fixed_costs=fixed_costs_monthly, expected_sales=expected_sales,
            addcost_low_mid=Decimal('0.50'), addcost_high=Decimal('1.75'), steuer_satz=steuer_satz
        )

        if not prof['rentabel']:
            logger.warning(f"[{sku}] Nach Preiserhöhung unrentabel! Beende Angebot...")
            await ebay_upload.withdraw_offer(session, sku, token, base_url)
            await DatabaseManager.record_sold_listing(db_pool, internal_id, bl_url, sku, title, "unprofitable_after_sync")
            SyncState.unprofitable_items_today += 1
            return
        
        # 4. Ist rentabel -> Preis bei eBay aktualisieren
        logger.info(f"[{sku}] Weiterhin rentabel. Aktualisiere eBay Preis auf {target_ebay_price}€...")
        success = await ebay_upload.update_inventory_price(session, sku, float(target_ebay_price), token, base_url)
        
        if success:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE library 
                    SET start_price = $1, margin = $2, purchase_price = $3, purchase_shipping = $4
                    WHERE id = $5
                """, target_ebay_price, prof['marge'], new_ek, new_shipping, internal_id)
            SyncState.price_updates_today += 1
    
    SyncState.items_processed_today += 1


async def sync_loop(db_pool):
    """Die 24/7 Hauptschleife für den Abgleich."""
    logger.info("Sync-Schleife gestartet.")
    from ebay_token_manager import get_token
    EBAY_USER_TOKEN = get_token()
    EBAY_BASE_URL = os.environ.get("EBAY_BASE_URL", "https://api.ebay.com")
    
    from decimal import Decimal
    try:
        fixed_costs_monthly = Decimal(os.environ.get("FIXKOSTEN_MONATLICH", "79.95").replace(',', '.'))
        expected_sales = int(os.environ.get("ERWARTETE_VERKAEUFE", "200"))
        steuer_satz = Decimal(os.environ.get("STEUERSATZ", "7.0").replace(',', '.'))
    except Exception as e:
        logger.warning(f"Fehler beim Laden der Sync-Kosten-Umgebungsvariablen: {e}. Nutze Fallback.")
        fixed_costs_monthly = Decimal("79.95")
        expected_sales = 200
        steuer_satz = Decimal("7.0")

    while True:
        if not SyncState.is_running:
            await asyncio.sleep(10)
            continue
            
        try:
            async with db_pool.acquire() as conn:
                items = await conn.fetch("""
                    SELECT id, title, start_price, sku, linktobl, link
                    FROM library 
                    WHERE ebay_listed = TRUE
                """)

            if not items:
                logger.info("Keine gelisteten Artikel gefunden. Warte 10 Minuten...")
                await asyncio.sleep(600)
                continue

            SyncState.total_items = len(items)
            
            # Daily Scan Berechnung: Alle Items über ~20h verteilen (lässt 4h Puffer)
            target_time_seconds = 20 * 60 * 60
            base_delay = target_time_seconds / len(items)
            if base_delay < 8.5:  # Max ~7 Queries pro Minute
                base_delay = 8.5

            logger.info(f"Starte Durchlauf für {len(items)} Artikel. Base Delay: {base_delay:.2f}s")
            
            async with aiohttp.ClientSession() as session:
                for idx, record in enumerate(items):
                    if not SyncState.is_running:
                        logger.info("Sync pausiert.")
                        break

                    await process_item(dict(record), db_pool, session, EBAY_USER_TOKEN, EBAY_BASE_URL, fixed_costs_monthly, expected_sales, steuer_satz)
                    
                    # Anti-Blocking Jitter: +/- 30% vom base_delay
                    jitter = random.uniform(-0.3, 0.3) * base_delay
                    final_delay = max(5.0, base_delay + jitter)
                    
                    if idx % 50 == 0:
                        logger.info(f"Fortschritt: {idx}/{len(items)} Artikel geprüft.")
                        
                    await asyncio.sleep(final_delay)

            logger.info("Tages-Durchlauf abgeschlossen. Warte auf nächsten Zyklus...")
            
            # Reset Tageszähler nach erfolgreichem Loop
            SyncState.items_processed_today = 0
            SyncState.price_updates_today = 0
            SyncState.sold_items_today = 0
            SyncState.unprofitable_items_today = 0

            await asyncio.sleep(3600) # Warte 1 Stunde bevor es von vorne anfängt

        except Exception as e:
            logger.error(f"Fehler in der Sync-Schleife: {e}")
            await asyncio.sleep(60)

# ==========================================
# Telegram Bot Commands
# ==========================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    SyncState.is_running = True
    await update.message.reply_text("✅ Sync-Dienst gestartet!")

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    SyncState.is_running = False
    await update.message.reply_text("⏸️ Sync-Dienst pausiert!")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status = "Aktiv 🟢" if SyncState.is_running else "Pausiert ⏸️"
    msg = (f"📊 *Sync-Status:*\n"
           f"Zustand: {status}\n"
           f"Geprüft heute: {SyncState.items_processed_today} / {SyncState.total_items}\n"
           f"Preisanpassungen: {SyncState.price_updates_today}\n"
           f"BL-Verkäufe erkannt: {SyncState.sold_items_today}\n"
           f"Unrentabel geworden: {SyncState.unprofitable_items_today}")
    await update.message.reply_text(msg, parse_mode='Markdown')

async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (f"📈 *Sofort-Report:*\n"
           f"Geprüft: {SyncState.items_processed_today}\n"
           f"Angepasste Preise: {SyncState.price_updates_today}\n"
           f"Verkäufe BL erkannt: {SyncState.sold_items_today}\n"
           f"Unrentabel geworden: {SyncState.unprofitable_items_today}")
    await update.message.reply_text(msg, parse_mode='Markdown')

async def evening_report_loop(bot):
    """Sendet täglich um 21:00 Uhr einen automatisierten Bericht."""
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not chat_id:
        logger.warning("TELEGRAM_CHAT_ID fehlt. Abend-Report deaktiviert.")
        return

    while True:
        now = datetime.now()
        target = now.replace(hour=21, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        wait_seconds = (target - now).total_seconds()
        
        logger.info(f"Nächster Abend-Report in {wait_seconds/3600:.2f} Stunden.")
        await asyncio.sleep(wait_seconds)
        
        try:
            msg = (f"🌙 *Abend-Report (21:00 Uhr):*\n"
                   f"Geprüft heute: {SyncState.items_processed_today}\n"
                   f"Angepasste Preise: {SyncState.price_updates_today}\n"
                   f"Verkäufe BL erkannt: {SyncState.sold_items_today}\n"
                   f"Unrentabel geworden: {SyncState.unprofitable_items_today}")
            await bot.send_message(chat_id=chat_id, text=msg, parse_mode='Markdown')
            logger.info("Abend-Report gesendet.")
        except Exception as e:
            logger.error(f"Fehler beim Senden des Abend-Reports: {e}")

async def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL fehlt!")
        return

    pool = await DatabaseManager.create_pool(db_url)
    
    # Starte Sync im Hintergrund
    asyncio.create_task(sync_loop(pool))

    # Starte Telegram Bot
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if bot_token:
        logger.info("Starte Telegram Bot...")
        app = ApplicationBuilder().token(bot_token).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("stop", cmd_stop))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("report", cmd_report))
        
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        
        # Starte den abendlichen Report-Loop
        asyncio.create_task(evening_report_loop(app.bot))
        
        # Halte den Prozess am Leben
        while True:
            await asyncio.sleep(3600)
    else:
        logger.warning("TELEGRAM_BOT_TOKEN fehlt. Bot läuft nur als Hintergrund-Worker ohne Telegram.")
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

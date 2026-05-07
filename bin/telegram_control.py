import os
import time
import asyncio
import logging
import requests
from dotenv import load_dotenv
import asyncpg

# Importiere die Kern-Logik aus den bestehenden Modulen
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sync.booklooker.reactivate_vacation import reactivate_vacation
from sync.booklooker import ebay as sync_ebay
from sync import ebay_inventory_check
from database import DatabaseManager

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TelegramControl")

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_URL = os.getenv("DATABASE_URL")

if not TOKEN or not CHAT_ID:
    logger.error("TELEGRAM_BOT_TOKEN oder TELEGRAM_CHAT_ID fehlen in .env!")
    exit(1)

def send_message(text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Fehler beim Senden der Nachricht: {e}")

async def run_urlaub():
    send_message("🏖️ *Urlaubs-Reaktivierung gestartet...*")
    pool = await asyncpg.create_pool(dsn=DB_URL, ssl=True)
    try:
        res = await reactivate_vacation(pool)
        msg = f"✅ *Urlaubs-Reaktivierung abgeschlossen!*\n\n• Geprüft: {res['found']}\n• Reaktiviert: *{res['reactivated']}* 📖"
        send_message(msg)
    except Exception as e:
        send_message(f"❌ *Fehler bei Urlaubs-Reaktivierung:* {str(e)}")
    finally:
        await pool.close()

async def run_ebaysync():
    send_message("📦 *eBay Bestandsabgleich gestartet...*")
    pool = await asyncpg.create_pool(dsn=DB_URL, ssl=True)
    try:
        res = await ebay_inventory_check.run_inventory_sync(pool)
        msg = f"✅ *eBay Bestandsabgleich fertig!*\n\n• DB-Artikel geprüft: {res['total_checked']}\n• Von eBay gelöscht: *{res['removed']}* 🗑️\n• eBay Orphans (nicht in DB): {res['orphans']}"
        send_message(msg)
    except Exception as e:
        send_message(f"❌ *Fehler bei eBay Bestandsabgleich:* {str(e)}")
    finally:
        await pool.close()

async def run_blsync():
    send_message("🔄 *Bestands- & Preis-Sync gestartet...*")
    pool = await asyncpg.create_pool(dsn=DB_URL, ssl="require")
    try:
        res = await sync_ebay.run_sync(pool)
        s = res["stats"]
        msg = f"✅ *Bestands- & Preis-Sync fertig!*\n\n• Artikel geprüft: {res['total']}\n• Preise aktualisiert: *{s.get('price_updated', 0)}* 💸\n• Verkauft (BL): *{s.get('sold', 0)}* 🛒\n• Urlaub (Pausiert): {s.get('vacation_paused', 0)} 🏖️\n• Unrentabel: {s.get('unprofitable', 0)} ✂️"
        send_message(msg)
    except Exception as e:
        send_message(f"❌ *Fehler bei Bestands- & Preis-Sync:* {str(e)}")
    finally:
        await pool.close()

async def handle_update(update):
    if "message" not in update:
        return
    
    msg = update["message"]
    text = msg.get("text", "")
    sender_id = str(msg["chat"]["id"])
    
    if sender_id != CHAT_ID:
        logger.warning(f"Unbefugter Zugriff von ID: {sender_id}")
        return

    if text == "/urlaub":
        asyncio.create_task(run_urlaub())
    elif text == "/ebaysync":
        asyncio.create_task(run_ebaysync())
    elif text == "/blsync":
        asyncio.create_task(run_blsync())
    elif text == "/start":
        send_message("🎮 *BL_BOT Control Panel aktiv*\n\nBefehle:\n/urlaub - Reaktivierung\n/ebaysync - eBay Abgleich\n/blsync - BL Preis-Sync")

async def main():
    logger.info("Telegram Control Bot gestartet...")
    offset = 0
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    
    while True:
        try:
            params = {"offset": offset, "timeout": 30}
            resp = requests.get(url, params=params, timeout=35)
            if resp.status_code == 200:
                data = resp.json()
                for update in data.get("result", []):
                    await handle_update(update)
                    offset = update["update_id"] + 1
            else:
                logger.error(f"Telegram API Error: {resp.status_code}")
        except Exception as e:
            logger.error(f"Polling Fehler: {e}")
            await asyncio.sleep(5)
        
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())

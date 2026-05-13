import os
import time
import asyncio
import logging
import aiohttp
import subprocess
import json
import signal
from dotenv import load_dotenv

# Importiere die Kern-Logik aus den bestehenden Modulen
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sync.booklooker.reactivate_vacation import reactivate_vacation
from sync.booklooker import ebay as sync_ebay
from sync import ebay_inventory_check
from database import DatabaseManager
import ebay_upload

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TelegramControl")

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_URL = os.getenv("DATABASE_URL")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', 'bot_config.json')
PID_FILE = os.path.join(os.path.dirname(__file__), '..', 'sync_service.pid')

if not TOKEN or not CHAT_ID:
    logger.error("TELEGRAM_BOT_TOKEN oder TELEGRAM_CHAT_ID fehlen in .env!")
    exit(1)

def load_config():
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
    except: pass
    return {"auto_scrape": True, "auto_sync": True}

def save_config(cfg):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)

async def send_message_async(text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    payload["parse_mode"] = ""
                    await session.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Fehler beim Senden der Nachricht: {e}")

# --- Process Management ---

def is_service_running():
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                pid = int(f.read().strip())
            
            if os.name == 'nt':
                # Windows Check
                output = subprocess.check_output(f'tasklist /FI "PID eq {pid}"', shell=True).decode()
                return str(pid) in output
            else:
                # Linux/Unix Check
                try:
                    os.kill(pid, 0)
                    return True
                except OSError:
                    return False
        except:
            return False
    return False

async def start_service():
    if is_service_running():
        return "⚠️ Service läuft bereits."
    
    try:
        script_path = os.path.join(os.path.dirname(__file__), '..', 'sync_service.py')
        # Start detached process
        proc = subprocess.Popen([sys.executable, script_path], 
                                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS if os.name == 'nt' else 0,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(PID_FILE, "w") as f:
            f.write(str(proc.pid))
        return "🚀 Automatisierung gestartet."
    except Exception as e:
        return f"❌ Fehler beim Starten: {e}"

async def stop_service():
    msg = "🛑 Beende alle Prozesse..."
    # 1. Stop Flag für Graceful Stop
    with open(os.path.join(os.path.dirname(__file__), '..', 'stop_service.flag'), "w") as f:
        f.write("stop")
    with open(os.path.join(os.path.dirname(__file__), '..', 'stop_upload.flag'), "w") as f:
        f.write("stop")
        
    # 2. Kill the main service if PID exists
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                pid = int(f.read().strip())
            subprocess.run(f"taskkill /F /PID {pid} /T", shell=True, capture_output=True)
            os.remove(PID_FILE)
        except: pass
    
    # 3. Kill any orphan python processes of this project (except this one)
    # Be cautious here, but the user asked for "generally everything"
    try:
        current_pid = os.getpid()
        # Find all python processes and kill those that are not current_pid and are in this directory
        # Simplified: just kill common script names
        for script in ["scrape.py", "ebay_upload.py", "sync_service.py", "main.py"]:
            subprocess.run(f'taskkill /F /FI "IMAGENAME eq python.exe" /FI "WINDOWTITLE eq {script}*"', shell=True, capture_output=True)
    except: pass

    return "✅ Alles gestoppt."

# --- Handlers ---

async def run_urlaub():
    await send_message_async("🏖️ *Urlaubs-Reaktivierung gestartet...*")
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        res = await reactivate_vacation(pool)
        await send_message_async(f"✅ *Urlaub fertig!*\n• Geprüft: {res['found']}\n• Reaktiviert: *{res['reactivated']}*")
    except Exception as e:
        await send_message_async(f"❌ Fehler: {e}")
    finally:
        if 'pool' in locals() and pool: await pool.close()

async def run_ebaysync():
    await send_message_async("📦 *eBay Abgleich gestartet...*")
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        res = await ebay_inventory_check.run_inventory_sync(pool)
        await send_message_async(f"✅ *eBay Abgleich fertig!*\n• Gelöscht: *{res['removed']}*")
    except Exception as e:
        await send_message_async(f"❌ Fehler: {e}")
    finally:
        if 'pool' in locals() and pool: await pool.close()

async def run_upload():
    await send_message_async("🚀 *Manueller eBay-Upload gestartet (Batch)...*")
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        res = await ebay_upload.run_upload_batch(pool, limit=50)
        await send_message_async(f"✅ *Upload fertig!*\n• Erfolg: *{res['success']}*\n• Fehler: {res['failed']}")
    except Exception as e:
        await send_message_async(f"❌ Fehler: {e}")
    finally:
        if 'pool' in locals() and pool: await pool.close()

async def run_blsync():
    await send_message_async("🔄 *Bestands- & Preis-Sync gestartet...*")
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        await sync_ebay.run_sync(pool)
        await send_message_async("✅ *Bestands- & Preis-Sync fertig!*")
    except Exception as e:
        await send_message_async(f"❌ Fehler: {e}")
    finally:
        if 'pool' in locals() and pool: await pool.close()

async def handle_update(update):
    if "message" not in update: return
    msg = update["message"]
    text = msg.get("text", "")
    sender_id = str(msg["chat"]["id"])
    if sender_id != CHAT_ID: return

    if text == "/start":
        res = await start_service()
        await send_message_async(f"{res}\n\nBefehle:\n/status - Aktueller Stand\n/stop - Alles beenden\n/config - Einstellungen\n/upload - Manueller Upload")
    elif text == "/stop":
        res = await stop_service()
        await send_message_async(res)
    elif text == "/status":
        running = "✅ LÄUFT" if is_service_running() else "🛑 GESTOPPT"
        try:
            pool = await DatabaseManager.create_pool(DB_URL)
            stats = await DatabaseManager.get_library_stats(pool)
            await pool.close()
            msg = (f"🤖 *Status:* {running}\n\n"
                   f"📥 Wartend: {stats['pipeline']}\n"
                   f"✅ Bereit: {stats['ready']}\n"
                   f"📦 Gelistet: {stats['listed']}\n"
                   f"🗑️ Gefiltert: {stats['filtered']}")
            await send_message_async(msg)
        except Exception as e:
            await send_message_async(f"🤖 *Status:* {running}\n(Fehler bei Stats-Abfrage: {e})")
    elif text == "/config":
        cfg = load_config()
        s = "✅ AN" if cfg.get("auto_sync") else "❌ AUS"
        sc = "✅ AN" if cfg.get("auto_scrape") else "❌ AUS"
        await send_message_async(f"⚙️ *Konfiguration:*\n\n• Auto-Sync: {s} (/toggle_sync)\n• Auto-Scrape: {sc} (/toggle_scrape)\n\n_Sync: 03:00 Uhr | Scrape: 10:00 Uhr_")
    elif text == "/toggle_sync":
        cfg = load_config()
        cfg["auto_sync"] = not cfg.get("auto_sync", True)
        save_config(cfg)
        await send_message_async(f"Sync ist jetzt {'AN' if cfg['auto_sync'] else 'AUS'}.")
    elif text == "/toggle_scrape":
        cfg = load_config()
        cfg["auto_scrape"] = not cfg.get("auto_scrape", True)
        save_config(cfg)
        await send_message_async(f"Scrape ist jetzt {'AN' if cfg['auto_scrape'] else 'AUS'}.")
    elif text == "/upload":
        asyncio.create_task(run_upload())
    elif text == "/urlaub":
        asyncio.create_task(run_urlaub())
    elif text == "/ebaysync":
        asyncio.create_task(run_ebaysync())
    elif text == "/blsync":
        asyncio.create_task(run_blsync())

async def main():
    logger.info("Telegram Control Bot gestartet...")
    offset = 0
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                params = {"offset": offset, "timeout": 30}
                async with session.get(url, params=params, timeout=35) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for update in data.get("result", []):
                            await handle_update(update)
                            offset = update["update_id"] + 1
                    else: logger.error(f"Telegram API Error: {resp.status}")
            except Exception as e:
                logger.error(f"Polling Fehler: {e}")
                await asyncio.sleep(5)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())

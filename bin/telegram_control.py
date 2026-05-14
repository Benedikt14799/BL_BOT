import os
import time
import asyncio
import logging
import aiohttp
import subprocess
import json
import signal
from datetime import datetime
from dotenv import load_dotenv

# Importiere die Kern-Logik aus den bestehenden Modulen
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sync.booklooker.reactivate_vacation import reactivate_vacation
from sync.booklooker import ebay as sync_ebay
from sync import ebay_inventory_check
from sync.ebay_orders import process_orders, generate_daily_report
from sync.ebay_negotiation import eBayNegotiation
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

async def run_sales():
    await send_message_async("💰 *Suche nach neuen eBay Verkäufen...*")
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        # 1. Neue Verkäufe abrufen & verarbeiten
        notifications = await process_orders()
        if not notifications:
            await send_message_async("ℹ️ Keine neuen Verkäufe gefunden.")
        else:
            for note in notifications:
                await send_message_async(note)
        
        # 2. Tagesbericht senden (letzte 24h)
        report = await generate_daily_report(pool, 24)
        await send_message_async(report)
        
    except Exception as e:
        await send_message_async(f"❌ Fehler: {e}")
    finally:
        if 'pool' in locals() and pool: await pool.close()

async def run_report():
    await send_message_async("📊 *Generiere Controlling-Bericht...*")
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        async with pool.acquire() as conn:
            # 1. Alle Fixkosten summieren
            fixed_costs = await conn.fetchval("SELECT SUM(amount) FROM fixed_costs") or 0
            
            # 2. Gewinne des aktuellen Monats holen
            first_of_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            rows = await conn.fetch("""
                SELECT net_profit FROM ebay_orders 
                WHERE creation_date >= $1
            """, first_of_month)
            
            total_profit = sum(r["net_profit"] for r in rows) if rows else 0
            netto_after_fixed = total_profit - fixed_costs
            
            # 3. Break-Even Analyse
            msg = f"📈 *Controlling-Bericht ({datetime.now().strftime('%B')})*\n"
            msg += f"━━━━━━━━━━━━━━━━━━━━\n"
            msg += f"💰 Brutto-Gewinn: {total_profit:.2f}€\n"
            msg += f"🏠 Fixkosten (Summe): {fixed_costs:.2f}€\n"
            msg += f"━━━━━━━━━━━━━━━━━━━━\n"
            
            if netto_after_fixed >= 0:
                msg += f"✅ *Netto-Gewinn: {netto_after_fixed:.2f}€*\n"
                msg += f"🥳 Du bist diesen Monat im Plus!"
            else:
                needed = abs(netto_after_fixed)
                msg += f"⚠️ *Netto: {netto_after_fixed:.2f}€*\n"
                msg += f"📉 Noch {needed:.2f}€ bis zum Break-Even.\n\n"
                
                # Durchschnittsgewinn berechnen für Prognose
                if rows:
                    avg_profit = total_profit / len(rows)
                    if avg_profit > 0:
                        books_needed = int(needed / avg_profit) + 1
                        msg += f"👉 Du musst noch ca. *{books_needed} Bücher* verkaufen."
            
            await send_message_async(msg)
            
    except Exception as e:
        await send_message_async(f"❌ Fehler: {e}")
    finally:
        if 'pool' in locals() and pool: await pool.close()

async def run_watchers():
    await send_message_async("🔍 *Suche nach Artikeln mit Beobachtern...*")
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        neg = eBayNegotiation(pool)
        items = await neg.find_eligible_items()
        
        if not items:
            await send_message_async("ℹ️ Aktuell keine Artikel für Preisvorschläge berechtigt.")
        else:
            msg = f"👀 *Berechtigte Artikel ({len(items)}):*\n\n"
            for item in items[:15]: # Max 15 anzeigen
                price_data = item.get("display_price", {})
                val = price_data.get("value", "0")
                curr = price_data.get("currency", "EUR")
                msg += f"• Listing `{item.get('listingId')}`: {val} {curr}\n"
            
            if len(items) > 15:
                msg += f"\n... und {len(items)-15} weitere."
            
            msg += f"\n\nNutze `/send_offers 35` um 35% deines Gewinns als Rabatt zu geben."
            await send_message_async(msg)
    except Exception as e:
        await send_message_async(f"❌ Fehler: {e}")
    finally:
        if 'pool' in locals() and pool: await pool.close()

async def run_send_offers(profit_share):
    await send_message_async(f"🚀 *Sende Angebote (Rabatt = {profit_share}% deines Gewinns) an alle Beobachter...*")
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        neg = eBayNegotiation(pool)
        count = await neg.send_offers_to_watchers(profit_share_percent=profit_share)
        await send_message_async(f"✅ Erfolgreich *{count} Angebote* verschickt!")
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

    if text in ["/start", "/help"]:
        res = await start_service()
        msg = (f"{res}\n\n"
               f"📋 *Alle Befehle:*\n"
               f"━━━━━━━━━━━━\n"
               f"📦 *Bestand & Sync:*\n"
               f"• /status - Aktueller Datenbank-Stand\n"
               f"• /blsync - Abgleich Booklooker ↔️ eBay\n"
               f"• /ebaysync - eBay-Listen bereinigen\n"
               f"• /urlaub - Urlaubs-Reaktivierung\n"
               f"• /upload - Manueller eBay-Upload\n\n"
               f"💰 *Sales & Controlling:*\n"
               f"• /sales - Suche nach neuen Verkäufen\n"
               f"• /report - Monatsbericht & Break-Even\n"
               f"• /costs - Fixkosten verwalten\n\n"
               f"👀 *Marketing:*\n"
               f"• /watchers - Beobachter finden\n"
               f"• /send_offers - Angebote senden (Anteil vom Gewinn)\n\n"
               f"⚙️ *System:*\n"
               f"• /config - Einstellungen (Auto-Sync etc.)\n"
               f"• /stop - Alle Prozesse beenden")
        await send_message_async(msg)
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
    elif text == "/sales":
        asyncio.create_task(run_sales())
    elif text == "/report":
        asyncio.create_task(run_report())
    elif text == "/costs":
        try:
            pool = await DatabaseManager.create_pool(DB_URL)
            async with pool.acquire() as conn:
                # Sicherheits-Check: Tabelle händisch anlegen, falls sie fehlt
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS fixed_costs (
                        id SERIAL PRIMARY KEY,
                        label VARCHAR(255) NOT NULL,
                        amount NUMERIC(10, 2) NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                rows = await conn.fetch("SELECT id, label, amount FROM fixed_costs ORDER BY id")
                if not rows:
                    await send_message_async("ℹ️ Keine Fixkosten hinterlegt. Nutze `/add_cost Name Betrag`.")
                else:
                    msg = "🏠 *Deine Fixkosten-Übersicht:*\n\n"
                    total = 0
                    for r in rows:
                        msg += f"ID {r['id']}: *{r['label']}* - {r['amount']:.2f}€\n"
                        total += r["amount"]
                    msg += f"\n━━━━━━━━━━━━\n💰 *Gesamt: {total:.2f}€*"
                    await send_message_async(msg)
            await pool.close()
        except Exception as e:
            await send_message_async(f"❌ Fehler: {e}")
    elif text.startswith("/add_cost"):
        try:
            parts = text.split(maxsplit=2)
            if len(parts) < 3:
                await send_message_async("ℹ️ Nutzung: `/add_cost Name Betrag` (z.B. `/add_cost Miete 50`)")
                return
            label = parts[1]
            amount = float(parts[2].replace(",", "."))
            pool = await DatabaseManager.create_pool(DB_URL)
            async with pool.acquire() as conn:
                # Auch hier Sicherheits-Check
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS fixed_costs (
                        id SERIAL PRIMARY KEY,
                        label VARCHAR(255) NOT NULL,
                        amount NUMERIC(10, 2) NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                await conn.execute("INSERT INTO fixed_costs (label, amount) VALUES ($1, $2)", label, amount)
            await pool.close()
            await send_message_async(f"✅ Kostenpunkt *{label}* ({amount:.2f}€) hinzugefügt.")
        except Exception as e:
            await send_message_async(f"❌ Fehler: {e}")
    elif text.startswith("/del_cost"):
        try:
            parts = text.split()
            if len(parts) < 2:
                await send_message_async("ℹ️ Nutzung: `/del_cost ID` (Die ID findest du über `/costs`)")
                return
            cid = int(parts[1])
            pool = await DatabaseManager.create_pool(DB_URL)
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM fixed_costs WHERE id = $1", cid)
            await pool.close()
            await send_message_async(f"✅ Kostenpunkt ID {cid} gelöscht.")
        except Exception as e:
            await send_message_async(f"❌ Fehler: {e}")
    elif text == "/watchers":
        asyncio.create_task(run_watchers())
    elif text.startswith("/send_offers"):
        try:
            parts = text.split()
            percent = 5 # Default
            if len(parts) > 1:
                percent = int(parts[1])
            asyncio.create_task(run_send_offers(percent))
        except:
            await send_message_async("❌ Bitte gib eine Zahl ein, z.B. `/send_offers 10`")

async def main():
    logger.info("Telegram Control Bot gestartet...")
    
    # Sicherstellen, dass alle Tabellen existieren
    try:
        pool = await DatabaseManager.create_pool(DB_URL)
        await DatabaseManager.create_table(pool)
        await pool.close()
    except Exception as e:
        logger.error(f"Fehler bei Initial-DB-Check: {e}")

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

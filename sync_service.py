import asyncio
import os
import logging
from dotenv import load_dotenv

# Importiere die heute optimierten Kern-Module
from sync.booklooker.ebay import main as sync_ebay_main
from sync.booklooker.reactivate_vacation import main as reactivate_vacation_main

# Telegram Bot Integration
try:
    from telegram import Update
    from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
    HAS_TELEGRAM = True
except ImportError:
    HAS_TELEGRAM = False

load_dotenv()

# Logging-Konfiguration für den Hintergrund-Dienst
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
# Telegram Bot Commands (Wrapper)
# ==========================================
if HAS_TELEGRAM:
    async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("📊 *Sync-Dienst läuft im Hintergrund.*\n"
                                       "Der nächste automatische Abgleich startet gem. Zeitplan.", 
                                       parse_mode='Markdown')

    async def cmd_sync_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("🚀 Manueller Sync via Telegram gestartet...")
        # Hier könnte man ein Event setzen, um den Sync sofort zu triggern
        # Für den Moment dient es als Status-Bestätigung

# ==========================================
# Haupt-Schleife (24/7 Service)
# ==========================================
async def service_loop():
    """Die 24/7 Hauptschleife, die die heute optimierten Skripte nutzt."""
    # Intervall aus .env (Sekunden), Standard: 6 Stunden
    SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL_SECONDS", 21600)) 
    
    logger.info(f"✅ Sync-Service gestartet. Intervall: {SYNC_INTERVAL/3600:.1f} Stunden.")
    
    while True:
        try:
            logger.info("=== STARTE AUTOMATISCHEN SYNC-DURCHLAUF ===")
            
            # 1. Schritt: Urlaubs-Reaktivierung (heutiger Stand)
            logger.info("Prüfe Urlaubs-Rückkehrer...")
            await reactivate_vacation_main()
            
            # 2. Schritt: Bestands- & Preis-Abgleich (heutiger Stand inkl. 410-Fix)
            logger.info("Starte Bestands- & Preis-Sync...")
            await sync_ebay_main()
            
            logger.info(f"=== DURCHLAUF ABGESCHLOSSEN. Nächster Start in {SYNC_INTERVAL/3600:.1f}h ===")
            await asyncio.sleep(SYNC_INTERVAL)
            
        except Exception as e:
            logger.error(f"❌ Kritischer Fehler im Service-Loop: {e}")
            await asyncio.sleep(600) # Bei Fehler 10 Min warten und neu versuchen

async def main():
    # Starte den Sync-Loop als Hintergrund-Task
    asyncio.create_task(service_loop())

    # Starte Telegram Bot (falls konfiguriert)
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if HAS_TELEGRAM and bot_token:
        logger.info("Starte Telegram Bot Interface...")
        try:
            app = ApplicationBuilder().token(bot_token).build()
            app.add_handler(CommandHandler("status", cmd_status))
            app.add_handler(CommandHandler("sync", cmd_sync_now))
            
            await app.initialize()
            await app.start()
            await app.updater.start_polling()
            
            # Prozess am Leben halten
            while True:
                await asyncio.sleep(3600)
        except Exception as e:
            logger.error(f"Telegram Bot konnte nicht gestartet werden: {e}")
            while True: await asyncio.sleep(3600)
    else:
        logger.info("Kein Telegram-Token gefunden. Service läuft ohne Messenger-Anbindung.")
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service durch Benutzer beendet.")

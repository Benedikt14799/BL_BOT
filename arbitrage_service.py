import asyncio
import os
import logging
import aiohttp
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from dotenv import load_dotenv

from database import DatabaseManager
from price_processing import PriceProcessing

# Telegram Bot (stubbed)
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("arbitrage_service.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ArbitrageService")

# ==========================================
# Kern-Funktionen: eBay Fulfillment
# ==========================================

async def fetch_new_orders(session: aiohttp.ClientSession, token: str, base_url: str) -> list:
    """Holt neue/unbearbeitete Bestellungen von eBay (Fulfillment API)."""
    # Filtert nach noch nicht verschickten Bestellungen
    url = f"{base_url}/sell/fulfillment/v1/order?filter=orderfulfillmentstatus:{{NOT_STARTED|IN_PROGRESS}}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("orders", [])
            else:
                text = await resp.text()
                logger.error(f"eBay Fulfillment API Fehler {resp.status}: {text}")
                return []
    except Exception as e:
        logger.error(f"Fehler bei fetch_new_orders: {e}")
        return []

async def calculate_margin_2_0(ebay_sale_price: Decimal, purchase_price: Decimal, bl_shipping: Decimal) -> dict:
    """Implementiert Margin Calculator 2.0 (Gebühren + EK + Versand) exklusiv für echte Verkäufe."""
    fee_rate = PriceProcessing.EBAY_PERCENTAGE_FEE
    fee_fixed = PriceProcessing.EBAY_FIXED_FEE
    
    ebay_fees = (ebay_sale_price * fee_rate) + fee_fixed
    ebay_fees = ebay_fees.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    net_margin = ebay_sale_price - ebay_fees - purchase_price - bl_shipping
    
    return {
        "net_margin": float(net_margin.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
        "ebay_fees": float(ebay_fees)
    }

async def process_orders(db_pool, bot):
    """Prüft neue Bestellungen, loggt sie und sendet Telegram-Benachrichtigungen."""
    from ebay_token_manager import get_token
    EBAY_USER_TOKEN = get_token()
    EBAY_BASE_URL = os.environ.get("EBAY_BASE_URL", "https://api.ebay.com")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not EBAY_USER_TOKEN:
        logger.error("EBAY_USER_TOKEN fehlt. Polling abgebrochen.")
        return

    async with aiohttp.ClientSession() as session:
        orders = await fetch_new_orders(session, EBAY_USER_TOKEN, EBAY_BASE_URL)
        
        if not orders:
            logger.debug("Keine neuen Bestellungen gefunden.")
            return

        async with db_pool.acquire() as conn:
            for order in orders:
                order_id = order.get("orderId")
                
                # Check if already processed
                existing = await conn.fetchval("SELECT order_id FROM arbitrage_reporting WHERE order_id = $1", order_id)
                if existing:
                    continue

                # Parse order details
                line_items = order.get("lineItems", [])
                if not line_items:
                    continue
                    
                item = line_items[0] # Assume 1 item per order for now
                sku = item.get("sku")
                title = item.get("title")
                
                payment_summary = order.get("pricingSummary", {}).get("total", {})
                ebay_sale_price = Decimal(payment_summary.get("value", "0"))

                # Finde EK und BL-Versand in der DB
                # Zuerst in library suchen, falls durch Sync schon gelöscht in sold_listings
                bl_data = await conn.fetchrow("""
                    SELECT purchase_price, purchase_shipping, linktobl
                    FROM library WHERE sku = $1
                """, sku)

                bl_purchase_price = Decimal('0.00')
                bl_shipping_cost = Decimal('0.00')
                
                if bl_data:
                    bl_purchase_price = Decimal(str(bl_data.get("purchase_price", 0) or 0))
                    bl_shipping_cost = Decimal(str(bl_data.get("purchase_shipping", 0) or 0))
                
                # Berechne Marge 2.0
                margin_calc = await calculate_margin_2_0(ebay_sale_price, bl_purchase_price, bl_shipping_cost)
                net_margin = margin_calc["net_margin"]
                ebay_fee = margin_calc["ebay_fees"]
                
                # Speichern in arbitrage_reporting
                await conn.execute("""
                    INSERT INTO arbitrage_reporting 
                    (order_id, sku, title, ebay_sale_price, bl_purchase_price, bl_shipping_cost, ebay_fee, net_margin, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, order_id, sku, title, float(ebay_sale_price), float(bl_purchase_price), float(bl_shipping_cost), ebay_fee, net_margin, "pending_approval")
                
                logger.info(f"Neue Bestellung erkannt: {order_id} (SKU: {sku}) | Marge: {net_margin}€")
                
                # Telegram Benachrichtigung
                if chat_id and bot:
                    msg = (f"📦 *Neue eBay Bestellung!*\n\n"
                           f"📖 {title}\n"
                           f"🆔 eBay Order: `{order_id}`\n"
                           f"🔖 SKU: `{sku}`\n\n"
                           f"💰 *Verkaufspreis:* {ebay_sale_price} €\n"
                           f"🛒 *Booklooker EK+Vs:* {float(bl_purchase_price + bl_shipping_cost)} €\n"
                           f"💸 *Nettomarge:* {net_margin} €\n\n"
                           f"Soll ich bestellen?\n"
                           f"`/confirm {order_id}` oder `/skip {order_id}`")
                    await bot.send_message(chat_id=chat_id, text=msg, parse_mode='Markdown')

# ==========================================
# Hintergrund-Schleife
# ==========================================

async def polling_loop(db_pool, bot):
    """Die 24/7 Hauptschleife für das Bestell-Polling."""
    logger.info("Arbitrage-Polling gestartet (alle 5 Minuten).")
    while True:
        try:
            await process_orders(db_pool, bot)
        except Exception as e:
            logger.error(f"Fehler in der Polling-Schleife: {e}")
        
        # Alle 5 Minuten abfragen
        await asyncio.sleep(300)

# ==========================================
# Telegram Bot Commands
# ==========================================

async def cmd_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Bitte Order-ID angeben: `/confirm <order_id>`", parse_mode='Markdown')
        return
        
    order_id = context.args[0]
    
    # DB Fetch & Update
    db_url = os.environ.get("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    bl_url = ""
    
    async with pool.acquire() as conn:
        # 1. Finde SKU über order_id
        record = await conn.fetchrow("SELECT sku FROM arbitrage_reporting WHERE order_id = $1", order_id)
        if record:
            sku = record["sku"]
            # 2. Finde Booklooker URL in library (oder sold_listings)
            lib_rec = await conn.fetchrow("SELECT linktobl FROM library WHERE sku = $1", sku)
            if lib_rec and lib_rec["linktobl"]:
                bl_url = lib_rec["linktobl"]
            else:
                sold_rec = await conn.fetchrow("SELECT link FROM sold_listings WHERE sku = $1", sku)
                if sold_rec and sold_rec["link"]:
                    bl_url = sold_rec["link"]
                    
        await conn.execute("UPDATE arbitrage_reporting SET status = 'confirmed', updated_at = NOW() WHERE order_id = $1", order_id)
    await pool.close()

    await update.message.reply_text(f"⏳ Starte Booklooker Automatisierung für Order {order_id}...\n🔗 URL: {bl_url}")
    
    # Simulierter Playwright Call (später wird hier import booklooker_automator aufgerufen)
    # automator = BooklookerAutomator()
    # addr = {"name": eBay_Name, "street": eBay_Street, ...}
    # await automator.prepare_checkout(order_id, bl_url, addr)
    
    await asyncio.sleep(2)
    
    await update.message.reply_text(f"✅ Checkout für {order_id} erfolgreich vorbereitet (Demo)!")

async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Bitte Order-ID angeben: `/skip <order_id>`", parse_mode='Markdown')
        return
        
    order_id = context.args[0]
    
    # DB Update
    db_url = os.environ.get("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE arbitrage_reporting SET status = 'skipped', updated_at = NOW() WHERE order_id = $1", order_id)
    await pool.close()
    
    await update.message.reply_text(f"🛑 Bestellung {order_id} übersprungen/abgebrochen.")

async def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL fehlt!")
        return

    pool = await DatabaseManager.create_pool(db_url)
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    
    if bot_token:
        logger.info("Starte Telegram Bot für Arbitrage...")
        app = ApplicationBuilder().token(bot_token).build()
        
        # Commands
        app.add_handler(CommandHandler("confirm", cmd_confirm))
        app.add_handler(CommandHandler("skip", cmd_skip))
        
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        
        # Starte Polling im Hintergrund
        asyncio.create_task(polling_loop(pool, app.bot))
        
        # Halte den Prozess am Leben
        while True:
            await asyncio.sleep(3600)
    else:
        logger.warning("TELEGRAM_BOT_TOKEN fehlt. Polling läuft ohne Benachrichtigungen.")
        asyncio.create_task(polling_loop(pool, None))
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

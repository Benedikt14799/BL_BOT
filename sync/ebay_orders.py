import asyncio
import os
import sys
import logging
import aiohttp
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from decimal import Decimal
from dotenv import load_dotenv

# Projekt-Root in den Suchpfad legen
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database import DatabaseManager
from ebay_token_manager import get_token

# Logging Setup
logger = logging.getLogger("eBay-Orders")
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Google Sheets Setup
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]
CRED_FILE = os.path.join(PROJECT_ROOT, "credentials.json")
SHEET_NAME = "Sales eBay"

def _get_cost_params() -> dict:
    def to_dec(val, default):
        if not val: return Decimal(default)
        return Decimal(str(val).replace(",", "."))

    return {
        "fixed_costs": to_dec(os.getenv("FIXKOSTEN_MONATLICH"), "79.95"),
        "expected_sales": int(os.getenv("ERWARTETE_VERKAEUFE", "200")),
        "steuer_satz": to_dec(os.getenv("STEUERSATZ"), "7.0"),
        "addcost_low_mid": to_dec(os.getenv("ZUSATZKOSTEN_LOW_MID"), "0.50"),
        "addcost_high": to_dec(os.getenv("ZUSATZKOSTEN_HIGH"), "1.75"),
    }

async def fetch_recent_orders(session: aiohttp.ClientSession, token: str, base_url: str, hours_back: int = 48) -> list:
    """Holt die eBay-Bestellungen der letzten N Stunden über die Fulfillment API."""
    now = datetime.utcnow()
    past = now - timedelta(hours=hours_back)
    
    # ISO 8601 Format für eBay: YYYY-MM-DDTHH:MM:SS.000Z
    filter_date = past.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    url = f"{base_url}/sell/fulfillment/v1/order"
    params = {
        "filter": f"creationdate:[{filter_date}..]",
        "limit": "50"
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("orders", [])
            else:
                text = await resp.text()
                logger.error(f"Fehler beim Abruf der Orders ({resp.status}): {text}")
                return []
    except Exception as e:
        logger.error(f"Netzwerkfehler bei getOrders: {e}")
        return []

def estimate_ebay_fee(gross_revenue: Decimal, cost_params: dict) -> Decimal:
    """
    Schätzt die eBay-Gebühren. Normalerweise ~11% + 0.35€,
    wir nutzen hier die Logik aus dem Kostenmodell.
    """
    # Vereinfacht: Oft sind es bei Büchern 11-15% plus Fixum.
    # Da wir in der PriceProcessing Klasse exakt rechnen, machen wir hier eine gute Schätzung.
    # Oftmals 12% + 0.35€ als Daumenregel für Bücher, je nach Vertrag.
    fee = (gross_revenue * Decimal("0.12")) + Decimal("0.35")
    return round(fee, 2)

async def write_to_google_sheet(order_data: dict):
    """Schreibt die Bestelldaten in das Google Sheet."""
    if not os.path.exists(CRED_FILE):
        logger.warning("credentials.json nicht gefunden! Google Sheets Export übersprungen.")
        return False
        
    try:
        # Dies ist eine synchrone API (gspread blockiert), wir rufen es im Threadpool auf
        loop = asyncio.get_event_loop()
        def sync_write():
            creds = ServiceAccountCredentials.from_json_keyfile_name(CRED_FILE, SCOPE)
            client = gspread.authorize(creds)
            
            try:
                sheet = client.open(SHEET_NAME).sheet1
            except gspread.exceptions.SpreadsheetNotFound:
                logger.error(f"Google Sheet '{SHEET_NAME}' wurde nicht gefunden! Bitte prüfen, ob der Name stimmt und es geteilt wurde.")
                return False

            # Header schreiben, falls die erste Zeile keine Überschrift ist
            first_row = sheet.row_values(1)
            if not first_row or first_row[0] != "Order-ID":
                header = [
                    "Order-ID", "Datum", "Vorname", "Nachname", "Strasse", 
                    "PLZ", "Ort", "Land", "Titel", "Gewinn (EUR)", "Marge (%)", "Booklooker-Link"
                ]
                sheet.insert_row(header, 1)

            # Format: Order-ID, Datum, Vorname, Nachname, Str, PLZ, Ort, Land, Titel, Gewinn, Marge, Booklooker-Link
            row = [
                order_data["order_id"],
                order_data["creation_date"].strftime("%d.%m.%Y %H:%M"),
                order_data.get("first_name", ""),
                order_data.get("last_name", ""),
                order_data.get("street", ""),
                order_data.get("zip", ""),
                order_data.get("city", ""),
                order_data.get("country", ""),
                order_data.get("title", ""),
                str(order_data.get("net_profit", "0.00")).replace(".", ","),
                str(order_data.get("margin", "0.00")).replace(".", ","),
                order_data.get("linktobl", "")
            ]
            sheet.append_row(row)
            return True
            
        success = await loop.run_in_executor(None, sync_write)
        return success
    except Exception as e:
        logger.error(f"Kritischer Fehler beim Google Sheets Export: {e}")
        return False

async def process_orders():
    """Hauptlogik für den Bestellabruf und Verarbeitung."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url: return []
    
    pool = await DatabaseManager.create_pool(db_url)
    EBAY_BASE_URL = os.getenv("EBAY_BASE_URL", "https://api.ebay.com")
    token = get_token()
    cost_params = _get_cost_params()
    
    new_orders_count = 0
    total_profit_new = Decimal("0.00")
    notifications = []
    
    async with aiohttp.ClientSession() as session:
        orders = await fetch_recent_orders(session, token, EBAY_BASE_URL)
        logger.info(f"{len(orders)} Bestellungen im Zeitfenster gefunden.")
        
        for order in orders:
            order_id = order.get("orderId")
            
            # 1. Prüfen, ob schon vorhanden
            async with pool.acquire() as conn:
                exists = await conn.fetchval("SELECT 1 FROM ebay_orders WHERE order_id = $1", order_id)
                if exists:
                    continue # Bereits verarbeitet
            
            logger.info(f"Neue Bestellung gefunden: {order_id}")
            
            # 2. Relevante Daten extrahieren
            line_items = order.get("lineItems", [])
            if not line_items: continue
            
            item = line_items[0]
            sku = item.get("sku")
            title = item.get("title", "Unbekannt")
            legacy_item_id = item.get("legacyItemId", "")
            legacy_transaction_id = item.get("lineItemId", "")
            
            # Preis-Info
            pricing = order.get("pricingSummary", {})
            gross_revenue = Decimal(pricing.get("total", {}).get("value", "0.00"))
            
            # Käufer & Versand
            buyer_name = order.get("buyer", {}).get("username", "")
            shipping_info = {}
            instructions = order.get("fulfillmentStartInstructions", [])
            if instructions:
                ship_to = instructions[0].get("shippingStep", {}).get("shipTo", {})
                full_name = ship_to.get("fullName", "")
                contact = ship_to.get("contactAddress", {})
                
                # Namen aufsplitten und großschreiben
                parts = full_name.split(" ", 1)
                first_name = parts[0].strip().capitalize()
                last_name = parts[1].strip().capitalize() if len(parts) > 1 else ""
                
                # Straße und Ort ebenfalls großschreiben (Title Case)
                street = (str(contact.get("addressLine1", "")) + " " + str(contact.get("addressLine2", ""))).strip().title()
                city = str(contact.get("city", "")).strip().title()
                
                shipping_info = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "street": street,
                    "zip": contact.get("postalCode", ""),
                    "city": city,
                    "country": contact.get("countryCode", "")
                }
            
            # 3. Lokale Library matchen für EK & Link
            purchase_price = Decimal("0.00")
            purchase_shipping = Decimal("0.00")
            linktobl = ""
            
            async with pool.acquire() as conn:
                lib_row = await conn.fetchrow("""
                    SELECT purchase_price, purchase_shipping, linktobl 
                    FROM library WHERE sku = $1
                """, sku)
                
                if lib_row:
                    purchase_price = Decimal(str(lib_row.get("purchase_price") or 0))
                    purchase_shipping = Decimal(str(lib_row.get("purchase_shipping") or 0))
                    linktobl = lib_row.get("linktobl", "")
            
            # 4. Rentabilität / Marge berechnen (Präzise mit echten API-Gebühren)
            fee_obj = order.get("totalMarketplaceFee", {})
            actual_marketplace_fee = Decimal(str(fee_obj.get("value", "0.00")))
            
            is_ad_sale = any(it.get("properties", {}).get("soldViaAdCampaign") for it in line_items)
            ad_fee = Decimal("0.00")
            if is_ad_sale:
                ad_rate = Decimal(os.getenv("EBAY_AD_RATE", "2.0")) / 100
                ad_fee = gross_revenue * ad_rate * Decimal("1.19")
                ad_fee = round(ad_fee, 2)
                
            ebay_fee = actual_marketplace_fee + ad_fee
            
            # Netto-Erlös (nach echten Gebühren)
            net_revenue = gross_revenue - ebay_fee
            
            # Reingewinn
            net_profit = net_revenue - purchase_price - purchase_shipping
            
            # Marge in %
            margin_percent = Decimal("0.00")
            if purchase_price + purchase_shipping > 0:
                 margin_percent = (net_profit / (purchase_price + purchase_shipping)) * 100
                 margin_percent = round(margin_percent, 2)
            
            net_profit = round(net_profit, 2)
            
            # 5. Speichern in DB (ebay_orders Tabelle)
            cdate_str = order.get("creationDate")
            # 2023-10-01T12:00:00.000Z -> Format anpassen
            try:
                # Wir nehmen nur die ersten 19 Zeichen für YYYY-MM-DDTHH:MM:SS
                clean_date = cdate_str[:19]
                cdate = datetime.strptime(clean_date, "%Y-%m-%dT%H:%M:%S")
            except:
                cdate = datetime.now()
            
            async with pool.acquire() as conn:
                # Transaktion starten
                async with conn.transaction():
                    # In ebay_orders speichern
                    await conn.execute("""
                        INSERT INTO ebay_orders (
                            order_id, creation_date, sku, title, buyer_name, buyer_address,
                            gross_revenue, ebay_fee, purchase_price, purchase_shipping,
                            net_profit, margin, status
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    """, order_id, cdate, sku, title, buyer_name, str(shipping_info), 
                         gross_revenue, ebay_fee, purchase_price, purchase_shipping, net_profit, margin_percent, order.get("orderPaymentStatus", "PAID"))
                    
                    # In library updaten (Status 8 = sold_on_ebay)
                    await conn.execute("""
                        UPDATE library 
                        SET status_id = 8, 
                            ebay_listed = FALSE, 
                            gewinn_real = $2
                        WHERE sku = $1
                    """, sku, net_profit)
            
            # 6. Daten für Google Sheets und Telegram aufbereiten
            order_data = {
                "order_id": order_id,
                "creation_date": cdate,
                "title": title,
                "net_profit": net_profit,
                "margin": margin_percent,
                "linktobl": linktobl,
                **shipping_info
            }
            
            await write_to_google_sheet(order_data)
            
            new_orders_count += 1
            total_profit_new += net_profit
            
            # Links generieren
            ebay_details_url = f"https://www.ebay.de/sh/ord/details?orderid={order_id}"
            
            # Shoop 5% Cashback auf den reinen Buch-Einkaufspreis (Netto exkl. 7% MwSt)
            purchase_price_net = purchase_price / Decimal("1.07")
            cashback = purchase_price_net * Decimal("0.05")
            cashback = round(cashback, 2)
            
            # Gesamtprofit & neue Marge
            total_profit = net_profit + cashback
            new_margin_percent = Decimal("0.00")
            total_ek = purchase_price + purchase_shipping
            if total_ek > 0:
                new_margin_percent = (total_profit / total_ek) * 100
                new_margin_percent = round(new_margin_percent, 2)
            
            # Notification bauen (Emoji durch Text ersetzen für Windows Console logs, Telegram kann Emojis)
            msg = (
                f"🎉 *NEUER VERKAUF!*\n"
                f"📖 *Line:* {title}\n"
                f"💰 *Profit (Reingewinn):* {net_profit} Euro ({margin_percent}% Marge)\n"
                f"💸 *Cashback (Shoop 5%):* {cashback} Euro\n"
                f"📈 *Gesamtprofit:* {total_profit} Euro ({new_margin_percent}% Marge)\n"
                f"📢 *Werbekosten (eBay Ad):* {ad_fee} Euro\n"
                f"🔗 *Link:* [Booklooker-Link]({linktobl})\n"
                f"📋 *eBay-Details:* [Bestell-Details]({ebay_details_url})"
            )
            
            notifications.append(msg)
            
    await pool.close()
    return notifications

async def generate_daily_report(pool, hours_back: int = 24) -> str:
    """Erstellt einen formatierten Bericht über die Verkäufe der letzten N Stunden."""
    start_time = datetime.now() - timedelta(hours=hours_back)
    
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT title, gross_revenue, net_profit, margin 
            FROM ebay_orders 
            WHERE creation_date >= $1 
            ORDER BY creation_date DESC
        """, start_time)
        
        if not rows:
            return "Keine Verkäufe im gewählten Zeitraum gefunden."
        
        total_revenue = sum(Decimal(str(r["gross_revenue"])) for r in rows)
        total_profit = sum(Decimal(str(r["net_profit"])) for r in rows)
        count = len(rows)
        
        report = f"📊 *eBay Sales Report ({hours_back}h)*\n"
        report += f"━━━━━━━━━━━━━━━━━━━━\n"
        report += f"💰 *Gesamtumsatz:* {total_revenue:.2f}€\n"
        report += f"💵 *Reingewinn:* {total_profit:.2f}€\n"
        report += f"📦 *Anzahl:* {count} Bücher\n\n"
        
        report += "📖 *Verkaufte Titel:*\n"
        for r in rows:
            title_short = r["title"][:40] + "..." if len(r["title"]) > 40 else r["title"]
            report += f"• {title_short} | *+{r['net_profit']:.2f}€*\n"
            
        return report

async def test_run():
    # DB Init für Test
    db_url = os.getenv("DATABASE_URL")
    pool = await DatabaseManager.create_pool(db_url)
    try:
        notifications = await process_orders()
        for n in notifications:
             print(n.encode('ascii', 'ignore').decode('ascii'))
             
        report = await generate_daily_report(pool, 48)
        print("\n--- DAILY REPORT TEST ---\n")
        print(report.encode('ascii', 'ignore').decode('ascii'))
    except Exception as e:
        logger.error(f"Fehler im Testlauf: {e}")
    finally:
        await pool.close()

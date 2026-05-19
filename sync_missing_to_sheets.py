# sync_missing_to_sheets.py
import asyncio
import os
import sys
import logging
import ast
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Sync-Missing-Sheets")

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]
CRED_FILE = os.path.join(PROJECT_ROOT, "credentials.json")
SHEET_NAME = "Sales eBay"

async def main():
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=True)
    db_url = os.getenv("DATABASE_URL")
    
    if not os.path.exists(CRED_FILE):
        logger.error("credentials.json nicht gefunden! Kann nicht mit Google Sheets verbinden.")
        return
        
    pool = await DatabaseManager.create_pool(db_url)
    try:
        # 1. Alle Orders aus der DB laden mit BookLooker-Link falls vorhanden
        async with pool.acquire() as conn:
            db_orders = await conn.fetch("""
                SELECT o.*, l.linktobl 
                FROM ebay_orders o 
                LEFT JOIN library l ON o.sku = l.sku 
                ORDER BY o.creation_date ASC
            """)
            
        logger.info(f"{len(db_orders)} Bestellungen in der Datenbank gefunden.")
        
        # 2. Verbindung zu Google Sheets herstellen
        creds = ServiceAccountCredentials.from_json_keyfile_name(CRED_FILE, SCOPE)
        client = gspread.authorize(creds)
        
        try:
            sheet = client.open(SHEET_NAME).sheet1
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Google Sheet '{SHEET_NAME}' nicht gefunden!")
            return
            
        # 3. Vorhandene Order-IDs aus Spalte A holen
        existing_ids = set(sheet.col_values(1))
        logger.info(f"{len(existing_ids)} Zeilen im Google Sheet gefunden.")
        
        # 4. Fehlende hinzufügen
        added_count = 0
        for row in db_orders:
            order_id = row["order_id"]
            if order_id in existing_ids:
                continue
                
            logger.info(f"Synchronisiere fehlende Bestellung: {order_id} ({row['title']})")
            
            # Adresse parsen
            shipping_info = {}
            if row["buyer_address"]:
                try:
                    shipping_info = ast.literal_eval(row["buyer_address"])
                except Exception as e:
                    logger.warning(f"Konnte Adresse nicht parsen: {e}")
            
            street_val = shipping_info.get("street", "")
            match = re.match(r"^(.+?)\s*(\d+\s*[a-zA-Z]?(?:\s*-\s*\d+)?)$", street_val.strip())
            if match:
                street_name = match.group(1).strip()
                house_number = match.group(2).strip()
            else:
                street_name = street_val.strip()
                house_number = ""
                
            # Row zusammenstellen
            # Format: Order-ID, Datum, Vorname, Nachname, Strasse, Hausnummer, PLZ, Ort, Land, Titel, Gewinn, Marge, Booklooker-Link
            sheet_row = [
                order_id,
                row["creation_date"].strftime("%d.%m.%Y %H:%M"),
                shipping_info.get("first_name", ""),
                shipping_info.get("last_name", ""),
                street_name,
                house_number,
                shipping_info.get("zip", ""),
                shipping_info.get("city", ""),
                shipping_info.get("country", ""),
                row["title"],
                str(row.get("net_profit", "0.00")).replace(".", ","),
                str(row.get("margin", "0.00")).replace(".", ","),
                row.get("linktobl") or ""
            ]
            
            sheet.append_row(sheet_row)
            added_count += 1
            logger.info(f"✅ Hinzugefügt: {row['title']}")
            
        logger.info(f"Synchronisation beendet. {added_count} neue Zeilen hinzugefügt.")
        
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

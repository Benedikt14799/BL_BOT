import asyncio
import os
import logging
import aiohttp
from bs4 import BeautifulSoup
from database import DatabaseManager
from price_processing import PriceProcessing
import ebay_upload
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def fetch_bl_html(session: aiohttp.ClientSession, url: str) -> str:
    """Fetches the HTML of a Booklooker offer page."""
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                return await resp.text()
            else:
                logger.error(f"Failed to fetch BL URL {url}: {resp.status}")
                return ""
    except Exception as e:
        logger.error(f"Error fetching BL URL {url}: {e}")
        return ""

async def check_and_sync_price(session: aiohttp.ClientSession, db_pool, book: dict, token: str, base_url: str):
    """
    Checks if the price on Booklooker has changed and updates eBay if necessary.
    """
    internal_id = book['id']
    bl_url = book.get('linktobl') or book.get('link') # Support different col names if needed
    current_ebay_price = book.get('start_price')
    sku = book.get('isbn')
    
    if not bl_url or not sku:
        logger.warning(f"Book {internal_id} missing BL URL or SKU. Skipping.")
        return

    # 1. Fetch current BL state
    html = await fetch_bl_html(session, bl_url)
    if not html:
        return

    soup = BeautifulSoup(html, 'html.parser')
    
    # 2. Extract price/shipping using existing Logic
    new_ek = PriceProcessing._safe_clean_price(soup)
    new_shipping = PriceProcessing._safe_extract_shipping(soup)
    
    if new_ek == 0:
        logger.warning(f"Item {internal_id} seems SOLD OUT on Booklooker. Ending eBay listing...")
        # End listing on eBay
        withdraw_success = await ebay_upload.withdraw_offer(session, sku, token, base_url)
        
        if withdraw_success:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE library 
                    SET ebay_listed = FALSE, 
                        ebay_status = 'sold_out',
                        ebay_listing_id = NULL,
                        ebay_last_sync = NOW()
                    WHERE id = $1
                """, internal_id)
            logger.info(f"SUCCESS: eBay listing ended for sold out item {internal_id}")
        return

    # 3. Compute target eBay price
    target_ebay_price = PriceProcessing._compute_final_price(new_ek, new_shipping)
    
    if target_ebay_price is None:
        logger.error(f"Failed to compute target price for Book {internal_id}")
        return

    # 4. Compare with DB (decimal precision)
    if abs(target_ebay_price - current_ebay_price) > 0.01:
        logger.info(f"Price CHANGE detected for {internal_id}: {current_ebay_price} -> {target_ebay_price}")
        
        # 5. Update eBay
        success = await ebay_upload.update_inventory_price(session, sku, float(target_ebay_price), token, base_url)
        
        if success:
            # 6. Update DB
            # We re-calculate margin for the logs
            fee = PriceProcessing._fee_on_price(target_ebay_price)
            add_costs = PriceProcessing._additional_costs_for_price(target_ebay_price)
            new_margin = target_ebay_price - (new_ek + new_shipping + add_costs + fee)
            
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE library 
                    SET start_price = $1, 
                        margin = $2, 
                        purchase_price = $3, 
                        purchase_shipping = $4,
                        ebay_last_sync = NOW()
                    WHERE id = $5
                """, target_ebay_price, new_margin, new_ek, new_shipping, internal_id)
            logger.info(f"DB updated for {internal_id}")
        else:
            logger.error(f"Failed to update eBay price for {internal_id}")
    else:
        # Just update the sync timestamp
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE library SET ebay_last_sync = NOW() WHERE id = $1", internal_id)
        logger.info(f"Price for {internal_id} is still sync ({target_ebay_price})")

async def run_price_monitor(db_pool):
    """ Main loop for the price monitor. """
    EBAY_USER_TOKEN = os.environ.get("EBAY_USER_TOKEN")
    EBAY_BASE_URL = os.environ.get("EBAY_BASE_URL", "https://api.sandbox.ebay.com")
    
    if not EBAY_USER_TOKEN:
        logger.error("EBAY_USER_TOKEN missing. Monitor idle.")
        return

    logger.info("Starting Price Monitor iteration...")
    
    async with db_pool.acquire() as conn:
        # Get all listed books
        # column names check: id, title, start_price, isbn, linktobl or link
        books = await conn.fetch("""
            SELECT id, title, start_price, isbn, linktobl 
            FROM library 
            WHERE ebay_listed = TRUE AND ebay_listing_id IS NOT NULL
            ORDER BY ebay_last_sync ASC NULLS FIRST
            LIMIT 20
        """)
    
    if not books:
        logger.info("No listed books found to monitor.")
        return

    logger.info(f"Monitoring {len(books)} books...")
    
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        # Validate token first
        if not await ebay_upload.validate_token(session, EBAY_USER_TOKEN, EBAY_BASE_URL):
            logger.error("eBay Token expired. Cannot monitor prices.")
            return

        for b in books:
            await check_and_sync_price(session, db_pool, dict(b), EBAY_USER_TOKEN, EBAY_BASE_URL)
            # Be nice to BL
            await asyncio.sleep(2)

    logger.info("Price Monitor iteration finished.")

if __name__ == "__main__":
    async def main():
        db_url = os.environ.get("DATABASE_URL")
        pool = await DatabaseManager.create_pool(db_url)
        await run_price_monitor(pool)
        await pool.close()
    
    asyncio.run(main())

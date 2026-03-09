import json
import os
import logging
import asyncio
import aiohttp

logger = logging.getLogger(__name__)

upload_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent uploads to respect rate limits

async def validate_token(session: aiohttp.ClientSession, token: str, base_url: str) -> bool:
    """
    Checks if the token is still valid by making a simple metadata call.
    """
    url = f"{base_url}/sell/inventory/v1/inventory_item?limit=1"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                return True
            if resp.status == 401:
                logger.error("eBay Token validation failed: 401 Unauthorized.")
            return False
    except Exception as e:
        logger.error(f"Error validating eBay token: {e}")
        return False

async def get_unlisted_books(db_pool, limit: int = 50, specific_ids: list = None):
    """
    Fetches books that haven't been listed on eBay yet and have an ISBN (SKU).
    If specific_ids is provided, ONLY those IDs will be returned (ignoring limit).
    """
    async with db_pool.acquire() as conn:
        if specific_ids:
            query = """
                SELECT id, isbn, sku, title, autor, verlag as publisher, erscheinungsjahr, 
                       description, photo, start_price, condition_id,
                       sprache, ebay_error,
                       COALESCE(ebay_status, 'pending') as ebay_status
                FROM library 
                WHERE id = ANY($1::int[])
            """
            rows = await conn.fetch(query, specific_ids)
        else:
            # More permissive query: 
            # - Not listed (ebay_listed is false/null OR ebay_status is not 'listed')
            # - Has ISBN
            # - Include those with errors so they can be retried from GUI
            query = """
                SELECT id, isbn, sku, title, autor, verlag as publisher, erscheinungsjahr, 
                       description, photo, start_price, condition_id,
                       sprache, ebay_error,
                       COALESCE(ebay_status, 'pending') as ebay_status
                FROM library 
                WHERE (ebay_listed IS FALSE OR ebay_listed IS NULL)
                  AND (ebay_status IS NULL OR ebay_status != 'listed')
                  AND isbn IS NOT NULL 
                  AND LENGTH(isbn) > 5
                ORDER BY id DESC LIMIT $1
            """
            rows = await conn.fetch(query, limit)
            
    return [dict(r) for r in rows]


async def create_inventory_item(session: aiohttp.ClientSession, book_data: dict, token: str, base_url: str) -> bool:
    """
    Step 1: Create or replace Inventory Item.
    """
    url = f"{base_url}/sell/inventory/v1/inventory_item/{book_data['isbn']}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Content-Language": "de-DE",
        "Accept": "application/json"
    }

    # Map Condition
    # Booklooker mapping to eBay condition (Sandbox requires predefined ENUMS)
    # Default to USED_GOOD if parse fails
    condition = "LIKE_NEW"
    
    aspects = {}
    if book_data.get('sprache'):
        aspects['Sprache'] = [str(book_data['sprache'])[:65]]
    if book_data.get('autor'):
        aspects['Autor'] = [str(book_data['autor'])[:65]]
    if book_data.get('publisher'):
        aspects['Verlag'] = [str(book_data['publisher'])[:65]]
    if book_data.get('erscheinungsjahr'):
        aspects['Erscheinungsjahr'] = [str(book_data['erscheinungsjahr'])[:65]]
    if book_data.get('seitenanzahl'):
        aspects['Seitenanzahl'] = [str(book_data['seitenanzahl']).replace('S.', '').strip()[:65]]
    if book_data.get('title'):
        aspects['Buchtitel'] = [str(book_data['title'])[:65]]

    payload = {
        "product": {
            "title": str(book_data.get("title", ""))[:80],  # eBay max title length is 80
            "description": book_data.get("description", "Keine Beschreibung verfügbar."),
            "imageUrls": str(book_data['photo']).split('|') if book_data.get('photo') else [],
            "aspects": aspects,
            "isbn": [book_data['isbn']]
        },
        "condition": condition,
        "availability": {
            "shipToLocationAvailability": {
                "quantity": 1
            }
        }
    }

    async with session.put(url, headers=headers, json=payload) as resp:
        if resp.status in (200, 201, 204):
            return True
        else:
            resp_text = await resp.text()
            raise Exception(f"Inventory Item Error ({resp.status}): {resp_text}")


async def create_offer(session: aiohttp.ClientSession, book_data: dict, token: str, base_url: str, policies: dict) -> str:
    """
    Step 2: Create Offer and return Offer ID.
    Category 268 = Books
    """
    url = f"{base_url}/sell/inventory/v1/offer"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Content-Language": "de-DE",
        "Accept": "application/json"
    }

    price = book_data.get('start_price')
    if not price:
        raise Exception("Kein Preis vorhanden.")

    payload = {
        "sku": book_data['isbn'],
        "marketplaceId": "EBAY_DE",
        "format": "FIXED_PRICE",
        "merchantLocationKey": "DEFAULT",
        "pricingSummary": {
            "price": {
                "value": str(price),
                "currency": "EUR"
            }
        },
        "categoryId": "268",  # Standard-Buch Kategorie EBAY_DE
        "listingPolicies": {
            "fulfillmentPolicyId": policies['EBAY_FULFILLMENT_POLICY_ID'],
            "paymentPolicyId": policies['EBAY_PAYMENT_POLICY_ID'],
            "returnPolicyId": policies['EBAY_RETURN_POLICY_ID']
        }
    }

    async with session.post(url, headers=headers, json=payload) as resp:
        if resp.status in (200, 201):
            data = await resp.json()
            return data.get("offerId")
        else:
            resp_text = await resp.text()
            if resp.status == 400 and "already exists" in resp_text.lower():
                try:
                    error_data = json.loads(resp_text)
                    for err in error_data.get("errors", []):
                        for param in err.get("parameters", []):
                            if param.get("name") == "offerId":
                                offer_id = param.get("value")
                                # Altes Offer löschen, da Kategorie Updates per PUT ignoriert werden
                                delete_url = f"{base_url}/sell/inventory/v1/offer/{offer_id}"
                                async with session.delete(delete_url, headers=headers) as del_resp:
                                    if del_resp.status not in (200, 204):
                                        del_text = await del_resp.text()
                                        logger.warning(f"Konnte altes Offer nicht löschen: {del_resp.status} - {del_text}")
                                
                                # Offer komplett neu erstellen mit korrekter categoryId
                                async with session.post(url, headers=headers, json=payload) as post_resp:
                                    if post_resp.status in (200, 201):
                                        new_data = await post_resp.json()
                                        return new_data.get("offerId")
                                    else:
                                        post_text = await post_resp.text()
                                        raise Exception(f"Offer Neu-Erstellung Fehler ({post_resp.status}): {post_text}")
                except json.JSONDecodeError:
                    pass
            raise Exception(f"Offer Error ({resp.status}): {resp_text}")


async def publish_offer(session: aiohttp.ClientSession, offer_id: str, token: str, base_url: str) -> str:
    """
    Step 3: Publish Offer and return Listing ID.
    """
    url = f"{base_url}/sell/inventory/v1/offer/{offer_id}/publish/"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Content-Language": "de-DE",
        "Accept": "application/json"
    }

    async with session.post(url, headers=headers) as resp:
        if resp.status in (200, 201):
            data = await resp.json()
            return data.get("listingId")
        else:
            resp_text = await resp.text()
            raise Exception(f"Publish Error ({resp.status}): {resp_text}")


async def mark_as_listed(db_pool, internal_id: int, listing_id: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE library 
            SET ebay_listed = TRUE, ebay_listing_id = $1, ebay_error = NULL
            WHERE id = $2
        """, listing_id, internal_id)


async def mark_as_error(db_pool, internal_id: int, error_msg: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE library 
            SET ebay_error = $1
            WHERE id = $2
        """, error_msg, internal_id)


async def _process_single_book(session: aiohttp.ClientSession, book_data: dict, db_pool, token: str, base_url: str, policies: dict):
    internal_id = book_data['id']
    isbn = book_data['isbn']
    title = book_data.get('title', 'Unknown Title')
    
    try:
        async with upload_semaphore:
            logger.info(f"Uploading Item {internal_id} (ISBN: {isbn}) - {title[:30]}...")
            
            # Step 1
            await create_inventory_item(session, book_data, token, base_url)
            
            # Step 2
            offer_id = await create_offer(session, book_data, token, base_url, policies)
            if not offer_id:
                raise Exception("No Offer ID returned.")
            
            # Step 3
            listing_id = await publish_offer(session, offer_id, token, base_url)
            if not listing_id:
                raise Exception("No Listing ID returned.")
            
            # Update DB
            await mark_as_listed(db_pool, internal_id, listing_id)
            logger.info(f"SUCCESS: Item {internal_id} listed as {listing_id}")

    except Exception as e:
        error_str = str(e)
        logger.error(f"FAILED: Item {internal_id} - {error_str}")
        await mark_as_error(db_pool, internal_id, error_str)


async def run_upload_batch(db_pool, specific_ids: list = None):
    EBAY_USER_TOKEN = os.environ.get("EBAY_USER_TOKEN")
    EBAY_FULFILLMENT_POLICY_ID = os.environ.get("EBAY_FULFILLMENT_POLICY_ID")
    EBAY_PAYMENT_POLICY_ID = os.environ.get("EBAY_PAYMENT_POLICY_ID")
    EBAY_RETURN_POLICY_ID = os.environ.get("EBAY_RETURN_POLICY_ID")
    EBAY_BASE_URL = os.environ.get("EBAY_BASE_URL", "https://api.sandbox.ebay.com")

    if not EBAY_USER_TOKEN:
        logger.error("EBAY_USER_TOKEN is missing. Aborting eBay upload.")
        return
    if not all([EBAY_FULFILLMENT_POLICY_ID, EBAY_PAYMENT_POLICY_ID, EBAY_RETURN_POLICY_ID]):
        logger.error("eBay Policy IDs are missing. Aborting eBay upload.")
        return

    policies = {
        'EBAY_FULFILLMENT_POLICY_ID': EBAY_FULFILLMENT_POLICY_ID,
        'EBAY_PAYMENT_POLICY_ID': EBAY_PAYMENT_POLICY_ID,
        'EBAY_RETURN_POLICY_ID': EBAY_RETURN_POLICY_ID
    }

    async with aiohttp.ClientSession() as session:
        # Token Validation before starting
        if not await validate_token(session, EBAY_USER_TOKEN, EBAY_BASE_URL):
            logger.error("eBay Token ist ungültig oder abgelaufen! Bitte in der GUI (Settings) erneuern.")
            return

        logger.info("Starting eBay Upload Batch...")
        
        books_to_upload = await get_unlisted_books(db_pool, limit=5, specific_ids=specific_ids) # Sandbox testing batch slice
        
        if not books_to_upload:
            logger.info("No unlisted books found with valid ISBN.")
            return

        logger.info(f"Found {len(books_to_upload)} books to upload.")

        tasks = [
            asyncio.create_task(_process_single_book(session, book, db_pool, EBAY_USER_TOKEN, EBAY_BASE_URL, policies))
            for book in books_to_upload
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    logger.info("eBay Upload Batch Finished.")


async def update_inventory_price(session: aiohttp.ClientSession, sku: str, new_price: float, token: str, base_url: str) -> bool:
    """
    Updates ONLY the price of an existing offer/item using the bulk_update_price endpoint.
    """
    url_get_offers = f"{base_url}/sell/inventory/v1/offer?sku={sku}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    try:
        async with session.get(url_get_offers, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"Could not fetch offers for SKU {sku}: {resp.status}")
                return False
            data = await resp.json()
            offers = data.get('offers', [])
            if not offers:
                logger.warning(f"No active offers found for SKU {sku} on eBay.")
                return False
            
            success = True
            for offer in offers:
                offer_id = offer['offerId']
                bulk_url = f"{base_url}/sell/inventory/v1/bulk_update_price"
                bulk_payload = {
                    "requests": [
                        {
                            "offerId": offer_id,
                            "price": {
                                "value": str(new_price),
                                "currency": "EUR"
                            }
                        }
                    ]
                }
                async with session.post(bulk_url, headers=headers, json=bulk_payload) as bulk_resp:
                    if bulk_resp.status not in (200, 204):
                        logger.error(f"Price update failed for Offer {offer_id}: {bulk_resp.status}")
                        success = False
                    else:
                        logger.info(f"Price updated to {new_price} for SKU {sku} (Offer {offer_id})")
            
            return success
    except Exception as e:
        logger.error(f"Error updating price for SKU {sku}: {e}")
        return False


async def withdraw_offer(session: aiohttp.ClientSession, sku: str, token: str, base_url: str) -> bool:
    """
    Step 1: Find the offerId for the given SKU.
    Step 2: Withdraw the offer (Ends the listing on eBay).
    """
    url_get_offers = f"{base_url}/sell/inventory/v1/offer?sku={sku}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        async with session.get(url_get_offers, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"Could not fetch offers for SKU {sku} to withdraw: {resp.status}")
                return False
            
            data = await resp.json()
            offers = data.get('offers', [])
            if not offers:
                logger.warning(f"No active offers found for SKU {sku} to withdraw.")
                return True # Technically "done" if no offer exists

            success = True
            for offer in offers:
                offer_id = offer['offerId']
                withdraw_url = f"{base_url}/sell/inventory/v1/offer/{offer_id}/withdraw"
                
                async with session.post(withdraw_url, headers=headers) as w_resp:
                    if w_resp.status in (200, 204):
                        logger.info(f"Successfully withdrawn offer {offer_id} for SKU {sku}")
                    else:
                        logger.error(f"Failed to withdraw offer {offer_id}: {w_resp.status}")
                        success = False
            
            return success
    except Exception as e:
        logger.error(f"Error in withdraw_offer for SKU {sku}: {e}")
        return False

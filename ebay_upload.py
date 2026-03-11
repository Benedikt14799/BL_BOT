import re
import json
import os
import logging
import asyncio
import aiohttp
from decimal import Decimal
from datetime import datetime, timedelta
from ebay_template import generate_description, get_condition_metadata
from description_filter import filter_description

logger = logging.getLogger(__name__)

upload_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent uploads to respect rate limits

def map_ebay_condition(bl_condition: str) -> str:
    """
    Maps Booklooker condition strings to eBay Inventory API condition enums.
    Valid for Books category (268): NEW, USED_VERY_GOOD, USED_GOOD, USED_ACCEPTABLE.
    """
    if not bl_condition:
        return "USED_GOOD"
    c = str(bl_condition).lower()
    if "neu" in c and "wie neu" not in c:
        return "NEW"
    if "wie neu" in c:
        return "USED_VERY_GOOD"
    if "sehr gut" in c:
        return "USED_VERY_GOOD"
    if any(x in c for x in ["leichte gebrauchsspuren", "gut"]):
        return "USED_GOOD"
    if any(x in c for x in ["deutliche gebrauchsspuren", "akzeptabel", "stark"]):
        return "USED_ACCEPTABLE"
    return "USED_GOOD"

def strip_html(text: str) -> str:
    """Entfernt HTML-Tags sowie den Inhalt von Style- und Script-Tags."""
    if not text: return ""
    # Entferne <style>...</style> und <script>...</script> samt Inhalt
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', text, flags=re.DOTALL | re.IGNORECASE)
    # Entferne alle restlichen Tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Konsolidiere Whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

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
                       description, photo, start_price, condition_id, bl_condition,
                       best_offer_auto_accept_price, minimum_best_offer_price,
                       sprache, seitenanzahl, thematik, buchreihe, genre, cformat,
                       originalsprache, produktart, literarische_gattung, zielgruppe,
                       signiert_von, literarische_bewegung, ausgabe, ebay_error,
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
                       description, photo, start_price, condition_id, bl_condition,
                       best_offer_auto_accept_price, minimum_best_offer_price,
                       sprache, seitenanzahl, thematik, buchreihe, genre, cformat,
                       originalsprache, produktart, literarische_gattung, zielgruppe,
                       signiert_von, literarische_bewegung, ausgabe, ebay_error,
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
    condition = map_ebay_condition(book_data.get('bl_condition'))
    
    aspects = {}
    if book_data.get('sprache'):
        aspects['Sprache'] = [str(book_data['sprache'])[:65]]
    
    # [BUG E] Autor ist Pflichtfeld für Bücher (268)
    author_raw = book_data.get('autor') or ''
    aspects['Autor'] = [str(author_raw).strip()[:65] if author_raw else "Unbekannt"]

    if book_data.get('publisher'):
        aspects['Verlag'] = [str(book_data['publisher'])[:65]]
    if book_data.get('erscheinungsjahr'):
        aspects['Erscheinungsjahr'] = [str(book_data['erscheinungsjahr'])[:65]]
    if book_data.get('seitenanzahl'):
        aspects['Seitenanzahl'] = [str(book_data['seitenanzahl']).replace('S.', '').strip()[:65]]
    if book_data.get('title'):
        aspects['Buchtitel'] = [str(book_data['title'])[:65]]
    if book_data.get('thematik'):
        aspects['Thematik'] = [str(book_data['thematik'])[:65]]
    if book_data.get('buchreihe'):
        aspects['Buchreihe'] = [str(book_data['buchreihe'])[:65]]
    if book_data.get('genre'):
        aspects['Genre'] = [str(book_data['genre'])[:65]]
    if book_data.get('cformat'):
        aspects['Format'] = [str(book_data['cformat'])[:65]]
    if book_data.get('originalsprache'):
        aspects['Originalsprache'] = [str(book_data['originalsprache'])[:65]]
    if book_data.get('produktart'):
        aspects['Produktart'] = [str(book_data['produktart'])[:65]]
    if book_data.get('literarische_gattung'):
        aspects['Literarische Gattung'] = [str(book_data['literarische_gattung'])[:65]]
    if book_data.get('zielgruppe'):
        aspects['Zielgruppe'] = [str(book_data['zielgruppe'])[:65]]
    if book_data.get('signiert_von'):
        aspects['Signiert von'] = [str(book_data['signiert_von'])[:65]]
    if book_data.get('literarische_bewegung'):
        aspects['Literarische Bewegung'] = [str(book_data['literarische_bewegung'])[:65]]
    if book_data.get('ausgabe'):
        aspects['Ausgabe'] = [str(book_data['ausgabe'])[:65]]

    # 1. Condition-Metadaten für Template bestimmen
    cond_meta = get_condition_metadata(book_data.get('bl_condition'))
    
    # 2. HTML-Beschreibung mit Template generieren
    template_data = {
        'title': book_data.get('title', 'Unbekannter Titel'),
        'author': book_data.get('autor', 'Unbekannt'),
        'publisher': book_data.get('publisher', 'Unbekannter Verlag'),
        'language': book_data.get('sprache', 'Deutsch'),
        'condition': cond_meta['text'],
        'condition_color': cond_meta['color'],
        'extra_notes': filter_description(book_data.get('description', '')),
        'shipping_cost': os.environ.get("SHIPPING_DESCRIPTION_EBAY", "Standardversand"),
        'delivery_time': os.environ.get("DELIVERY_TIME_EBAY", "1-3 Werktage")
    }
    html_description = generate_description(template_data)

    # Condition Description nur bei gebrauchten Artikeln (nicht bei NEW)
    condition_desc = None
    if condition != "NEW":
        bl_cond = (book_data.get('bl_condition') or '').strip()
        # "None" als String abfangen
        if bl_cond.lower() == 'none': bl_cond = ''

        # HTML entfernen aus den internen Notizen
        clean_notes = filter_description(strip_html(book_data.get('description') or ''))
        
        if bl_cond and clean_notes:
            raw_desc = f"Zustand: {bl_cond}. {clean_notes}"
        elif bl_cond:
            raw_desc = f"Zustand: {bl_cond}"
        else:
            raw_desc = clean_notes
            
        # Finale Bereinigung (doppelte Punkte etc.) und Kürzung auf 1000 Zeichen
        condition_desc = re.sub(r'\.\s*\.', '.', raw_desc).strip()
        # [BUG 4 Fix] Falls es nur ein Punkt ist oder mit Zustand: . anfängt
        condition_desc = re.sub(r'^Zustand\s*:\s*\.?\s*', 'Zustand: ', condition_desc)
        if condition_desc in ("Zustand:", "Zustand: .", "."):
            condition_desc = ""
        
        condition_desc = condition_desc[:1000].strip()
        if not condition_desc:
            condition_desc = None # Ganz weglassen wenn leer

    # Robustes Image-URL Parsing (filtert leere oder "None" Strings)
    raw_photos = book_data.get('photo') or ''
    image_urls = [
        url.strip()
        for url in str(raw_photos).split('|')
        if url.strip() and url.strip().lower() != 'none'
    ]

    # [BUG A] Description auf 4000 Zeichen begrenzen (Safety Limit für eBay)
    final_desc = html_description
    if len(final_desc) > 4000:
        logger.warning(f"Description still too long ({len(final_desc)} chars). Using compact fallback.")
        # Wenn immer noch > 4000, bauen wir eine radikal gekürzte Version mit Basis-HTML
        author_text = f" von {book_data.get('autor')}" if book_data.get('autor') else ""
        compact_html = (
            f"<div style='font-family:sans-serif;padding:15px;line-height:1.5;'>"
            f"<h1 style='font-size:18px;color:#0053a0;'>{book_data.get('title', 'Buch')}</h1>"
            f"<p><b>Autor:</b> {book_data.get('autor', 'Unbekannt')}<br>"
            f"<b>Verlag:</b> {book_data.get('publisher', 'Unbekannt')}</p>"
            f"<hr style='border:none;border-top:1px solid #eee;margin:15px 0;'>"
            f"<p style='font-style:italic;'>{clean_notes[:3000]}</p>"
            f"<p style='font-size:12px;color:#888;margin-top:20px;'>Vielen Dank für Ihren Einkauf!</p>"
            f"</div>"
        )
        final_desc = compact_html[:3990]

    payload = {
        "product": {
            "title": str(book_data.get("title", ""))[:80],  # eBay max title length is 80
            "description": final_desc,
            "imageUrls": image_urls,
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
    
    if condition_desc:
        payload["conditionDescription"] = condition_desc

    # Debug-Logging für Payload (hilft bei Serialization Errors)
    logger.info(f"Inventory Item Payload für ISBN {book_data['isbn']}: {json.dumps(payload)}")

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

    # Streichpreis-Logik (STP)
    # Nur für Normalpreis-Artikel (10€ - 50€)
    pricing_summary = {
        "price": {
            "value": str(price),
            "currency": "EUR"
        }
    }
    
    price_dec = Decimal(str(price))
    if Decimal('10.00') <= price_dec <= Decimal('50.00'):
        original_p = (price_dec * Decimal('1.25')).quantize(Decimal('0.01'))
        pricing_summary["originalRetailPrice"] = {
            "value": str(original_p),
            "currency": "EUR"
        }

    payload = {
        "sku": book_data['isbn'],
        "marketplaceId": "EBAY_DE",
        "format": "FIXED_PRICE",
        "merchantLocationKey": "hauptlager",
        "pricingSummary": pricing_summary,
        "tax": {
            "vatPercentage": 7.0
        },
        "categoryId": "268",  # Standard-Buch Kategorie EBAY_DE
        "listingPolicies": {
            "fulfillmentPolicyId": policies['EBAY_FULFILLMENT_POLICY_ID'],
            "paymentPolicyId": policies['EBAY_PAYMENT_POLICY_ID'],
            "returnPolicyId": policies['EBAY_RETURN_POLICY_ID']
        }
    }

    # Best Offer Logik (5% Rabatt-Stufen) hinzufügen, falls vorhanden
    auto_accept = book_data.get('best_offer_auto_accept_price')
    min_offer = book_data.get('minimum_best_offer_price')
    
    if auto_accept and min_offer:
        payload["bestOfferTerms"] = {
            "bestOfferEnabled": True,
            "autoAcceptPrice": {
                "value": str(auto_accept),
                "currency": "EUR"
            },
            "autoDeclinePrice": {
                "value": str(min_offer),
                "currency": "EUR"
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


async def ensure_volume_pricing_promotion(session: aiohttp.ClientSession, token: str, base_url: str):
    """
    Erstellt eine globale Marketing-Promotion für 5% Mengenrabatt ab 2 Artikeln in der Buch-Kategorie (268).
    Nutzt die eBay Marketing API.
    """
    url = f"{base_url}/sell/marketing/v1/item_promotion"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # Wir prüfen nicht erst, ob sie existiert, sondern versuchen sie zu erstellen.
    # Falls sie existiert, gibt eBay meist einen 409 Conflict oder 400 mit Detail-Fehler zurück.
    
    start_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_date = (datetime.utcnow() + timedelta(days=365*2)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    payload = {
        "name": "Mengenrabatt 5% ab 2 Büchern",
        "description": "Spare 5% beim Kauf von 2 oder mehr Artikeln aus der Kategorie Bücher.",
        "marketplaceId": "EBAY_DE",
        "startDate": start_date,
        "endDate": end_date,
        "promotionStatus": "ENABLED",
        "discountRules": [
            {
                "discountBenefit": {
                    "percentage": "5"
                },
                "numberOfItems": 2
            }
        ],
        "inventoryCriterion": {
            "inventoryCriterionType": "INVENTORY_BY_RULE",
            "ruleCriteria": {
                "selectionRules": [
                    {
                        "categoryIds": ["268"],
                        "categoryScope": "MARKETPLACE"
                    }
                ]
            }
        }
    }

    try:
        async with session.post(url, headers=headers, json=payload) as resp:
            if resp.status in (200, 201):
                logger.info("Mengenrabatt-Promotion erfolgreich erstellt.")
            elif resp.status == 409:
                logger.info("Mengenrabatt-Promotion existiert bereits (409 Conflict).")
            else:
                resp_text = await resp.text()
                # Wir loggen es nur als Info, da der Upload nicht abbrechen soll, falls Marketing API zickt
                logger.info(f"Marketing API Hinweis ({resp.status}): {resp_text}")
    except Exception as e:
        logger.warning(f"Fehler beim Erstellen der Mengenrabatt-Promotion: {e}")


async def run_upload_batch(db_pool, specific_ids: list = None):
    from ebay_token_manager import get_token
    EBAY_USER_TOKEN = get_token()
    EBAY_FULFILLMENT_POLICY_ID = os.environ.get("EBAY_FULFILLMENT_POLICY_ID")
    EBAY_PAYMENT_POLICY_ID = os.environ.get("EBAY_PAYMENT_POLICY_ID")
    EBAY_RETURN_POLICY_ID = os.environ.get("EBAY_RETURN_POLICY_ID")
    # Default safely to sandbox, but warn if we expect production
    EBAY_BASE_URL = os.environ.get("EBAY_BASE_URL", "https://api.sandbox.ebay.com")
    
    if "sandbox" not in EBAY_BASE_URL.lower():
        logger.info("PRODUCTION MODE: Uploading to real eBay Marketplace.")
    else:
        logger.info("SANDBOX MODE: Uploading to eBay Sandbox.")

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
        
        # 0. Sicherstellen, dass die globale Mengenrabatt-Promotion aktiv ist (5% ab 2 Artikeln)
        # await ensure_volume_pricing_promotion(session, EBAY_USER_TOKEN, EBAY_BASE_URL)
        
        books = await get_unlisted_books(db_pool, specific_ids=specific_ids)
        
        if not books:
            logger.info("No unlisted books found with valid ISBN.")
            return

        logger.info(f"Found {len(books)} books to upload.")

        tasks = [
            asyncio.create_task(_process_single_book(session, book, db_pool, EBAY_USER_TOKEN, EBAY_BASE_URL, policies))
            for book in books
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


async def run_inventory_reconciliation(db_pool) -> dict:
    """
    Vergleicht die lokalen DB-Einträge mit den tatsächlich auf eBay aktiven Inseraten (Trading API).
    Listings, deren ebay_listing_id nicht mehr auf eBay aktiv ist (aber in der DB noch ebay_listed=True sind),
    werden in der DB auf ebay_listed=False gesetzt.
    Rückgabe: Statistik-Dict
    """
    import xml.etree.ElementTree as ET
    from ebay_token_manager import get_token
    
    token = get_token()
    env = os.environ.get("EBAY_ENV", "SANDBOX")
    
    if env == "PRODUCTION":
        endpoint = "https://api.ebay.com/ws/api.dll"
    else:
        endpoint = "https://api.sandbox.ebay.com/ws/api.dll"
    
    if not token:
        logger.error("Kein eBay-Token für den Bestandsabgleich verfügbar.")
        return {"error": "Kein eBay-Token vorhanden."}

    logger.info("Starte eBay-Bestandsabgleich via Trading API (Fetching active ItemIDs)...")
    
    active_ebay_item_ids = set()
    page_number = 1
    entries_per_page = 200
    has_more = True
    
    headers = {
        "X-EBAY-API-SITEID": "77", # Germany
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml"
    }

    async with aiohttp.ClientSession() as session:
        # 1. Alle aktiven ItemIDs von eBay holen (Paginierung)
        while has_more:
            # Wichtig: <ActiveList> anstatt <ActiveListings>, sonst gibt eBay eine leere Liste!
            request_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <DetailLevel>ReturnAll</DetailLevel>
  <ActiveList>
    <Pagination>
      <EntriesPerPage>{entries_per_page}</EntriesPerPage>
      <PageNumber>{page_number}</PageNumber>
    </Pagination>
  </ActiveList>
</GetMyeBaySellingRequest>
"""
            try:
                async with session.post(endpoint, headers=headers, data=request_xml) as resp:
                    if resp.status != 200:
                        resp_text = await resp.text()
                        logger.error(f"Fehler beim eBay Trading API Fetch ({resp.status}): {resp_text}")
                        return {"error": f"API Fehler: {resp.status}"}
                    
                    xml_text = await resp.text()
                    root = ET.fromstring(xml_text)
                    
                    # XML Namespace handling
                    ns = {'ebay': 'urn:ebay:apis:eBLBaseComponents'}
                    
                    ack = root.find('.//ebay:Ack', ns)
                    if ack is not None and ack.text not in ['Success', 'Warning']:
                        err_msg = root.find('.//ebay:Errors/ebay:LongMessage', ns)
                        err_text = err_msg.text if err_msg is not None else "Unknown API Error"
                        logger.error(f"Trading API Error: {err_text}")
                        return {"error": err_text}

                    active_list = root.find('.//ebay:ActiveList', ns)
                    if active_list is not None:
                        item_array = active_list.find('ebay:ItemArray', ns)
                        if item_array is not None:
                            for item in item_array.findall('ebay:Item', ns):
                                item_id = item.find('ebay:ItemID', ns)
                                if item_id is not None and item_id.text:
                                    active_ebay_item_ids.add(item_id.text)
                        
                        pagination_result = active_list.find('ebay:PaginationResult', ns)
                        if pagination_result is not None:
                            total_pages = pagination_result.find('ebay:TotalNumberOfPages', ns)
                            if total_pages is not None and int(total_pages.text) > page_number:
                                page_number += 1
                            else:
                                has_more = False
                        else:
                            has_more = False
                    else:
                        has_more = False

            except Exception as e:
                logger.error(f"Exception beim Abrufen der eBay ActiveList: {e}")
                return {"error": str(e)}

    # 2. Lokale DB abgleichen
    stats = {
        "total_ebay": len(active_ebay_item_ids),
        "total_db": 0,
        "corrected": 0,
        "ebay_only": 0
    }
    
    try:
        async with db_pool.acquire() as conn:
            # Hole alle Einträge, die laut DB auf eBay gelistet sind (ebay_listing_id wird hierfür benötigt)
            db_listed = await conn.fetch("SELECT id, ebay_listing_id FROM library WHERE ebay_listed = TRUE AND ebay_listing_id IS NOT NULL")
            stats["total_db"] = len(db_listed)
            
            db_item_ids = {row["ebay_listing_id"] for row in db_listed}
            
            # DB-Einträge, deren ItemID NICHT aktiv auf eBay ist:
            to_delist_ids = []
            for row in db_listed:
                if row["ebay_listing_id"] not in active_ebay_item_ids:
                    to_delist_ids.append(row["id"])
                    
            if to_delist_ids:
                # Markiere sie in der DB als NICHT mehr gelistet
                await conn.execute("""
                    UPDATE library 
                    SET ebay_listed = FALSE, 
                        ebay_delisted_reason = 'Reconciliation: ItemID not found in active eBay listings',
                        ebay_status = 'pending'
                    WHERE id = ANY($1::int[])
                """, to_delist_ids)
                
                stats["corrected"] = len(to_delist_ids)
                logger.info(f"Habe {len(to_delist_ids)} Artikel lokal delisted, da sie auf eBay nicht mehr aktiv sind.")
            
            # (Optional) Zählen, wie viele SKUs auf eBay sind, aber lokal NICHT gelistet sind
            ebay_only_item_ids = active_ebay_item_ids - db_item_ids
            stats["ebay_only"] = len(ebay_only_item_ids)
            
    except Exception as e:
        logger.error(f"Fehler beim Datenbank-Abgleich: {e}")
        return {"error": str(e)}
        
    logger.info(f"Bestandsabgleich abgeschlossen: eBay({stats['total_ebay']}), DB({stats['total_db']}), Korrigiert({stats['corrected']})")
    return stats

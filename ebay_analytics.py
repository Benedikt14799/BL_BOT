import logging
import aiohttp
import json
import os
import base64
from datetime import datetime

logger = logging.getLogger(__name__)

async def get_application_token(session: aiohttp.ClientSession) -> str:
    """
    Holt einen OAuth 2.0 Application Token via Client Credentials Grant.
    Wird für die Analytics API benötigt.
    """
    app_id = os.environ.get("EBAY_APP_ID", "").strip().strip("'").strip('"').strip()
    cert_id = os.environ.get("EBAY_CERT_ID", "").strip().strip("'").strip('"').strip()
    env = os.environ.get("EBAY_ENV", "PRODUCTION").upper().strip().strip("'").strip('"').strip()
    
    if not app_id or not cert_id:
        raise Exception("EBAY_APP_ID oder EBAY_CERT_ID fehlt in der .env")
        
    # URL bestimmen
    auth_url = "https://api.ebay.com/identity/v1/oauth2/token"
    if env == "SANDBOX":
        auth_url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
        
    # Credentials base64 kodieren
    auth_str = f"{app_id}:{cert_id}"
    encoded_auth = base64.b64encode(auth_str.encode()).decode()
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_auth}"
    }
    
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }
    
    async with session.post(auth_url, headers=headers, data=data) as resp:
        if resp.status == 200:
            res_data = await resp.json()
            return res_data.get("access_token")
        else:
            err_text = await resp.text()
            raise Exception(f"OAuth Fehler ({resp.status}): {err_text}")

async def get_rate_limit_status(session: aiohttp.ClientSession) -> dict:
    """
    Fragt den Rate Limit Status der eBay API ab.
    Gibt ein Dict mit den relevanten Werten zurück.
    """
    env = os.environ.get("EBAY_ENV", "PRODUCTION").upper()
    base_url = "https://api.ebay.com"
    if env == "SANDBOX":
        base_url = "https://api.sandbox.ebay.com"
        
    url = f"{base_url}/developer/analytics/v1_beta/rate_limit/"
    
    # 1. Token holen
    token = await get_application_token(session)
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            return parse_rate_limit_response(data)
        else:
            err_text = await resp.text()
            raise Exception(f"Analytics API Fehler ({resp.status}): {err_text}")

def parse_rate_limit_response(data: dict) -> dict:
    """
    Extrahiert die relevanten API-Limits. Sichert sowohl das Limit für Uploads (Sell)
    als auch für den Konkurrenzcheck (Buy).
    """
    result = {
        "sell": {"limit": 0, "remaining": 0, "used": 0, "reset": "Unbekannt"},
        "buy": {"limit": 0, "remaining": 0, "used": 0, "reset": "Unbekannt"}
    }
    
    try:
        buy_target = "buy.browse"
        sell_target = "sell.inventory"
        sell_fallback = "AddFixedPriceItem"
        
        sell_found = False
        
        for context in data.get("rateLimits", []):
            for resource in context.get("resources", []):
                res_name = resource.get("name", "")
                
                for rate in resource.get("rates", []):
                    limit = rate.get("limit", 0)
                    if limit > 0:
                        parsed_rate = {
                            "limit": limit,
                            "remaining": rate.get("remaining", 0),
                            "used": limit - rate.get("remaining", 0),
                            "reset": rate.get("reset")
                        }
                        
                        reset_str = parsed_rate["reset"]
                        if reset_str:
                            try:
                                dt = datetime.fromisoformat(reset_str.replace("Z", "+00:00"))
                                parsed_rate["reset"] = dt.strftime("%d.%m.%Y, %H:%M Uhr UTC")
                            except:
                                parsed_rate["reset"] = reset_str
                                
                        # Buy/Browse API (Konkurrenzcheck)
                        if res_name == buy_target:
                            result["buy"] = parsed_rate
                            
                        # Sell/Inventory API (Uploads)
                        if res_name == sell_target:
                            result["sell"] = parsed_rate
                            sell_found = True
                            
                        # Fallback für Uploads, falls sell.inventory fehlt
                        if res_name == sell_fallback and not sell_found:
                            result["sell"] = parsed_rate

        return result
    except Exception as e:
        logger.error(f"Fehler beim Parsen der Rate Limit Response: {e}")
        
    return result

async def has_sufficient_quota(session: aiohttp.ClientSession, min_required: int = 10) -> tuple[bool, int, str]:
    """
    Prüft, ob genügend Kontingent für die Buy/Browse API übrig ist.
    Gibt (bool_erfolg, verbleibend, reset_zeit) zurück.
    """
    try:
        status = await get_rate_limit_status(session)
        buy = status.get("buy", {})
        remaining = buy.get("remaining", 0)
        reset = buy.get("reset", "Unbekannt")
        return (remaining >= min_required), remaining, reset
    except Exception as e:
        logger.error(f"Fehler bei Quoten-Prüfung: {e}")
        # Im Fehlerfall lieber pausieren oder weitermachen? 
        # Da wir kein Risiko gehen wollen: False
        return False, 0, "Fehler bei Abfrage"

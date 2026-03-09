import os
import json
import asyncio
import aiohttp
import logging
from dotenv import load_dotenv, set_key

# Logging konfigurieren
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Lade Umgebungsvariablen
ENV_FILE = ".env"
load_dotenv(ENV_FILE)

EBAY_USER_TOKEN = os.environ.get("EBAY_USER_TOKEN")
EBAY_BASE_URL = os.environ.get("EBAY_BASE_URL", "https://api.sandbox.ebay.com")
MARKETPLACE_ID = "EBAY_DE"

if not EBAY_USER_TOKEN:
    logger.error("EBAY_USER_TOKEN ist nicht in der .env-Datei gesetzt.")
    exit(1)

HEADERS = {
    "Authorization": f"Bearer {EBAY_USER_TOKEN}",
    "Content-Type": "application/json",
    "Content-Language": "de-DE",
    "Accept": "application/json"
}

POLICIES = {
    "fulfillment": {
        "endpoint": "/sell/account/v1/fulfillment_policy",
        "env_key": "EBAY_FULFILLMENT_POLICY_ID",
        "name_key": "fulfillmentPolicies",
        "id_key": "fulfillmentPolicyId",
        "payload": {
            "name": "BL_BOT_Fulfillment",
            "marketplaceId": MARKETPLACE_ID,
            "categoryTypes": [{"name": "ALL_EXCLUDING_MOTORS_VEHICLES"}],
            "handlingTime": {"value": 2, "unit": "DAY"},
            "shippingOptions": [{
                "optionType": "DOMESTIC",
                "costType": "FLAT_RATE",
                "shippingServices": [{
                    "shippingServiceCode": "DE_DHLPaket",
                    "buyerResponsibleForShipping": False,
                    "shippingCost": {"value": "3.99", "currency": "EUR"}
                }]
            }]
        }
    },
    "payment": {
        "endpoint": "/sell/account/v1/payment_policy",
        "env_key": "EBAY_PAYMENT_POLICY_ID",
        "name_key": "paymentPolicies",
        "id_key": "paymentPolicyId",
        "payload": {
            "name": "BL_BOT_Payment",
            "marketplaceId": MARKETPLACE_ID,
            "categoryTypes": [{"name": "ALL_EXCLUDING_MOTORS_VEHICLES"}],
            "paymentMethods": [{
                "paymentMethodType": "PAYPAL",
                "recipientAccountReference": {"referenceId": "test-sandbox@example.com"}
            }]
        }
    },
    "return": {
        "endpoint": "/sell/account/v1/return_policy",
        "env_key": "EBAY_RETURN_POLICY_ID",
        "name_key": "returnPolicies",
        "id_key": "returnPolicyId",
        "payload": {
            "name": "BL_BOT_Return",
            "marketplaceId": MARKETPLACE_ID,
            "categoryTypes": [{"name": "ALL_EXCLUDING_MOTORS_VEHICLES"}],
            "returnsAccepted": True,
            "returnPeriod": {"value": 30, "unit": "DAY"},
            "returnShippingCostPayer": "BUYER",
            "refundMethod": "MONEY_BACK"
        }
    }
}

async def get_existing_policy_id(session, endpoint: str, list_key: str, id_key: str, policy_name: str) -> str:
    """Holt die Policy ID per GET-Request, falls sie bereits existiert."""
    url = f"{EBAY_BASE_URL}{endpoint}?marketplace_id={MARKETPLACE_ID}"
    async with session.get(url, headers=HEADERS) as resp:
        if resp.status == 200:
            data = await resp.json()
            policies = data.get(list_key, [])
            for policy in policies:
                if policy.get("name") == policy_name:
                    return policy.get(id_key)
        else:
            logger.error(f"Fehler beim Abrufen bestehender Policies ({endpoint}): {await resp.text()}")
    return None

async def create_policy(session, policy_type: str, policy_data: dict) -> str:
    """Versucht eine Policy anzulegen und gibt die ID zurück."""
    url = f"{EBAY_BASE_URL}{policy_data['endpoint']}"
    payload = policy_data['payload']
    policy_name = payload["name"]

    logger.info(f"Erstelle Policy '{policy_name}'...")
    async with session.post(url, headers=HEADERS, json=payload) as resp:
        if resp.status in (200, 201):
            data = await resp.json()
            policy_id = data.get(policy_data['id_key'])
            logger.info(f"Erfolgreich erstellt! ID={policy_id}")
            return policy_id
        
        resp_text = await resp.text()
        
        # Versuche DuplicateProfileId direkt aus der Antwort zu extrahieren
        is_duplicate = False
        if resp.status == 400:
            lower_resp = resp_text.lower()
            if "20400" in lower_resp or "20401" in lower_resp or "already exists" in lower_resp or "doppelt vorhanden" in lower_resp:
                is_duplicate = True

        if is_duplicate:
            try:
                error_data = json.loads(resp_text)
                for err in error_data.get("errors", []):
                    for param in err.get("parameters", []):
                        if param.get("name") == "DuplicateProfileId":
                            duplicate_id = param.get("value")
                            logger.info(f"Die Policy '{policy_name}' existiert bereits. ID aus Fehlermeldung extrahiert: {duplicate_id}")
                            return duplicate_id
            except json.JSONDecodeError:
                pass
            
            # Fallback: Manuelles GET, falls extrahieren fehlschlägt
            logger.warning(f"Die Policy '{policy_name}' existiert bereits. Suche nach bestehender ID (GET Fallback)...")
            policy_id = await get_existing_policy_id(
                session, 
                policy_data['endpoint'], 
                policy_data['name_key'], 
                policy_data['id_key'], 
                policy_name
            )
            if policy_id:
                logger.info(f"Vorhandene ID gefunden: {policy_id}")
                return policy_id
            else:
                logger.error(f"Fehler: Konnte die existierende Policy '{policy_name}' nicht in der Liste finden.")
        else:
            logger.error(f"API Fehler beim Erstellen der '{policy_name}'-Policy: {resp_text}")

    return None

async def opt_in_business_policies(session):
    """Opt-In für Business Policies (Seller Profiles), falls der Account noch nicht aktiviert ist."""
    url = f"{EBAY_BASE_URL}/sell/account/v1/program/opt_in"
    payload = {"programType": "SELLING_POLICY_MANAGEMENT"}
    
    logger.info("Überprüfe Business Policy Opt-In Status...")
    async with session.post(url, headers=HEADERS, json=payload) as resp:
        if resp.status in (200, 201):
            logger.info("Erfolgreich für Business Policies (SELLING_POLICY_MANAGEMENT) angemeldet.")
        else:
            resp_text = await resp.text()
            if "already opted in" in resp_text.lower() or "already" in resp_text.lower():
                logger.info("Account ist bereits für Business Policies angemeldet.")
            else:
                logger.warning(f"Opt-in antwortete mit {resp.status}: {resp_text}")

async def create_inventory_location(session):
    """Erstellt die Standard-Lagerort (Inventory Location) in eBay, da diese für Offers zwingend ist."""
    url = f"{EBAY_BASE_URL}/sell/inventory/v1/location/DEFAULT"
    payload = {
        "location": {
            "address": {
                "addressLine1": "Musterstr. 1",
                "city": "Berlin",
                "postalCode": "10115",
                "country": "DE"
            }
        },
        "locationInstructions": "Versandfertig aus Deutschland",
        "name": "Standard Lager",
        "merchantLocationStatus": "ENABLED",
        "locationTypes": ["STORE"]
    }
    
    logger.info("Erstelle Inventory Location 'DEFAULT'...")
    async with session.post(url, headers=HEADERS, json=payload) as resp:
        if resp.status in (200, 201, 204):
            logger.info("Inventory Location 'DEFAULT' erfolgreich erstellt/aktiviert.")
        else:
            resp_text = await resp.text()
            if "already exists" in resp_text.lower() or "existiert" in resp_text.lower() or "25802" in resp_text:
                logger.info("Inventory Location 'DEFAULT' existiert bereits.")
            else:
                logger.warning(f"Fehler beim Erstellen der Inventory Location: {resp_text}")

async def setup():
    logger.info("Starte eBay Business Policies Auto-Setup...")

    async with aiohttp.ClientSession() as session:
        # Im Sandbox-Environment ist oft ein manuelles Opt-In für SELLING_POLICY_MANAGEMENT nötig
        await opt_in_business_policies(session)
        
        # Erstelle den notwendigen DEFAULT Lagerort
        await create_inventory_location(session)
        
        for p_type, p_data in POLICIES.items():
            policy_id = await create_policy(session, p_type, p_data)
            
            if policy_id:
                env_key = p_data['env_key']
                # Schreibe ID in die .env-Datei
                set_key(ENV_FILE, env_key, policy_id)
                logger.info(f"Geschrieben in .env: {env_key}={policy_id}")
            else:
                logger.error(f"Setup für {p_type}-Policy fehlgeschlagen. Überspringe...")

    logger.info("Setup abgeschlossen!")

if __name__ == "__main__":
    asyncio.run(setup())

"""
EbayTokenManager – Automatischer OAuth2 Token-Refresh für die eBay API.

Singleton-Pattern: Das Modul stellt eine einzige, globale get_token() Funktion bereit.
Die EbayTokenManager Instanz wird LAZY beim ersten Aufruf erstellt, sodass .env/os.environ
vorher geladen sein kann (z.B. durch die GUI oder dotenv).

Verwendung in allen Modulen:
    from ebay_token_manager import get_token
    token = get_token()   # gibt immer einen gültigen Access Token zurück
"""

import requests
import base64
import time
import os
import logging

logger = logging.getLogger(__name__)


class EbayTokenManager:
    """Verwaltet den eBay OAuth2 Access Token mit automatischem Refresh."""

    def __init__(self):
        self.client_id = os.getenv("EBAY_CLIENT_ID")
        self.client_secret = os.getenv("EBAY_CLIENT_SECRET")
        self.refresh_token = os.getenv("EBAY_REFRESH_TOKEN")
        self.access_token = None
        self.token_expiry = 0

        if not self.client_id or not self.client_secret or not self.refresh_token:
            logger.warning(
                "eBay OAuth Credentials unvollständig! "
                "Benötigt: EBAY_CLIENT_ID, EBAY_CLIENT_SECRET, EBAY_REFRESH_TOKEN. "
                "Token-Refresh wird fehlschlagen."
            )

    def get_access_token(self) -> str:
        """
        Gibt einen gültigen Access Token zurück.
        Refreshed automatisch 60 Sekunden vor Ablauf.
        """
        # Noch gültig → direkt zurückgeben
        if self.access_token and time.time() < self.token_expiry - 60:
            return self.access_token

        # Abgelaufen oder noch nicht vorhanden → automatisch erneuern
        logger.info("eBay Access Token wird erneuert (Refresh Token Flow)...")

        if not self.client_id or not self.client_secret or not self.refresh_token:
            raise RuntimeError(
                "eBay OAuth Credentials fehlen. Bitte EBAY_CLIENT_ID, "
                "EBAY_CLIENT_SECRET und EBAY_REFRESH_TOKEN in den Settings eintragen."
            )

        credentials = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        try:
            response = requests.post(
                "https://api.ebay.com/identity/v1/oauth2/token",
                headers={
                    "Authorization": f"Basic {credentials}",
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "scope": (
                        "https://api.ebay.com/oauth/api_scope "
                        "https://api.ebay.com/oauth/api_scope/sell.fulfillment "
                        "https://api.ebay.com/oauth/api_scope/sell.inventory "
                        "https://api.ebay.com/oauth/api_scope/sell.marketing"

                    )
                },
                timeout=15
            )
        except requests.RequestException as e:
            logger.error(f"Netzwerkfehler beim Token-Refresh: {e}")
            raise RuntimeError(f"Token-Refresh Netzwerkfehler: {e}")

        data = response.json()

        if "access_token" not in data:
            error_msg = f"Token Refresh fehlgeschlagen: {data}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.access_token = data["access_token"]
        self.token_expiry = time.time() + data.get("expires_in", 7200)

        logger.info(
            f"✅ eBay Access Token erfolgreich erneuert. "
            f"Gültig für {data.get('expires_in', 7200)} Sekunden."
        )
        return self.access_token


# ---------------------------------------------------------------------------
# Lazy Singleton: Die Instanz wird beim ERSTEN Aufruf von get_token() erstellt.
# Das stellt sicher, dass os.environ / .env bereits geladen ist.
# ---------------------------------------------------------------------------
_instance: EbayTokenManager | None = None


def get_token() -> str:
    """
    Modulweite Convenience-Funktion.
    Erstellt den EbayTokenManager beim ersten Aufruf (lazy)
    und gibt danach immer den aktuellen Access Token zurück.

    Verwendung:
        from ebay_token_manager import get_token
        token = get_token()
    """
    global _instance
    if _instance is None:
        _instance = EbayTokenManager()
    return _instance.get_access_token()


def reset():
    """Setzt die Singleton-Instanz zurück (z.B. nach Settings-Änderung in der GUI)."""
    global _instance
    _instance = None
    logger.info("EbayTokenManager zurückgesetzt. Nächster Aufruf erstellt neue Instanz.")

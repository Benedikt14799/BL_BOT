"""
Einmaliges Helper-Skript zum Generieren eines eBay OAuth2 Refresh Tokens.

WICHTIG: Im eBay Developer Portal unter OAuth -> RuName:
  - Lass "Your auth accepted URL" LEER (nur https://)
  - Lass "Your auth declined URL" LEER (nur https://)
  eBay nutzt dann ihre eigenen Standard-Seiten.

Anleitung:
  1. python get_refresh_token.py
  2. Kopiere die angezeigte URL in deinen Browser
  3. Logge dich bei eBay ein und erteile die Berechtigung
  4. Nach dem Login zeigt eBay eine Seite mit dem Authorization Code
     ODER leitet dich weiter - in beiden Fällen steht der Code in der URL
  5. Kopiere die KOMPLETTE URL und füge sie im Terminal ein
"""

import urllib.parse
import requests
import base64

# ===== DEINE DATEN =====
CLIENT_ID     = "Benedikt-n8-PRD-60bbd31c9-66429cd3"
CLIENT_SECRET = "PRD-bf1f19d43b1c-2863-46c0-a43c-c24e"
RUNAME        = "Benedikt_Faude-Benedikt-n8-PRD-dculqoxjm"

SCOPES = " ".join([
    "https://api.ebay.com/oauth/api_scope",
    "https://api.ebay.com/oauth/api_scope/sell.inventory",
    "https://api.ebay.com/oauth/api_scope/sell.fulfillment",
    "https://api.ebay.com/oauth/api_scope/sell.marketing",
])
# =======================


def main():
    print("=" * 60)
    print("  eBay Refresh Token Generator")
    print("=" * 60)
    print()

    # Auth URL bauen
    auth_url = (
        f"https://auth.ebay.com/oauth2/authorize"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={urllib.parse.quote(RUNAME)}"
        f"&response_type=code"
        f"&scope={urllib.parse.quote(SCOPES)}"
    )

    print("Schritt 1: Öffne diese URL in deinem Browser:")
    print()
    print(auth_url)
    print()
    print("Schritt 2: Logge dich bei eBay ein und erteile die Berechtigung.")
    print()
    print("Schritt 3: Nach dem Login wirst du weitergeleitet.")
    print("   Kopiere die KOMPLETTE URL aus der Adressleiste.")
    print("   (Auch wenn die Seite einen Fehler zeigt!)")
    print("   Die URL enthält '?code=...' - das brauchen wir.")
    print()
    print("   Falls eBay den Code direkt auf der Seite anzeigt,")
    print("   kannst du auch nur den Code-Text hier einfügen.")
    print("=" * 60)
    print()

    raw_input = input("👉 Füge hier die URL oder den Code ein: ").strip()

    # Prüfe zuerst ob die URL einen Fehler enthält
    if "error=" in raw_input:
        parsed = urllib.parse.urlparse(raw_input)
        params = urllib.parse.parse_qs(parsed.query)
        error = params.get("error", ["unbekannt"])[0]
        print()
        print("=" * 60)
        print("  ❌ eBay hat die Autorisierung NICHT erteilt!")
        print("=" * 60)
        print()
        print(f"  Fehler: {error}")
        if error == "invalid_scope":
            print()
            print("  💡 Ein oder mehrere Scopes sind nicht freigegeben.")
            print("     Prüfe im eBay Developer Portal unter")
            print("     'OAuth accepted/declined URL' und 'API Scopes'")
            print("     ob alle Scopes für deine App aktiviert sind.")
        print()
        return

    # Authorization Code extrahieren
    auth_code = None

    if "code=" in raw_input:
        # URL mit code-Parameter
        parsed = urllib.parse.urlparse(raw_input)
        params = urllib.parse.parse_qs(parsed.query)
        if "code" in params:
            auth_code = params["code"][0]
    elif raw_input.startswith("v%5E") or raw_input.startswith("v^"):
        # Direkt der Code (URL-encoded oder plain)
        auth_code = urllib.parse.unquote(raw_input)
    else:
        # Letzter Versuch: vielleicht ist es der Code ohne Prefix
        auth_code = raw_input

    if not auth_code:
        print("❌ Konnte keinen Authorization Code finden!")
        return

    print()
    print(f"✅ Code extrahiert: {auth_code[:40]}...")
    print("🔄 Tausche gegen Refresh Token...")
    print()

    # Token-Tausch
    credentials = base64.b64encode(
        f"{CLIENT_ID}:{CLIENT_SECRET}".encode()
    ).decode()

    print(f"[DEBUG] Client ID: {CLIENT_ID}")
    print(f"[DEBUG] RuName: {RUNAME}")
    print(f"[DEBUG] Credentials Länge: {len(credentials)}")
    print()

    response = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": RUNAME
        },
        timeout=30
    )

    print(f"[DEBUG] HTTP Status: {response.status_code}")
    print(f"[DEBUG] Response: {response.text[:500]}")
    print()

    data = response.json()

    if "refresh_token" in data:
        print("=" * 60)
        print("  🎉 REFRESH TOKEN ERFOLGREICH GENERIERT!")
        print("=" * 60)
        print()
        print(data["refresh_token"])
        print()
        days = data.get("refresh_token_expires_in", 0) // 86400
        print(f"Gültig für: ~{days} Tage ({days // 30} Monate)")
        print()
        print("👉 Trage diesen Token in den GUI-Settings")
        print("   unter 'EBAY_REFRESH_TOKEN' ein!")
    else:
        print("=" * 60)
        print("  ❌ FEHLER beim Token-Tausch")
        print("=" * 60)
        print()
        print(f"Error: {data.get('error', 'unbekannt')}")
        print(f"Beschreibung: {data.get('error_description', 'keine')}")
        print()
        if "invalid_grant" in str(data):
            print("💡 TIPP: Der Authorization Code ist abgelaufen (max 5 Min)!")
            print("   Starte das Skript neu und sei beim Einfügen schneller.")
        elif "invalid_client" in str(data):
            print("💡 TIPP: CLIENT_ID oder CLIENT_SECRET sind falsch!")
            print("   Prüfe die Werte im eBay Developer Portal.")

    print()
    print("Fertig. ✅")


if __name__ == "__main__":
    main()

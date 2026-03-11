import os
import logging
import asyncio
from playwright.async_api import async_playwright, BrowserContext, Page

logger = logging.getLogger("BooklookerAutomator")

class BooklookerAutomator:
    """
    Klasse zur Automatisierung des Booklooker-Checkouts via Playwright.
    Dient dem Arbitrage-Dienst zur Vorbereitung von Bestellungen und Erstellung von Screenshots.
    """
    def __init__(self):
        self.user = os.environ.get("BL_USER")
        self.password = os.environ.get("BL_PASSWORD")
        self.headless = os.environ.get("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
        self.storage_state = "bl_session.json"  # Speichert login cookies für persistente Sessions

    async def _login(self, page: Page):
        """Führt den Login bei Booklooker durch."""
        logger.info("Führe Login bei Booklooker durch...")
        await page.goto("https://www.booklooker.de/app/priv/login.php")
        
        # Akzeptiere Cookies, falls popup vorhanden
        try:
            cart = await page.wait_for_selector("text='Zustimmen'", timeout=3000)
            if cart:
                await cart.click()
        except Exception:
            pass

        await page.fill("input[name='login_email']", self.user)
        await page.fill("input[name='login_password']", self.password)
        await page.click("button[type='submit']")
        
        # Warte auf Erfolg (z.B. Sichtbarkeit des "Mein Konto" Elements)
        try:
            await page.wait_for_selector("text='Mein Konto'", timeout=5000)
            logger.info("Login erfolgreich.")
        except Exception:
            logger.warning("Login-Überprüfung fehlgeschlagen. Evtl. bereits eingeloggt oder Captcha.")

    async def prepare_checkout(self, order_id: str, bl_url: str, shipping_address: dict) -> str:
        """
        Sucht den Artikel, legt ihn in den Warenkorb, fügt die Lieferadresse ein
        und erstellt ein Screenshot von der Zusammenfassung/Zahlungsseite.
        Gibt den Dateipfad des Screenshots zurück.
        """
        screenshot_path = f"checkout_{order_id}.png"
        
        if not self.user or not self.password:
            logger.error("BL_USER oder BL_PASSWORD fehlen. Abbruch.")
            return ""

        async with async_playwright() as p:
            # Browser starten
            browser = await p.chromium.launch(headless=self.headless)
            
            # Context laden/erstellen (für Session Persistenz)
            context_args = {}
            if os.path.exists(self.storage_state):
                context_args['storage_state'] = self.storage_state
                
            context = await browser.new_context(**context_args)
            page = await context.new_page()

            try:
                # 1. Login-Check / Login
                await page.goto("https://www.booklooker.de")
                is_logged_in = await page.locator("text='Mein Konto'").count() > 0
                if not is_logged_in:
                    await self._login(page)
                    # Session speichern
                    await context.storage_state(path=self.storage_state)

                # 2. Direkt zum spezifischen Artikel navigieren
                logger.info(f"Navigiere direkt zum Artikel: {bl_url}")
                await page.goto(bl_url)
                
                # Warte auf Ergebnisse
                await page.wait_for_load_state("networkidle")

                # 3. In den Warenkorb
                logger.info("Lege Artikel in den Warenkorb...")
                cart_buttons = page.locator("input[value='In den Warenkorb']")
                if await cart_buttons.count() > 0:
                    await cart_buttons.first.click()
                else:
                    logger.error("Kein 'In den Warenkorb' Button gefunden. Artikel evtl. verkauft.")
                    await browser.close()
                    return ""

                # 4. Zur Kasse
                await page.goto("https://www.booklooker.de/app/cart.php")
                checkout_btn = page.locator("text='Zur Kasse'")
                if await checkout_btn.count() > 0:
                    await checkout_btn.first.click()

                # 5. Alternative Lieferadresse eintragen (Mockup Logik)
                # ACHTUNG: Das tatsächliche DOM von BL muss analysiert werden, um dies präzise auszufüllen
                logger.info("Versuche abweichende Lieferadresse einzutragen...")
                try:
                    await page.click("text='Abweichende Lieferadresse'")  # Checkbox o.ä.
                    
                    if "name" in shipping_address:
                        await page.fill("input[name='shipping_name']", shipping_address["name"])
                    if "street" in shipping_address:
                        await page.fill("input[name='shipping_street']", shipping_address["street"])
                    if "city" in shipping_address:
                        await page.fill("input[name='shipping_city']", shipping_address["city"])
                    if "zip" in shipping_address:
                        await page.fill("input[name='shipping_zip']", shipping_address["zip"])
                        
                    # Weiter zur Zahlung
                    await page.click("button[type='submit']") # oder ähnlich
                except Exception as e:
                    logger.warning(f"Konnte Lieferadresse nicht ausfüllen (Selectors fehlerhaft?): {e}")

                # 6. Screenshot der PayPal / Checkout Page
                logger.info(f"Erstelle Screenshot: {screenshot_path}")
                await page.wait_for_load_state("networkidle")
                await page.screenshot(path=screenshot_path, full_page=True)
                
                return screenshot_path

            except Exception as e:
                logger.error(f"Fehler während Playwright Session: {e}")
                return ""
                
            finally:
                # Am Ende immer sauber schließen
                await browser.close()

if __name__ == "__main__":
    # Test-Aufruf
    async def test():
        automator = BooklookerAutomator()
        addr = {"name": "Test Käufer", "street": "Hauptstr 1", "city": "Berlin", "zip": "10115"}
        # Setze hier einen ECHTEN Booklooker-Link ein zum Testen!
        test_url = "https://www.booklooker.de/app/detail.php?id=DEIN_TEST_ARTIKEL_ID" 
        res = await automator.prepare_checkout("test_order_123", test_url, addr)
        print("Screenshot Path:", res)
        
    asyncio.run(test())

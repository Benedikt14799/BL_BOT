# scrape.py
import logging
import re
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse, urljoin

import bl_processing
import database
import isbn_processing
import picture_processing
import price_processing
from database import DatabaseManager

logger = logging.getLogger(__name__)
number_pattern = re.compile(r"\d+")
semaphore = asyncio.Semaphore(20)

# Basis-URL für relative Pfade
BASE_URL = "https://www.booklooker.de"

async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    """
    GET-Request, wirft bei Fehlern und liefert den HTML-Text.
    """
    async with session.get(url, timeout=30) as resp:
        resp.raise_for_status()
        return await resp.text()


def extract_offer_links_from_page(html: str) -> list[str]:
    """
    Parst eine Übersichtsseite und gibt alle Detail-URLs der Angebote zurück.
    """
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    # Suche in div.resultlist_products nach div.articleRow
    for article in soup.select("div.resultlist_products div.articleRow.resultlist_productsproduct"):
        a_tag = article.select_one("span.artikeltitel.notranslate a")
        if not a_tag:
            continue
        href = a_tag.get("href", "")
        # nur Detailseiten mit '/id/'
        if "/id/" in href:
            full_url = urljoin(BASE_URL, href)
            links.append(full_url)
        else:
            logger.debug(f"Übersprungen (kein Detail-Link): {href}")
    return links

async def fetch_and_process(session: aiohttp.ClientSession, link: str):
    """
    Ermittelt für eine Basis-URL die Seiten- und Bücherzahl.
    Gibt (link, highest_page, books_count) zurück.
    """
    async with semaphore:
        try:
            html = await fetch_html(session, link)
            soup = BeautifulSoup(html, 'html.parser')

            # Bücheranzahl
            div = soup.find('div', class_='resultlist_count')
            books_count = int(number_pattern.search(div.text).group()) \
                if div and number_pattern.search(div.text) else 0

            # Seitenzahl
            nums = [int(e.text) for e in soup.find_all(class_='PageNavNumItem') if e.text.isdigit()]
            highest_page = max(nums) if nums else 1

            logger.info(f"{link} → Seiten: {highest_page}, Bücher: {books_count}")
            return link, highest_page, books_count

        except Exception as e:
            logger.error(f"Fehler bei fetch_and_process für {link}: {e}")
            return None

async def insert_links_into_sitetoscrape(links_to_scrape: list[str], db_pool):
    """
    Fügt neue Basis-Links in sitetoscrape ein (nur wenn noch nicht vorhanden).
    """
    async with db_pool.acquire() as conn:
        existing = {r["link"] for r in await conn.fetch("SELECT link FROM sitetoscrape;")}

    new_links = [l for l in links_to_scrape if l not in existing]
    if not new_links:
        logger.info("Keine neuen Links in sitetoscrape.")
        return

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *(fetch_and_process(session, l) for l in new_links),
            return_exceptions=True
        )

    insert_data = [r for r in results if isinstance(r, tuple)]
    if insert_data:
        async with db_pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO sitetoscrape (link, anzahlSeiten, numbersOfBooks)
                VALUES ($1,$2,$3)
                ON CONFLICT (link) DO NOTHING
                """,
                insert_data
            )
        logger.info(f"{len(insert_data)} neue Links in sitetoscrape eingefügt.")


def build_page_url(base_link: str, page: int) -> str:
    """
    Fügt/überschreibt ?setMediaType=0&page=<n> im Query-String.
    """
    p = urlparse(base_link)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q.update({"setMediaType": "0", "page": str(page)})
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q, doseq=True), p.fragment))

async def fetch_and_parse(session: aiohttp.ClientSession, page_url: str) -> list[str]:
    """
    Lädt eine Übersichtsseite und gibt alle Angebots-Detaillinks zurück.
    Zusätzlich loggt er jede gefundene URL, damit du sie prüfen kannst.
    """
    try:
        html_content = await fetch_html(session, page_url)
        links = extract_offer_links_from_page(html_content)
        logger.info(f"Seite {page_url}: {len(links)} Detail-Links gefunden")
        # Ausgabe aller Links zum Prüfen
        for link in links:
            logger.debug(f"Gefundener Link auf {page_url}: {link}")
        if not links:
            logger.warning(f"⚠️ Seite {page_url} lieferte 0 Detail-Links.")
        return links
    except Exception as e:
        logger.error(f"Fehler beim Parsen von {page_url}: {e}")
        return []


async def fetch_and_parse_and_store(session: aiohttp.ClientSession, page_url: str, db_pool) -> int:
    """
    Ruft fetch_and_parse auf, speichert jeden Angebots-Link in library und liefert die Anzahl gespeicherter Links.
    """
    links = await fetch_and_parse(session, page_url)
    if not links:
        return 0

    insert_data = [(l,) for l in links]
    try:
        async with db_pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO library (LinkToBL)
                VALUES ($1)
                ON CONFLICT (LinkToBL) DO NOTHING
                """,
                insert_data
            )
        logger.info(f"{len(links)} Links von {page_url} in library gespeichert.")
        return len(links)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Links von {page_url}: {e}")
        return 0

async def scrape_and_save_pages(db_pool):
    """
    1) Liest alle sitetoscrape-Einträge mit Seitenzahl > 0 aus.
    2) Generiert für jede Seite die korrekte URL und ruft fetch_and_parse_and_store auf.
    3) Summiert erwartete vs. gefundene Links und setzt Fremdschlüssel.
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, link, anzahlSeiten, numbersOfBooks FROM sitetoscrape WHERE anzahlSeiten > 0;"
        )
    if not rows:
        logger.info("Keine Seiten zum Scrapen.")
        return

    total_expected = sum(r["numbersofbooks"] for r in rows)
    total_scraped = 0

    tasks = []
    async with aiohttp.ClientSession() as session:
        for r in rows:
            for p in range(1, r["anzahlseiten"] + 1):
                page_url = build_page_url(r["link"], p)
                tasks.append(fetch_and_parse_and_store(session, page_url, db_pool))

        logger.info(f"Starte Scraping von {len(tasks)} Seiten…")
        for i in range(0, len(tasks), 50):
            results = await asyncio.gather(*tasks[i : i + 50], return_exceptions=True)
            for res in results:
                if isinstance(res, int):
                    total_scraped += res

    logger.info(f"Erwartet (numbersOfBooks insgesamt): {total_expected}, Gefunden (gespeichert): {total_scraped}")

    await DatabaseManager.set_foreignkey(db_pool)
    logger.info("Fremdschlüssel in library gesetzt.")


async def process_library_links_async(db_pool):
    """
    Verarbeitet alle Einträge in library:
    - Ruft process_entry ab (liefert nun auch soup)
    - Nutzt dieses soup für Preis-, Bild- und Property-Processing
    """
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, LinkToBL FROM library;")

        async with aiohttp.ClientSession() as session:
            for row in rows:
                num, link = row["id"], row["linktobl"]
                has_isbn, isbn, soup = await isbn_processing.process_entry(
                    session, link, num, db_pool
                )
                if not has_isbn:
                    # wurde bereits in missing_listings verschoben
                    continue

                await price_processing.PriceProcessing.get_price(session, soup, num, db_pool)
                await picture_processing.PictureProcessing.get_pictures_with_dnb(
                    session,
                    soup,
                    num,
                    db_pool,
                    isbn
                )

                await bl_processing.PropertyToDatabase.process_and_save(
                    soup, num, db_pool
                )

    except Exception as e:
        logger.error(f"Fehler in process_library_links_async: {e}")


async def perform_webscrape_async(db_pool):
    """
    Führt die gesamte Webscraping-Pipeline aus:
    1. Fragt einmalig den Category Name ab.
    2. Füllt die Tabelle `library` mit statischen Daten (`prefill_db_with_static_data`).
    3. Verarbeitet Buch-Links und ruft zusätzliche Daten ab (`process_library_links_async`).
    """
    try:
        # 1) Kategorie einmalig abfragen (blockiert nur vor Async-Operationen)
        category_name = input("Bitte geben Sie den Category Name ein: ").strip() or "/Bücher & Zeitschriften/Bücher"

        # 2) Statische Daten vorfüllen
        await DatabaseManager.prefill_db_with_static_data(db_pool, category_name)

        # 3) Webscraping und Verarbeitung von Buchdaten
        await process_library_links_async(db_pool)

    except Exception as e:
        logger.error(f"Fehler in perform_webscrape_async: {e}")


"""
Funktion: extract_properties
----------------------------
- Extrahiert Eigenschaften aus einem BeautifulSoup-Objekt.
- Durchsucht HTML-Elemente mit spezifischen Klassen und sammelt Eigenschaftsnamen und Werte.
- Gibt ein Wörterbuch mit den extrahierten Eigenschaften zurück.
"""
def extract_properties(soup):
    properties = {}
    property_items = soup.find_all(class_=re.compile(r"propertyItem_\d+"))

    for item in property_items:
        try:
            # Elemente für Name und Wert extrahieren
            property_name_elem = item.find(class_="propertyName")
            property_value_elem = item.find(class_="propertyValue")

            # Validierung: Elemente müssen vorhanden sein
            if not property_name_elem or not property_value_elem:
                logger.warning(f"Element hat fehlende Name- oder Wert-Felder: {item}")
                continue

            # Text bereinigen und speichern
            property_name = property_name_elem.text.strip()
            property_value = property_value_elem.text.strip()
            properties[property_name] = property_value
        except Exception as e:
            # Fehler loggen mit zusätzlichem Kontext
            logger.error(f"Fehler beim Extrahieren der Eigenschaft aus Element {item}: {e}")
    return properties
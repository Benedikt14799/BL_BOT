# scrape.py
import logging
import re
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qsl, parse_qs, urlencode, urlunparse, urljoin

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
    Parst eine Übersichtsseite und gibt alle Detail‑URLs der Angebote zurück.
    (Erfasst auch gelb hinterlegte Einträge.)
    """
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []

    # Nimm alle Artikel‑Container, egal ob gelb oder weiß
    for article in soup.select("div.resultlist_products div.articleRow"):
        # Finde das erste <a href="/.../id/..."> im Container
        a_tag = article.find("a", href=re.compile(r"/.*/id/"))
        if not a_tag:
            continue

        href = a_tag.get("href")
        if not href:
            continue

        # nur echte Detailseiten mit '/id/'
        if "/id/" not in href:
            continue

        full_url = urljoin(BASE_URL, href)
        links.append(full_url)

    return links


async def fetch_and_process(session: aiohttp.ClientSession, link: str):
    """
    Ermittelt für eine Basis-URL die Seiten- und Bücherzahl.
    Gibt (link, highest_page, books_count) zurück.
    ROBUSTE Paginierung: erkennt 'page' aus Links und Text.
    """
    async with semaphore:
        try:
            html = await fetch_html(session, link)
            soup = BeautifulSoup(html, 'html.parser')

            # Bücheranzahl
            div = soup.find('div', class_='resultlist_count')
            books_count = int(number_pattern.search(div.text).group()) \
                if div and number_pattern.search(div.text) else 0

            # ROBUST: Seitenzahl
            pages = set()

            # 1) Alle anklickbaren Links prüfen, ob sie page=<n> tragen
            for a in soup.select('.pagelinks a, .PageNavNumItem a, a'):
                href = a.get('href')
                if not href:
                    continue
                try:
                    parsed = urlparse(href)
                    qs = parse_qs(parsed.query)
                    p = qs.get('page', [])
                    if p and p[0].isdigit():
                        pages.add(int(p[0]))
                except Exception:
                    pass

            # 2) zusätzlich Zahlen aus Navigations-Elementen lesen
            for e in soup.select('.PageNavNumItem, .pagelinks, .pagination, .pagelinks_top, .pagelinks_bottom'):
                txt = (e.get_text() or '').strip()
                for m in re.findall(r'\b\d+\b', txt):
                    try:
                        pages.add(int(m))
                    except ValueError:
                        pass

            highest_page = max(pages) if pages else 1

            logger.info(f"{link} → erkannte Seiten: {highest_page}, Bücher: {books_count}")
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
    Zusätzlich loggt er bei Bedarf jede gefundene URL im Debug-Level.
    """
    try:
        html_content = await fetch_html(session, page_url)
        links = extract_offer_links_from_page(html_content)
        logger.info(f"Seite {page_url}: {len(links)} Detail-Links gefunden")
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
            base = r["link"]
            n_pages = r["anzahlseiten"]
            if n_pages <= 0:
                continue

            first_url = build_page_url(base, 1)
            last_url = build_page_url(base, n_pages)
            logger.info(f"Erzeuge Seiten für {base}: 1..{n_pages} (z.B. {first_url} ... {last_url})")

            for p in range(1, n_pages + 1):
                page_url = build_page_url(base, p)
                tasks.append(fetch_and_parse_and_store(session, page_url, db_pool))

        logger.info(f"Starte Scraping von {len(tasks)} Seiten…")
        for i in range(0, len(tasks), 50):
            results = await asyncio.gather(*tasks[i: i + 50], return_exceptions=True)
            for res in results:
                if isinstance(res, int):
                    total_scraped += res

    logger.info(f"Erwartet (numbersOfBooks insgesamt): {total_expected}, Gefunden (gespeichert): {total_scraped}")

    await DatabaseManager.set_foreignkey(db_pool)
    logger.info("Fremdschlüssel in library gesetzt.")


# ===============================
# Detailverarbeitung – optimiert
# ===============================

# Konfiguration für Detailphase
DETAIL_SEMAPHORE = asyncio.Semaphore(50)  # behutsame Parallelität (Serverfreundlich anpassen)
MAX_RETRIES = 2
BATCH_SIZE = 200  # für gather in Blöcken


async def _process_one_entry(session: aiohttp.ClientSession, row, db_pool):
    """
    Verarbeitet EIN library-Datensatz robust:
    - ISBN prüfen (löscht bei missing)
    - Price
    - Pictures (verschiebt bei missing_photo)
    - Properties
    """
    num, link = row["id"], row["linktobl"]

    # Retry-Loop pro Eintrag
    attempt = 0
    while attempt <= MAX_RETRIES:
        attempt += 1
        try:
            async with DETAIL_SEMAPHORE:
                # ISBN-Check (löscht bei fehlender ISBN, gibt dann False zurück)
                has_isbn, isbn, soup = await isbn_processing.process_entry(session, link, num, db_pool)
                if not has_isbn:
                    # bereits in missing_listings verschoben und gelöscht
                    return "deleted_missing_isbn"

                # Preis berechnen und speichern
                await price_processing.PriceProcessing.get_price(session, soup, num, db_pool)

                # Bilder extrahieren und speichern
                # Bei fehlender ISBN würde hier isbn="" durchgereicht; die Funktion verschiebt ohne Bilder in missing_listings
                await picture_processing.PictureProcessing.get_pictures_with_dnb(
                    session, soup, num, db_pool, isbn or ""
                )

                # Properties extrahieren und speichern
                await bl_processing.PropertyToDatabase.process_and_save(soup, num, db_pool)

                return "ok"

        except Exception as e:
            logger.error(f"[{num}] Fehler in Detailverarbeitung (Versuch {attempt}/{MAX_RETRIES}): {e}")
            if attempt > MAX_RETRIES:
                # Als missing_listings markieren, damit keine „toten“ Datensätze bleiben
                try:
                    from database import DatabaseManager
                    await DatabaseManager.record_missing_listing(db_pool, num, link, "detail_error")
                    async with db_pool.acquire() as conn:
                        await conn.execute("DELETE FROM library WHERE id = $1", num)
                    logger.warning(f"[{num}] Nach Fehler und {MAX_RETRIES} Retries in missing_listings verschoben und gelöscht.")
                except Exception as e2:
                    logger.error(f"[{num}] Fehler beim Verschieben nach detail_error: {e2}")
                return "error"
            # kurzer Backoff vor erneutem Versuch
            await asyncio.sleep(0.5 * attempt)


async def process_library_links_async(db_pool):
    """
    Parallele, robuste Verarbeitung aller Einträge in library.
    - Batches mit gather
    - Progress-Logging alle BATCH_SIZE Datensätze
    - Retry bei transienten Fehlern
    - Keine „toten“ Datensätze: bei fehlenden Bildern oder finalen Fehlern verschieben/löschen
    """
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, LinkToBL FROM library;")

        total = len(rows)
        if total == 0:
            logger.info("Keine Einträge in library zu verarbeiten.")
            return

        logger.info(f"Starte Detailverarbeitung für {total} Einträge…")

        processed = 0
        async with aiohttp.ClientSession() as session:
            # in Batches verarbeiten
            for i in range(0, total, BATCH_SIZE):
                batch = rows[i: i + BATCH_SIZE]
                tasks = [asyncio.create_task(_process_one_entry(session, row, db_pool)) for row in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Zählen/Loggen
                ok = sum(1 for r in results if r == "ok")
                deleted_isbn = sum(1 for r in results if r == "deleted_missing_isbn")
                errors = sum(1 for r in results if r == "error" or isinstance(r, Exception))

                processed += len(batch)
                logger.info(f"Progress: {processed}/{total} (ok={ok}, missing_isbn_deleted={deleted_isbn}, errors={errors})")

        # Finaler Cleanup: fehlende Fotos sicher entfernen (Soll-Regel)
        async with db_pool.acquire() as conn:
            missing_photo_rows = await conn.fetch("SELECT id, LinkToBL FROM library WHERE COALESCE(photo,'') = ''")
            if missing_photo_rows:
                from database import DatabaseManager
                for r in missing_photo_rows:
                    try:
                        await DatabaseManager.record_missing_listing(db_pool, r["id"], r["linktobl"], "missing_photo_final")
                        await conn.execute("DELETE FROM library WHERE id = $1", r["id"])
                    except Exception as e:
                        logger.error(f"[{r['id']}] Cleanup fehlende Fotos: {e}")
                logger.warning(f"Cleanup: {len(missing_photo_rows)} Einträge ohne Fotos endgültig verschoben/gelöscht.")

    except Exception as e:
        logger.error(f"Fehler in process_library_links_async: {e}")


async def perform_webscrape_async(db_pool, category_name: str = "/Bücher & Zeitschriften/Bücher"):
    """
    Führt die gesamte Webscraping-Pipeline aus:
    1) Füllt die Tabelle `library` mit statischen Daten (Default-Category).
    2) Verarbeitet Buch-Links und ruft zusätzliche Daten ab.
    """
    try:
        # Statische Daten vorfüllen (Category)
        await DatabaseManager.prefill_db_with_static_data(db_pool, category_name)

        # Detailverarbeitung
        await process_library_links_async(db_pool)

    except Exception as e:
        logger.error(f"Fehler in perform_webscrape_async: {e}")


# ===============================
# Properties-Extractor (Hilfsfun.)
# ===============================

def extract_properties(soup):
    """
    Extrahiert Eigenschaften aus einem BeautifulSoup-Objekt.
    Durchsucht HTML-Elemente mit spezifischen Klassen und sammelt Eigenschaftsnamen und Werte.
    Gibt ein Wörterbuch mit den extrahierten Eigenschaften zurück.
    """
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

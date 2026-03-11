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
semaphore = asyncio.Semaphore(15)

# Basis-URL für relative Pfade
BASE_URL = "https://www.booklooker.de"


async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    """
    GET-Request mit exponentiellem Backoff (3 Versuche), wirft bei Fehlern und liefert den HTML-Text.
    """
    max_retries = 3
    base_delay = 1
    
    for attempt in range(max_retries + 1):
        try:
            async with session.get(url, timeout=30) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"Alle {max_retries} Retries für {url} fehlgeschlagen: {e}")
                raise e
            wait_time = base_delay * (2 ** attempt)
            logger.warning(f"Fehler bei {url}: {e}. Retry {attempt + 1}/{max_retries} in {wait_time}s...")
            await asyncio.sleep(wait_time)


def extract_offer_links_from_page(html: str) -> list[str]:
    """
    Parst eine Übersichtsseite und gibt alle Detail‑URLs der Angebote zurück.
    (Erfasst auch gelb hinterlegte Einträge.)
    """
    soup = BeautifulSoup(html, "lxml")
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

    # Booklooker hat oft eine Listen- UND eine Kachelansicht im HTML (display:none), 
    # was zu doppelten Links führt. Wir deduplizieren hier (Reihenfolge bleibt erhalten):
    return list(dict.fromkeys(links))


async def fetch_and_process(session: aiohttp.ClientSession, link: str):
    """
    Ermittelt für eine Basis-URL die Seiten- und Bücherzahl.
    Gibt (link, highest_page, books_count) zurück.
    ROBUSTE Paginierung: erkennt 'page' aus Links und Text.
    """
    async with semaphore:
        try:
            html = await fetch_html(session, link)
            soup = BeautifulSoup(html, 'lxml')

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


async def fetch_and_parse_and_store(session: aiohttp.ClientSession, page_url: str, db_pool, sitetoscrape_id: int) -> int:
    """
    Ruft fetch_and_parse auf, speichert jeden Angebots-Link in library und liefert die Anzahl gespeicherter Links.
    """
    links = await fetch_and_parse(session, page_url)
    if not links:
        return 0

    insert_data = [(l, sitetoscrape_id) for l in links]
    try:
        async with db_pool.acquire() as conn:
            # executemany gibt keinen "RETURNING"-Wert in asyncpg direkt einfach zurück bei on conflict do nothing
            # Wir machen es eleganter:
            result = await conn.execute(
                """
                INSERT INTO library (LinkToBL, sitetoscrape_id)
                VALUES %s
                ON CONFLICT (LinkToBL) DO NOTHING
                """ % ", ".join(f"('{l}', {sitetoscrape_id})" for l, _ in insert_data) # Direkter String-Build für Execute (einfacher für Count)
            )
            # result ist z.B. "INSERT 0 15"
            inserted_count = int(result.split()[-1]) if result.startswith("INSERT") else 0
            
        logger.info(f"Seite {page_url}: {len(links)} Links gefunden -> {inserted_count} NEU in DB gespeichert (sitetoscrape_id: {sitetoscrape_id}).")
        return inserted_count
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
            "SELECT id, link, anzahlSeiten, numbersOfBooks FROM sitetoscrape WHERE anzahlSeiten > 0 AND (is_scraped IS NULL OR is_scraped = FALSE);"
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
            sitetoscrape_id = r["id"]
            if n_pages <= 0:
                continue

            first_url = build_page_url(base, 1)
            last_url = build_page_url(base, n_pages)
            logger.info(f"Erzeuge Seiten für {base}: 1..{n_pages} (z.B. {first_url} ... {last_url})")

            for p in range(1, n_pages + 1):
                page_url = build_page_url(base, p)
                tasks.append(fetch_and_parse_and_store(session, page_url, db_pool, sitetoscrape_id))

        logger.info(f"Starte Scraping von {len(tasks)} Seiten…")
        for i in range(0, len(tasks), 50):
            results = await asyncio.gather(*tasks[i: i + 50], return_exceptions=True)
            for res in results:
                if isinstance(res, int):
                    total_scraped += res

    logger.info(f"📊 ZUSAMMENFASSUNG SCRAPING: Erwartet (laut Booklooker-Anzeige): {total_expected} | Neu in Datenbank gespeichert: {total_scraped}")

    if rows:
        scraped_ids = [r["id"] for r in rows]
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE sitetoscrape SET is_scraped = TRUE WHERE id = ANY($1)", scraped_ids)
        logger.info(f"{len(scraped_ids)} Basis-Links (Kategorien) erfolgreich als 'gescrapt' markiert.")


# ===============================
# Detailverarbeitung – optimiert
# ===============================

# Konfiguration für Detailphase
DETAIL_SEMAPHORE = asyncio.Semaphore(50)  # behutsame Parallelität (Serverfreundlich anpassen)
MAX_RETRIES = 2
BATCH_SIZE = 200  # für gather in Blöcken


async def _process_one_entry(session: aiohttp.ClientSession, row: dict, db_pool, token=None, base_url=None, fixed_costs=None, expected_sales=None, min_margin=None, zusatzkosten_low=None, zusatzkosten_high=None, steuer_satz=None):
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
                has_isbn, isbn, soup, dnb_props = await isbn_processing.process_entry(session, link, num, db_pool)
                if not has_isbn:
                    # bereits in missing_listings verschoben und gelöscht
                    return "deleted_missing_isbn"

                # Preis berechnen und speichern
                prof = await price_processing.PriceProcessing.get_price(
                    session=session,
                    soup=soup,
                    num=num,
                    db_pool=db_pool,
                    token=token,
                    base_url=base_url,
                    fixed_costs_monthly=fixed_costs,
                    expected_sales=expected_sales,
                    min_margin_req=min_margin,
                    addcost_low_mid=zusatzkosten_low,
                    addcost_high=zusatzkosten_high,
                    steuer_satz=steuer_satz
                )

                # Wenn unrentabel, in separate Tabelle verschieben
                if prof and not prof.get('rentabel'):
                    await DatabaseManager.record_unprofitable_listing(
                        db_pool,
                        num,
                        link,
                        f"Nicht rentabel (fehlt {prof.get('fehlende_marge')}€)",
                        prof.get('ebay_p'),
                        prof.get('marge')
                    )
                    return "deleted_unprofitable"

                # Bilder extrahieren und speichern
                # Bei fehlender ISBN würde hier isbn="" durchgereicht; die Funktion verschiebt ohne Bilder in missing_listings
                await picture_processing.PictureProcessing.get_pictures_with_dnb(
                    session, soup, num, db_pool, isbn or ""
                )

                # Properties extrahieren und speichern (inkl. DNB)
                status = await bl_processing.PropertyToDatabase.process_and_save(soup, num, db_pool, extra_props=dnb_props)
                
                if status == "schlechte_bewertung":
                    logger.warning(f"Artikel {num} hat eine Verkäuferbewertung unter 98% – verschiebe.")
                    await DatabaseManager.record_missing_listing(db_pool, num, link, "schlechte_bewertung")
                    return "deleted_schlechte_bewertung"

                return "ok"

        except Exception as e:
            logger.error(f"[{num}] Fehler in Detailverarbeitung (Versuch {attempt}/{MAX_RETRIES}): {e}")
            if attempt > MAX_RETRIES:
                # Als missing_listings markieren, damit keine „toten“ Datensätze bleiben
                try:
                    await DatabaseManager.record_missing_listing(db_pool, num, link, "detail_error")
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
    import os
    from decimal import Decimal

    from ebay_token_manager import get_token
    token = get_token()
    env_str = os.getenv("EBAY_ENV", "PRODUCTION")
    base_url = "https://api.ebay.com" if env_str == "PRODUCTION" else "https://api.sandbox.ebay.com"

    try:
        fixed_costs = Decimal(os.getenv("FIXKOSTEN_MONATLICH", "79.95").replace(',', '.'))
        expected_sales = int(os.getenv("ERWARTETE_VERKAEUFE", "200"))
        min_margin = Decimal(os.getenv("MINDESTMARGE", "2.50").replace(',', '.'))
        zusatzkosten_low = Decimal(os.getenv("ZUSATZKOSTEN_LOW_MID", "0.50").replace(',', '.'))
        zusatzkosten_high = Decimal(os.getenv("ZUSATZKOSTEN_HIGH", "1.75").replace(',', '.'))
        steuer_satz = Decimal(os.getenv("STEUERSATZ", "7.0").replace(',', '.'))
    except Exception:
        fixed_costs = Decimal("79.95")
        expected_sales = 200
        min_margin = Decimal("2.50")
        zusatzkosten_low = Decimal("0.50")
        zusatzkosten_high = Decimal("1.75")
        steuer_satz = Decimal("7.0")

    try:
        async with db_pool.acquire() as conn:
            # Gesamtzahl aller erfassten Links ermitteln (für den Log-Vergleich)
            total_in_db_result = await conn.fetchval("SELECT COUNT(*) FROM library;")
            total_in_db = total_in_db_result if total_in_db_result else 0
            
            # Nur Bücher verarbeiten, die noch keine Daten (ISBN) haben
            rows = await conn.fetch("SELECT id, LinkToBL FROM library WHERE isbn IS NULL;")

        total_to_process = len(rows)
        skipped = total_in_db - total_to_process

        if total_to_process == 0:
            logger.info("Keine Einträge in library zu verarbeiten.")
            return

        if skipped > 0:
            logger.info(f"Starte Detailverarbeitung für {total_to_process} Einträge (überspringe {skipped} bereits verarbeitete Bücher)…")
        else:
            logger.info(f"Starte Detailverarbeitung für {total_to_process} Einträge…")

        processed = 0
        async with aiohttp.ClientSession() as session:
            # in Batches verarbeiten
            for i in range(0, total_to_process, BATCH_SIZE):
                batch = rows[i: i + BATCH_SIZE]
                tasks = [
                    asyncio.create_task(_process_one_entry(
                        session, row, db_pool, token, base_url, fixed_costs, expected_sales, min_margin, zusatzkosten_low, zusatzkosten_high, steuer_satz
                    )) for row in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Zählen/Loggen
                ok = sum(1 for r in results if r == "ok")
                deleted_isbn = sum(1 for r in results if r == "deleted_missing_isbn")
                errors = sum(1 for r in results if r == "error" or isinstance(r, Exception))

                processed += len(batch)
                logger.info(f"Progress: {processed}/{total_to_process} (ok={ok}, missing_isbn_deleted={deleted_isbn}, errors={errors})")

        # Finaler Cleanup: fehlende Fotos sicher entfernen (Soll-Regel)
        async with db_pool.acquire() as conn:
            missing_photo_rows = await conn.fetch("SELECT id, LinkToBL FROM library WHERE COALESCE(photo,'') = ''")
            if missing_photo_rows:
                for r in missing_photo_rows:
                    try:
                        await DatabaseManager.record_missing_listing(db_pool, r["id"], r["linktobl"], "missing_photo_final")
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

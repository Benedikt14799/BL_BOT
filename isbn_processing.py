# isbn_processing.py
import logging
import re
from typing import Optional, Tuple, Dict
import aiohttp
import asyncio
from bs4 import BeautifulSoup

from database import DatabaseManager

logger = logging.getLogger(__name__)

dnb_xml_semaphore = asyncio.Semaphore(5)

# Regex zum Finden von ISBN-13 bzw. ISBN-10 in beliebigem Text
ISBN13_RE = re.compile(r"\b97[89]\d{10}\b")
ISBN10_RE = re.compile(r"\b\d{9}[0-9X]\b")

async def process_entry(
    session: aiohttp.ClientSession,
    link: str,
    num: int,
    db_pool
) -> Tuple[bool, Optional[str], Optional[BeautifulSoup], Optional[Dict]]:
    from scrape import fetch_html, extract_properties

    try:
        html_content = await fetch_html(session, link)
        soup = BeautifulSoup(html_content, "lxml")

        props = extract_properties(soup)
        raw = props.get("ISBN") or props.get("ISBN:")
        if not raw:
            logger.warning(f"Artikel {num} ohne ISBN – verschiebe.")
            await DatabaseManager.record_missing_listing(db_pool, num, link, "missing_isbn")
            return False, None, None, None

        isbn = pick_isbn(raw)
        if not isbn:
            logger.warning(f"Artikel {num} ohne extrahierbare ISBN – verschiebe.")
            await DatabaseManager.record_missing_listing(db_pool, num, link, "missing_isbn")
            return False, None, None, None

        # Gültige ISBN speichern
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE library SET ISBN = $1 WHERE id = $2", isbn, num
            )
        logger.info(f"Artikel {num}: ISBN '{isbn}' gespeichert.")

        # DNB API Abfrage (XML MARC21)
        dnb_props = {}
        dnb_url = f"https://services.dnb.de/sru/dnb?version=1.1&operation=searchRetrieve&query=isbn%3D{isbn}&recordSchema=MARC21-xml"
        try:
            async with dnb_xml_semaphore:
                async with session.get(dnb_url, timeout=10) as resp:
                    if resp.status == 200:
                        xml_text = await resp.text()
                        dnb_soup = BeautifulSoup(xml_text, 'xml')
                        
                        f300 = dnb_soup.find('datafield', tag='300')
                        if f300 and f300.find('subfield', code='a'):
                            dnb_props['seitenanzahl:'] = f300.find('subfield', code='a').text.strip()
                        
                        f520 = dnb_soup.find('datafield', tag='520')
                        if f520 and f520.find('subfield', code='a'):
                            dnb_props['abstract:'] = f520.find('subfield', code='a').text.strip()
                        
                        if dnb_props:
                            logger.info(f"[{num}] DNB Metadaten extrahiert: {dnb_props}")
        except Exception as e:
            logger.warning(f"[{num}] DNB Metadaten Abruf fehlgeschlagen für {isbn}: {e}")

        return True, isbn, soup, dnb_props

    except Exception as e:
        logger.error(f"Fehler in process_entry für Artikel {num}: {e}")
        return False, None, None, None


def pick_isbn(raw: str) -> Optional[str]:
    """
    Extrahiert zuerst eine ISBN-13, wenn vorhanden,
    ansonsten eine ISBN-10. Rückgabe als reiner Text.
    """
    m13 = ISBN13_RE.search(raw)
    if m13:
        return m13.group()

    m10 = ISBN10_RE.search(raw)
    if m10:
        return m10.group()

    return None

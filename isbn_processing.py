# isbn_processing.py
import logging
import re
from typing import Optional, Tuple
import aiohttp
from bs4 import BeautifulSoup

from database import DatabaseManager

logger = logging.getLogger(__name__)

# Regex zum Finden von ISBN-13 bzw. ISBN-10 in beliebigem Text
ISBN13_RE = re.compile(r"\b97[89]\d{10}\b")
ISBN10_RE = re.compile(r"\b\d{9}[0-9X]\b")

async def process_entry(
    session: aiohttp.ClientSession,
    link: str,
    num: int,
    db_pool
) -> Tuple[bool, Optional[str], Optional[BeautifulSoup]]:
    from scrape import fetch_html, extract_properties

    try:
        html_content = await fetch_html(session, link)
        soup = BeautifulSoup(html_content, "html.parser")

        props = extract_properties(soup)
        raw = props.get("ISBN") or props.get("ISBN:")
        if not raw:
            logger.warning(f"Artikel {num} ohne ISBN – verschiebe.")
            await DatabaseManager.record_missing_listing(db_pool, num, link, "missing_isbn")
            async with db_pool.acquire() as conn:
                await conn.execute("DELETE FROM library WHERE id = $1", num)
            return False, None, None

        isbn = pick_isbn(raw)
        if not isbn:
            logger.warning(f"Artikel {num} ohne extrahierbare ISBN – verschiebe.")
            await DatabaseManager.record_missing_listing(db_pool, num, link, "missing_isbn")
            async with db_pool.acquire() as conn:
                await conn.execute("DELETE FROM library WHERE id = $1", num)
            return False, None, None

        # Gültige ISBN speichern
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE library SET ISBN = $1 WHERE id = $2", isbn, num
            )
        logger.info(f"Artikel {num}: ISBN '{isbn}' gespeichert.")
        return True, isbn, soup

    except Exception as e:
        logger.error(f"Fehler in process_entry für Artikel {num}: {e}")
        return False, None, None


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

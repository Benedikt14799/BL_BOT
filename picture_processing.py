# picture_processing.py
import logging
from typing import List
import aiohttp
from bs4 import BeautifulSoup

from database import DatabaseManager

logger = logging.getLogger(__name__)

import asyncio

dnb_semaphore = asyncio.Semaphore(5)

class PictureProcessing:
    async def get_pictures_with_dnb(
        session: aiohttp.ClientSession,
        soup: BeautifulSoup,
        num: int,
        db_pool,
        isbn: str
    ) -> str:
        """
        Extrahiert Bilder und speichert sie in `library.photo`.
        """
        picture_links: List[str] = []

        # 1) DNB-Cover prüfen (nur wenn ISBN vorhanden und nicht leer)
        if isbn:
            dnb_url = f"https://portal.dnb.de/opac/mvb/cover?isbn={isbn}"
            try:
                async with dnb_semaphore:
                    async with session.get(dnb_url, timeout=10) as resp:
                        if resp.status == 200:
                            picture_links.append(dnb_url)
                            logger.debug(f"[{num}] DNB-Cover hinzugefügt: {dnb_url}")
                        else:
                            logger.debug(f"[{num}] Kein DNB-Cover (Status {resp.status})")
            except aiohttp.ClientError as e:
                logger.warning(f"[{num}] DNB-Cover-Anfrage fehlgeschlagen: {e}")

        # 2) Booklooker-Vorschaubilder extrahieren (XXL via href)
        preview_links = soup.find_all("a", class_="previewTop")[:24]
        if preview_links:
            for idx, a in enumerate(preview_links, start=1):
                href = a.get("href", "").strip()
                if href.startswith("https://"):
                    picture_links.append(href)
                    logger.info(f"[{num}] Bild {idx} (XXL via href): {href}")
        else:
            # Fallback: einzelnes Hauptbild aus id="currentImage"
            main_img = soup.find("img", id="currentImage")
            if main_img:
                src = main_img.get("src", "").strip()
                if src.startswith("https://"):
                    picture_links.append(src)
                    logger.info(f"[{num}] Fallback currentImage: {src}")

        # 3) String bauen und in DB speichern (Zusätzliche Sicherheit)
        picture_links = list(dict.fromkeys(picture_links))
        result = "|".join(picture_links)
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE library SET photo = $1 WHERE id = $2",
                    result, num
                )
            logger.debug(f"[{num}] {len(picture_links)} Bilder in DB gespeichert.")
        except Exception as e:
            logger.error(f"[{num}] Fehler beim Speichern der Bilder: {e}")

        # 4) NEU: Keine Bilder → missing_listings verschieben und laden
        if len(picture_links) == 0:
            try:
                # Link zur Dokumentation des Missing-Eintrags holen
                link = None
                try:
                    async with db_pool.acquire() as conn:
                        row = await conn.fetchrow("SELECT LinkToBL FROM library WHERE id = $1", num)
                        link = row["linktobl"] if row else None
                except Exception:
                    pass

                await DatabaseManager.record_missing_listing(db_pool, num, link or "", "missing_photo")
                logger.warning(f"[{num}] Keine Bilder gefunden – Datensatz in missing_listings verschoben und aus library gelöscht.")
            except Exception as e:
                logger.error(f"[{num}] Fehler beim Verschieben wegen fehlender Bilder: {e}")

        return result

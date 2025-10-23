# picture_processing.py
import logging
from typing import List
import aiohttp
from bs4 import BeautifulSoup

from database import DatabaseManager

logger = logging.getLogger(__name__)

class PictureProcessing:
    """
    Verwaltet die Extraktion und Speicherung von Bildern:
    - Holt optional DNB-Cover per ISBN
    - Extrahiert bis zu 24 Vorschaubilder von Booklooker
    - Verschiebt Datensätze ohne Bilder in missing_listings
    """

    @staticmethod
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
                async with session.get(dnb_url, timeout=10) as resp:
                    if resp.status == 200:
                        picture_links.append(dnb_url)
                        logger.info(f"[{num}] DNB-Cover hinzugefügt: {dnb_url}")
                    else:
                        logger.debug(f"[{num}] Kein DNB-Cover (Status {resp.status})")
            except aiohttp.ClientError as e:
                logger.warning(f"[{num}] DNB-Cover-Anfrage fehlgeschlagen: {e}")

        # 2) Booklooker-Vorschaubilder extrahieren (bis max. 24)
        preview_images = soup.find_all(class_="previewImage")[:24]
        if not preview_images:
            logger.debug(f"[{num}] Keine Booklooker-Vorschaubilder gefunden.")
        for idx, img in enumerate(preview_images, start=1):
            src = img.get("src")
            if not src:
                logger.warning(f"[{num}] Bild {idx} ohne src-Attribut übersprungen.")
                continue
            highres = src.replace("/t/", "/x/")
            picture_links.append(highres)
            logger.info(f"[{num}] Bild {idx} hinzugefügt: {highres}")

        # 3) String bauen und in DB speichern
        result = "|".join(picture_links)
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE library SET photo = $1 WHERE id = $2",
                    result, num
                )
            logger.info(f"[{num}] {len(picture_links)} Bilder in DB gespeichert.")
        except Exception as e:
            logger.error(f"[{num}] Fehler beim Speichern der Bilder: {e}")

        # 4) NEU: Keine Bilder → missing_listings verschieben und löschen
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
                async with db_pool.acquire() as conn:
                    await conn.execute("DELETE FROM library WHERE id = $1", num)
                logger.warning(f"[{num}] Keine Bilder gefunden – Datensatz in missing_listings verschoben und aus library gelöscht.")
            except Exception as e:
                logger.error(f"[{num}] Fehler beim Verschieben wegen fehlender Bilder: {e}")

        return result

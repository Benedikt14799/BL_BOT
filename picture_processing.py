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
    """
    Verwaltet die Extraktion und Speicherung von Bildern:
    - Holt optional DNB-Cover per ISBN
    - Extrahiert bis zu 24 Vorschaubilder von Booklooker
    - Verschiebt Datensätze ohne Bilder in missing_listings
    """

    @staticmethod
    async def check_image_exists(session: aiohttp.ClientSession, url: str) -> bool:
        try:
            async with session.head(url, timeout=5) as resp:
                return resp.status == 200
        except Exception:
            return False

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
                async with dnb_semaphore:
                    async with session.get(dnb_url, timeout=10) as resp:
                        if resp.status == 200:
                            picture_links.append(dnb_url)
                            logger.debug(f"[{num}] DNB-Cover hinzugefügt: {dnb_url}")
                        else:
                            logger.debug(f"[{num}] Kein DNB-Cover (Status {resp.status})")
            except aiohttp.ClientError as e:
                logger.warning(f"[{num}] DNB-Cover-Anfrage fehlgeschlagen: {e}")

        # 2) Booklooker-Vorschaubilder extrahieren (bis max. 24)
        preview_images = soup.find_all(class_="previewImage")[:24]
        
        # Fallback: Wenn es keine "previewImage" gibt, gibt es oft ein einzelnes "articleImage"
        if not preview_images:
            logger.debug(f"[{num}] Keine previewImage gefunden. Suche nach articleImage...")
            preview_images = soup.find_all(class_="articleImage")[:1]
            
        if not preview_images:
            logger.debug(f"[{num}] Keine Booklooker-Vorschaubilder gefunden.")
            
        seen_srcs = set()
        img_counter = 1
        
        for img in preview_images:
            src = img.get("src")
            if not src or src in seen_srcs:
                continue
                
            seen_srcs.add(src)
            
            # "/t/" (Thumbnails bei mehreren Bildern) oder "/bilder/" (Thumbnails bei Einzelbildern)
            # werden potenziell durch "/x/" (maximale Auflösung) ersetzt
            highres_candidate = src.replace("/t/", "/x/").replace("/bilder/", "/x/")
            
            # Überprüfen, ob das High-Res Bild existiert
            if await PictureProcessing.check_image_exists(session, highres_candidate):
                final_src = highres_candidate
            else:
                final_src = src
            
            # Nur hinzufügen, wenn es noch nicht als highres/final aufgelöst existiert
            if final_src not in picture_links:
                picture_links.append(final_src)
                logger.debug(f"[{num}] Bild {img_counter} hinzugefügt: {final_src}")
                img_counter += 1

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
                logger.warning(f"[{num}] Keine Bilder gefunden – Datensatz in missing_listings verschoben und aus library gelöscht.")
            except Exception as e:
                logger.error(f"[{num}] Fehler beim Verschieben wegen fehlender Bilder: {e}")

        return result

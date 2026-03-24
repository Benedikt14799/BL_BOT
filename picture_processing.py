# picture_processing.py
import logging
import re
from typing import List
import aiohttp
from bs4 import BeautifulSoup

# Booklooker Bild-URLs: Zwei verschiedene Formate in volle Auflösung konvertieren
# Muster A: /u{buchstabe}{ziffern}/Name.jpg  →  /ux{ziffern}/Name.jpg   (z.B. /ut03ldlm/ → /ux03ldlm/)
# Muster B: /{buchstabe}/{isbn}/Name.jpg     →  /x/{isbn}/Name.jpg      (z.B. /t/978.../ → /x/978.../)
_BL_PATTERN_A = re.compile(r'(images\.booklooker\.de/u)[a-wyzA-WYZ](\d)')
_BL_PATTERN_B = re.compile(r'(images\.booklooker\.de/)[a-wyzA-WYZ](/)')

def _to_xxl(url: str) -> str:
    """Konvertiert eine Booklooker Bild-URL in die XXL-Variante."""
    url = _BL_PATTERN_A.sub(r'\1x\2', url)
    url = _BL_PATTERN_B.sub(r'\1x\2', url)
    return url

from database import DatabaseManager

logger = logging.getLogger(__name__)

import asyncio

dnb_semaphore = asyncio.Semaphore(10)

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

        # 2) Booklooker-Vorschaubilder extrahieren (in voller Auflösung)
        preview_links = soup.find_all("a", class_="previewTop")[:24]
        if preview_links:
            for idx, a in enumerate(preview_links, start=1):
                img_url = None

                # Bevorzugt: href direkt (ist im Browser die XXL-URL)
                href = a.get("href", "").strip()
                if href.startswith("https://"):
                    img_url = href

                # Fallback: das <img> innerhalb des <a> (Server-HTML hat absolute src)
                if not img_url:
                    img_inside = a.find("img")
                    if img_inside:
                        src = img_inside.get("src", "").strip()
                        if src.startswith("https://"):
                            img_url = src

                if img_url:
                    # Alle Größen → XXL konvertieren
                    img_url_xxl = _to_xxl(img_url)
                    picture_links.append(img_url_xxl)
                    if img_url_xxl != img_url:
                        logger.info(f"[{num}] Bild {idx} (XXL): {img_url_xxl}")
                    else:
                        logger.info(f"[{num}] Bild {idx}: {img_url_xxl}")

        # Fallback: Hauptbild aus id="currentImage" (wenn noch keine Bilder)
        if not picture_links:
            main_img = soup.find("img", id="currentImage")
            if main_img:
                src = main_img.get("src", "").strip()
                if src.startswith("https://"):
                    src_xxl = _to_xxl(src)
                    picture_links.append(src_xxl)
                    logger.info(f"[{num}] Fallback currentImage (XXL): {src_xxl}")

        # Letzter Fallback: Suche nach JEDEM Bild von images.booklooker.de, das verdächtig aussieht (u/ oder x/)
        if not picture_links:
            all_imgs = soup.find_all("img")
            for img in all_imgs:
                src = img.get("src", "").strip()
                if "images.booklooker.de" in src and ("/u/" in src or "/x/" in src or "/ut" in src or "/xt" in src):
                    src_xxl = _to_xxl(src)
                    if src_xxl not in picture_links:
                        picture_links.append(src_xxl)
                        logger.info(f"[{num}] Ultimate Fallback Booklooker-Logo/Photo (XXL): {src_xxl}")
                        break # Nur eines nehmen, wenn wir raten

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

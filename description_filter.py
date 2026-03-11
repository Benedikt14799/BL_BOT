# description_filter.py
"""
Filtert Beschreibungstexte vor dem eBay-Upload.
Entfernt Kontaktdaten, externe Links und Blacklist-Phrasen,
die gegen eBay-Richtlinien verstoßen.

Blacklist-Phrasen werden aus filter_config.json geladen und
können ohne Code-Änderung erweitert werden.
"""
import re
import os
import json
import logging

logger = logging.getLogger(__name__)

# ── Statische Muster ────────────────────────────────────────
PATTERN_EMAIL = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)
PATTERN_PHONE = re.compile(
    r'(\+49|0)[\s\-/]?(\d[\s\-/]?){7,14}'
)
PATTERN_EXTERNAL_URL = re.compile(
    r'https?://(?!www\.ebay\.)[^\s<\"\')]*'
)

# ── Blacklist aus Config laden ──────────────────────────────
_config_path = os.path.join(os.path.dirname(__file__), "filter_config.json")
_blacklist_regexes: list[re.Pattern] = []

def _load_blacklist():
    """Lädt die Blacklist-Phrasen aus filter_config.json."""
    global _blacklist_regexes
    try:
        with open(_config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        phrases = data.get("blacklist_phrases", [])
        _blacklist_regexes = [re.compile(p, re.IGNORECASE) for p in phrases]
        logger.debug(f"Beschreibungsfilter: {len(_blacklist_regexes)} Blacklist-Phrasen geladen.")
    except FileNotFoundError:
        logger.warning("filter_config.json nicht gefunden – kein Blacklist-Filter aktiv.")
    except Exception as e:
        logger.error(f"Fehler beim Laden von filter_config.json: {e}")

# Beim Import einmalig laden
_load_blacklist()


def reload_config():
    """Ermöglicht ein Neuladen der Config zur Laufzeit (z.B. aus der GUI)."""
    _load_blacklist()


def filter_description(text: str) -> str:
    """
    Bereinigt einen Beschreibungstext für den eBay-Upload.
    
    1. Entfernt ganze Sätze, die Blacklist-Phrasen enthalten
    2. Entfernt E-Mail-Adressen
    3. Entfernt Telefonnummern  
    4. Entfernt externe URLs (alles außer ebay.*)
    5. Bereinigt überschüssige Whitespace/Satzzeichen
    """
    if not text:
        return ""

    # 1) Sätze mit Blacklist-Phrasen entfernen
    #    Satz-Trennung: nach . ! ? (auch wenn mehrere Leerzeichen)
    sentences = re.split(r'(?<=[.!?])\s+', text)
    filtered = []
    for sentence in sentences:
        is_blocked = False
        for pattern in _blacklist_regexes:
            if pattern.search(sentence):
                is_blocked = True
                logger.debug(f"Blacklist-Satz entfernt: '{sentence[:60]}...'")
                break
        if not is_blocked:
            filtered.append(sentence)

    text = " ".join(filtered)

    # 2) E-Mail-Adressen entfernen
    text = PATTERN_EMAIL.sub("", text)

    # 3) Telefonnummern entfernen
    text = PATTERN_PHONE.sub("", text)

    # 4) Externe URLs entfernen (alles außer ebay.*)
    text = PATTERN_EXTERNAL_URL.sub("", text)

    # 5) Bereinigung: doppelte Leerzeichen, lose Satzzeichen, etc.
    text = re.sub(r'\s{2,}', ' ', text)           # Mehrfach-Spaces
    text = re.sub(r'\s+([.!?,;:])', r'\1', text)   # Space vor Satzzeichen
    text = re.sub(r'([.!?])\s*\1+', r'\1', text)   # Doppelte Satzzeichen
    text = text.strip()

    return text

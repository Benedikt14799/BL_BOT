# Booklooker Arbitrage Bot

Ein asynchroner, hochperformanter Web-Scraper, der Buchdaten von Booklooker.de automatisiert extrahiert, mit professionellen Metadaten der Deutschen Nationalbibliothek (DNB) anreichert und in eine PostgreSQL/Supabase-Datenbank zur weiteren Verarbeitung für eBay-Listings überführt.

## Features
- **High-Performance Scraping**: Komplett asynchron mit `aiohttp` und `BeautifulSoup` (mit performantem `lxml`-Parser).
- **Datenveredelung (Rich Data)**: 
  - Automatische Anreicherung der DNB-API mit verifizierten Buch-Detailinformationen wie z. B. exakten Seitenanzahlen.
  - Generierung von eleganten, conversion-optimierten HTML-Templates für eBay.
- **Intelligente Qualitätskontrolle**:
  - Filtert Verkäufer auf Booklooker streng heraus, falls deren Positiv-Bewertung unter 98% fällt.
  - Fehlende Produktbilder (DNB) oder unleserliche ISBNs werden revisionssicher geloggt und aussortiert (`missing_listings`).
- **Skalierbare Cloud-Datenbank**: Native Anbindung über den Supabase Session Pooler an PostgreSQL mittels `asyncpg`.

## Requirements
- Python 3.9+
- PostgreSQL oder Supabase-Account
- Siehe `requirements.txt` für Python-Pakete (z. B. `aiohttp`, `beautifulsoup4`, `asyncpg`, `python-dotenv`, `lxml`)

## Installation & Setup

1. **Repository klonen**
```bash
git clone https://github.com/DeinBenutzername/Booklooker-Arbitrage-Bot.git
cd Booklooker-Arbitrage-Bot
```

2. **Abhängigkeiten installieren**
```bash
pip install -r requirements.txt
```

3. **Umgebungsvariablen einrichten**
Erstelle eine Datei namens `supabase.env.txt` (oder `.env`) im Root-Verzeichnis mit folgendem Inhalt (der `DATABASE_URL` Pfad sollte den Supabase Session Pooler auf Port 5432 enthalten):
```env
DATABASE_URL=postgresql://postgres.[ProjectRef]:[DeinPasswort]@aws-1-[Region].pooler.supabase.com:5432/postgres
```

4. **Bot starten**
In `main.py` konfigurieren, welche initialen Such-Links gescraped werden sollen (aktuell in Zeile 54 anpassbar).
```bash
python main.py
```

## Architektur-Übersicht

- `main.py`: Koordinator des gesamten Asynchron-Prozesses. Initialisiert die Datenbankverbindung und führt das Scraping durch.
- `database.py`: Verwaltet das PostgreSQL/Supabase Schema. Implementiert Migrations-Logik sowie inserts und updates auf der `library`-Tabelle.
- `scrape.py`: Das Herzstück des Crawler. Ruft Übersichtsseiten und Artikelseiten ab, filtert irrelevante Angebote aus und loggt Fehlermeldungen.
- `bl_processing.py`: Normalisiert die von Booklooker ausgelesenen Eigenschaften, bewertet die Qualität des Verkäufers und erzeugt am Ende das eBay-Template-HTML.
- `isbn_processing.py`: Verarbeitet die ISBN (10 und 13), bereinigt diese und verknüpft sie mit den XML-Metadaten der Deutschen Nationalbibliothek (DNB).
- `picture_processing.py`: Zuständig für die massenhafte Prüfung und das Herunterladen von Coverbildern über Booklooker und die DNB.

## Architektur-Logik bei unvollständigen Daten
Datensätze, die die Kriterien (z. B. `<98%` Händlerbewertung, unlesbare ISBN oder komplett fehlende Bilder) nicht erfüllen, werden durchgängig als atomare Transaktion in PostgreSQL verarbeitet. Dabei wird der mangelhafte Datensatz aus der produktiven `library`-Tabelle gelöscht und mit einem klaren Begründungsfeld in die Tabelle `missing_listings` verschoben.

---
**Hinweis**: Diese Software unterliegt beim Abruf von externen Websitedaten stets den aktuellen Nutzungsbedingungen von `booklooker.de` und der Deutschen Nationalbibliothek.`

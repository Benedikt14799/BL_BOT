# Projekt-Struktur & Modul-Übersicht

Dieser Bot ist ein modulares System für Arbitrage und Bestandsmanagement zwischen Booklooker und eBay.

## 1. Kern-Prozess (Listing-Flow)
Diese Module steuern den Weg vom Booklooker-Link zum eBay-Inserat.

- **`main.py`**: Der zentrale Orchestrator. Startet den Prozess (Links einlesen -> Scrapen -> Analysieren -> optional Upload).
- **`scrape.py`**: Sammelt die Daten von Booklooker. Findet Angebots-Links in Kategorien und ruft die Detailseiten ab.
- **`bl_processing.py`**: Normalisiert die Daten (z.B. Zustände, Formate) und generiert daraus die eBay-Titel und HTML-Beschreibungen.
- **`isbn_processing.py`**: Validiert ISBNs und reichert Daten über die Deutsche Nationalbibliothek (DNB) an, falls möglich.
- **`price_processing.py`**: Die Preis-Logik. Berechnet Gebühren, Margen und prüft die Profitabilität gegen Markt-Mediane.
- **`picture_processing.py`**: Extrahiert Bilder von Booklooker und bereitet sie für eBay vor.
- **`ebay_upload.py`**: Kommuniziert mit der eBay Inventory API (Angebote erstellen, Bilder hochladen, Preise ändern).
- **`ebay_template.py`**: Enthält das HTML-Layout für die eBay-Beschreibungen.
- **`description_filter.py`**: Filtert unerwünschte Begriffe aus Produktbeschreibungen.

## 2. Monitoring & Automatisierung
Dienste, die im Hintergrund oder zur Wartung laufen.

- **`sync_service.py`**: Ein 24/7 Hintergrund-Dienst. Prüft regelmäßig auf Verkäufe bei Booklooker (404-Fehler) und beendet dann eBay-Angebote. Meldet Status via Telegram.
- **`cleanup_overpriced_listings.py`**: Analysiert den gesamten Bestand auf eBay, gleicht ihn mit neuen Marktpreisen ab und delistet unrentable Bücher.
- **`ebay_token_manager.py` / `get_refresh_token.py`**: Verwalten die OAuth2-Zugangstoken für die eBay API.

## 3. Benutzeroberfläche & Analyse
- **`gui.py`**: Die grafische Oberfläche (Tkinter) für die lokale Steuerung des Bots am PC.
- **`ebay_analytics.py`**: Ruft Statistiken und Rate-Limits von eBay ab (wird von der GUI genutzt).

## 4. System & Datenbank
- **`database.py`**: Datenbank-Schema und Zugriffsmethoden (PostgreSQL/Supabase).
- **`db_column_audit.py`**: Analyse-Tool zum Auffinden ungenutzter Spalten.
- **`db_cleanup_columns.py`**: Skript zur einmaligen Bereinigung unbenutzter Datenbank-Spalten.

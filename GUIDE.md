# 📘 BL BOT – Das Ultimative Handbuch

Herzlich willkommen zum BL BOT! Dieses Tool automatisiert den gesamten Prozess vom Finden rentabler Bücher auf **Booklooker (BL)** bis zum Upload und Bestandsabgleich auf **eBay**.

---

## 🚀 1. Schnellstart

### Schritt 1: Datenbank wecken (Wichtig!)
Da du den Bot länger nicht benutzt hast, wurde dein Supabase-Projekt wahrscheinlich pausiert.
*   Gehe zum [Supabase Dashboard](https://supabase.com/dashboard/projects).
*   Wähle das Projekt `afkzizkujltqqnnbohoh`.
*   Klicke auf **"Restore Project"**.

### Schritt 2: Anwendung starten
Starte den Bot über die Konsole:
```powershell
python gui.py
```

---

## 🛠️ 2. Die Benutzeroberfläche (GUI)

Der Bot ist in vier Hauptbereiche unterteilt:

### 📊 Scraper Dashboard
*   **Scraping Starten:** Beginnt den Suchlauf basierend auf deinen Links.
*   **Urlaubs-Reaktivierung:** Prüft, welche Bücher wieder online sind, nachdem ein Verkäufer aus dem Urlaub zurückgekehrt ist.
*   **Bestands- & Preis-Sync:** Gleicht Preise und Verkäufe zwischen eBay und Booklooker ab.
*   **Bestandsabgleich (eBay):** Bereinigt die lokale Datenbank, falls Artikel auf eBay manuell gelöscht wurden.

### 📦 Upload Manager
Hier siehst du alle "aktiven" und rentablen Bücher, die bereit für eBay sind.
*   Wähle Artikel aus und klicke auf **"Ausgewählte Hochladen"**.
*   Der Bot kümmert sich um Bilder, Beschreibung (HTML-Template) und Kategorisierung.

### 🔗 Links
*   Füge hier Booklooker-Such-URLs ein (z.B. eine Kategorie oder ein spezieller Filter).
*   Der Bot arbeitet diese Liste systematisch ab.

### ⚙️ Settings
Hier konfigurierst du deine API-Keys (eBay), deine Datenbank-URL und deine finanziellen Parameter (Margen, Fixkosten).

---

## 💰 3. Preislogik & Profitabilität

Der BL BOT rechnet knallhart, damit du am Ende wirklich Gewinn machst. Hier ist die Logik hinter jedem Preis:

### Die Gebühren-Struktur
Der Bot kalkuliert mit folgenden Abzügen vom eBay-Endpreis:
1.  **eBay Provision:** 12,8% + 0,35 € Pauschale.
2.  **Fixkosten-Umlage:** Deine monatlichen Shop-Gebühren (z.B. 100€) geteilt durch die erwarteten Verkäufe (z.B. 300).
3.  **Retouren-Puffer:** 2% des Preises + 2% Anteil an Rücksendekosten (3,50 €).
4.  **Umsatzsteuer:** Differenzbesteuerung oder voller Satz (einstellbar).

### Margen-Staffel (Netto-Gewinn nach allen Abzügen)
Der Bot setzt Preise so, dass folgende Mindestmargen erreicht werden:
*   **Günstige Bücher (< 15 €):** Mindestens **1,50 €** Gewinn.
*   **Mittlere Bücher (15 - 40 €):** Mindestens **15%** vom Preis (oder mind. 2,00 €).
*   **Teure Bücher (> 40 €):** Mindestens **15%** vom Preis.

> [!TIP]
> **Psychologisches Runden:** Der Bot rundet jeden berechneten Mindestpreis automatisch auf die nächste **xx,99 €** Stufe auf, um die Klickrate auf eBay zu erhöhen.

### Strategien beim Konkurrenz-Check
Wenn du einen eBay-Token hinterlegt hast, prüft der Bot die Konkurrenz:
*   **Hoher Druck (> 10 Anbieter):** Bot unterbietet den günstigsten Preis um 0,01 €.
*   **Mittlerer Markt (3-10 Anbieter):** Bot orientiert sich am Median (Durchschnitt) und nimmt 95% davon.
*   **Monopol / Selten:** Bot nutzt einen Sicherheits-Aufschlag (Rarity Factor) auf deinen Einkaufspreis.

---

## 🛡️ 4. Verkäuferschutz (Privat-Anbieter)

Wenn der Bot ein Buch von einem **Privatverkäufer** auf Booklooker findet, ist er besonders vorsichtig:
1.  Er sucht automatisch nach **Backups** (Ersatz-Anbietern) für dasselbe Buch (gleiche ISBN).
2.  **B1 (Privat):** Ein anderes privates Angebot, das auch profitabel ist.
3.  **B2 (Gewerblich):** Ein gewerbliches Angebot (Sicherheit), das mindestens kostendeckend ist.
4.  **Kein Backup?** Das Buch wird **nicht** hochgeladen, um zu verhindern, dass du bei einem Verkauf ohne Ware dastehst.

---

## 📝 5. Wartung & Troubleshooting

*   **Token Refresh:** eBay-Token laufen alle 18 Monate ab. Sollten Logs zeigen, dass der Token ungültig ist, nutze das Skript `get_refresh_token.py`.
*   **Datenbank-Reset:** Im Settings-Tab kannst du die Datenbank komplett leeren. **Achtung: Dies löscht alle gescrapten Daten!**
*   **Log-Überwachung:** Das Live-Log Fenster im Dashboard zeigt dir genau, warum ein Buch eventuell "gefiltert" wurde (z.B. Marge zu gering oder Verkäufer-Rating < 98%).

---

> [!IMPORTANT]
> Achte immer darauf, dass dein **EBAY_ENV** in den Settings auf `PRODUCTION` steht, wenn du echte Artikel verkaufen willst. Im `SANDBOX` Modus finden keine echten Transaktionen statt.

Viel Erfolg beim Verkaufen! 📈

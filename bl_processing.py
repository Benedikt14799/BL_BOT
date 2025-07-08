import re  # Modul für reguläre Ausdrücke
from bs4 import BeautifulSoup


class PropertyToDatabase:
    """
    Klasse: PropertyToDatabase
    ---------------------------
    Verarbeitet PropertyItems aus einer Webseite und speichert sie
    in die passende Spalte der `library`-Datenbank.
    """

    # Aktualisiertes Mapping der Property-Namen zu den entsprechenden Spalten in der Tabelle
    # HINWEIS: Hier sind zwei Keys "Titel:" drin – im Python-Dict überschreibt der zweite
    # den ersten. Die Sonderbehandlung der Spalten 'Title' und 'Buchtitel' erfolgt daher
    # über einen if-check in `insert_properties_to_db`.
    PROPERTY_MAPPING = {
        "Titel:": "Buchtitel",
        "Titel:": "Title",  # Achtung: Überschreibt denselben Key im Dictionary
        "Zustand:": "Condition_ID",
        "Verlag:": "Verlag",
        "Format:": "CFormat",
        "Auflage:": "Ausgabe",
        "Sprache:": "Sprache",
        "Stichwörter:": "Thematik",
        "Autor/in:": "Autor",
        "vom Autor signiert:": "Signiert_von",
        "Einband:": "Produktart",
        "Erschienen:": "Erscheinungsjahr"
    }

    @staticmethod
    def truncate_title(title: str, max_length: int = 80) -> str:
        """
        Kürzt den Titel so, dass Wörter nicht zerschnitten werden.
        Wenn der Titel länger als `max_length` ist, wird er auf das letzte vollständige Wort
        innerhalb der maximalen Länge gekürzt.

        Parameter:
        - title (str): Der Originaltitel.
        - max_length (int): Maximale Länge des Titels (Standard: 80 Zeichen).

        Rückgabe:
        - str: Der gekürzte Titel.
        """
        if len(title) <= max_length:
            return title

        # Kürze den Titel auf die maximale Länge
        truncated = title[:max_length]

        # Schneide bis zum letzten Leerzeichen, um ein vollständiges Wort zu erhalten
        if " " in truncated:
            return truncated.rsplit(" ", 1)[0]
        else:
            return truncated

    @staticmethod
    def truncate_to_max_length(text: str, max_length: int = 65) -> str:
        """
        Kürzt einen Text so, dass kein Wort in der Mitte abgeschnitten wird und
        die Länge `max_length` nicht überschreitet.

        Parameter:
        - text (str): Der Originaltext.
        - max_length (int): Maximale erlaubte Länge (Standard: 65 Zeichen).

        Rückgabe:
        - str: Der gekürzte Text.
        """
        if len(text) <= max_length:
            return text  # Falls die Länge bereits passt, ändere nichts

        truncated = text[:max_length]  # Schneide grob auf max. Länge
        if " " in truncated:
            return truncated.rsplit(" ", 1)[0]  # Kürze bis zum letzten Leerzeichen
        return truncated  # Falls kein Leerzeichen gefunden wurde, gib einfach das Maximum zurück

    @staticmethod
    async def process_and_save(soup: BeautifulSoup, num: int, db_pool):
        """
        Führt die Extraktion und Speicherung von PropertyItems in einem Schritt aus.
        """
        # 1) Extrahiere die Properties aus dem HTML
        properties = PropertyExtractor.extract_property_items(soup)

        # 2) Speichere die Properties + Description in der Datenbank
        if properties:
            await PropertyToDatabase.insert_properties_to_db(properties, num, db_pool)

    @staticmethod
    async def insert_properties_to_db(properties: dict, num: int, db_pool):
        try:
            db_columns = []
            db_values = []

            # 1) Baue anhand des PROPERTY_MAPPING die zu setzenden Spalten und Werte
            for property_name, db_column in PropertyToDatabase.PROPERTY_MAPPING.items():
                # Falls die Property im `properties`-Dictionary fehlt, nutze "Keine Angabe"
                property_value = properties.get(property_name, "Keine Angabe")

                # Sonderbehandlung für Thematik (Max. 65 Zeichen, aber kein Wort abschneiden)
                if property_name == "Stichwörter:":
                    property_value = PropertyToDatabase.truncate_to_max_length(property_value, 65)

                # Sonderbehandlung für Titel (wir wollen 'Title' gekürzt und 'Buchtitel' ungekürzt)
                elif property_name == "Titel:":
                    db_columns.append("Title")
                    db_values.append(PropertyToDatabase.truncate_title(property_value))
                    db_columns.append("Buchtitel")
                    db_values.append(property_value)

                    # Sonderbehandlung für Zustand (mapping auf Condition_ID)
                elif property_name == "Zustand:":
                    property_value = PropertyToDatabase.map_condition(property_value)
                    db_columns.append(db_column)
                    db_values.append(property_value)

                else:
                    db_columns.append(db_column)
                    db_values.append(property_value)

            # 2) Generische Beschreibung generieren
            description_html = PropertyToDatabase.build_description_html(properties)

            # 3) Description-Feld hinzufügen
            db_columns.append("Description")
            db_values.append(description_html)

            if not db_columns:
                print(f"Keine gültigen Spalten für Artikel {num} gefunden.")
                return False

            # 4) SQL-Query dynamisch erstellen (nur 1x UPDATE)
            sql_query = f"""
                UPDATE library
                SET {', '.join(f"{col} = ${i + 1}" for i, col in enumerate(db_columns))}
                WHERE id = ${len(db_columns) + 1}
            """

            # 5) Query ausführen
            async with db_pool.acquire() as conn:
                await conn.execute(sql_query, *db_values, num)

            print(f"PropertyItems erfolgreich für Artikel {num} gespeichert.")
            return True

        except Exception as e:
            print(f"Fehler beim Speichern der PropertyItems für Artikel {num}: {e}")
            return False

    @staticmethod
    def map_condition(condition: str) -> str:
        """
        Funktion: map_condition
        ------------------------
        Ordnet eine Zustandsbeschreibung der entsprechenden ID zu.

        Parameter:
        - condition (str): Die Zustandsbeschreibung (z. B. "wie neu").

        Rückgabe:
        - str: Die zugehörige ID als String (z. B. "2750") oder "Keine Angabe", falls nicht zuordenbar.
        """
        if not condition:
            return "Keine Angabe"

        # Normalisiere die Eingabe
        normalized_condition = condition.strip().lower()

        # Mapping-Tabelle mit normalisierten Werten
        normalized_mapping = {
            "neu, aktuelle ausgabe": "1000-Neuwertig",
            "neuware": "1000-Neuwertig",
            "wie neu": "5000-Gut",
            "leichte gebrauchsspuren": "5000-Gut",
            "deutliche gebrauchsspuren": "6000",
            "stark abgenutzt": "6000"
        }

        return normalized_mapping.get(normalized_condition, "Keine Angabe")

    @staticmethod
    def build_description_html(properties: dict) -> str:
        """
        Funktion: build_description_html
        --------------------------------
        Erstellt eine einfache, generische HTML-Beschreibung auf Basis
        der vorliegenden Properties. Max. ~32.765 Zeichen für Excel-Zellen.

        Parameter:
        - properties (dict): Das Dictionary mit allen Property-Werten,
                             z.B. properties["Titel:"], properties["Autor/in:"], etc.

        Rückgabe:
        - str: Die generierte HTML-Beschreibung.
        """
        try:
            # Wir holen ein paar Felder exemplarisch für die Beschreibung:
            titel = properties.get("Titel:", "Keine Angabe")
            autor = properties.get("Autor/in:", "Keine Angabe")
            verlag = properties.get("Verlag:", "Keine Angabe")
            ausgabe = properties.get("Auflage:", "Keine Angabe")
            sprache = properties.get("Sprache:", "Keine Angabe")
            zustand = properties.get("Zustand:", "Keine Angabe")

            # Einfaches HTML-Template
            # <p> für Absätze, <br> für Zeilenumbruch
            description_html = (
                f"<p><strong>Titel:</strong> {titel}</p>"
                f"<p><strong>Autor/in:</strong> {autor}</p>"
                f"<p><strong>Verlag:</strong> {verlag}</p>"
                f"<p><strong>Ausgabe:</strong> {ausgabe}</p>"
                f"<p><strong>Sprache:</strong> {sprache}</p>"
                f"<p><strong>Zustand:</strong> {zustand}</p>"
                f"<br>"
                f"Weitere Details entnehmen Sie bitte den oben aufgeführten Angaben.</p>"
                f"<p>Viel Freude beim Schmökern!</p>"
            )
            return description_html
        except Exception as e:
            print(f"Fehler beim Erstellen der Beschreibung: {e}")
            return "Keine Beschreibung vorhanden"


class PropertyExtractor:
    """
    Klasse: PropertyExtractor
    --------------------------
    Verwaltet die Extraktion von PropertyItems aus einer Webseite.
    """

    @staticmethod
    def extract_property_items(soup: BeautifulSoup) -> dict:
        """
        Funktion: extract_property_items
        --------------------------------
        Extrahiert alle PropertyItems aus einer Webseite und gibt sie als
        Schlüssel-Wert-Paare in einem Wörterbuch zurück.

        Parameter:
        - soup (BeautifulSoup): Das BeautifulSoup-Objekt der Seite.

        Rückgabe:
        - dict: Ein Wörterbuch mit den extrahierten PropertyItems
                (Eigenschaftsname → Wert).
        """
        try:
            properties = {}  # Initialisiere ein leeres Wörterbuch für die Eigenschaften

            # Suche nach allen div-Tags mit Klassen, die mit 'propertyItem_' beginnen
            property_items = soup.find_all(class_=re.compile(r"propertyItem_\d+"))

            for item in property_items:
                try:
                    # Suche nach dem Element mit dem Eigenschaftsnamen
                    property_name_elem = item.find(class_="propertyName")
                    # Suche nach dem Element mit dem Eigenschaftswert
                    property_value_elem = item.find(class_="propertyValue")

                    # Wenn entweder Name oder Wert fehlt, überspringen
                    if not property_name_elem or not property_value_elem:
                        continue

                    # Extrahiere und bereinige den Text für Name und Wert
                    property_name = property_name_elem.text.strip()
                    property_value = property_value_elem.get_text(separator=" ").strip()

                    # Speichere die Eigenschaft im Wörterbuch
                    properties[property_name] = property_value

                except Exception as e:
                    # Fehlerprotokollierung für einzelne PropertyItems
                    print(f"Fehler beim Extrahieren eines PropertyItems: {e}")

            # Rückgabe des Wörterbuchs mit allen extrahierten Eigenschaften
            return properties
        except Exception as e:
            # Allgemeine Fehlerprotokollierung für die gesamte Extraktion
            print(f"Fehler beim Extrahieren der PropertyItems: {e}")
            return {}

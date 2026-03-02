# bl_processing.py
import re
import logging
from typing import Optional, Tuple, List
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class PropertyToDatabase:
    NORMALIZED_MAPPING = {
        "titel": ["Title", "Buchtitel"],
        "zustand": ["Condition_ID"],
        "verlag": ["Verlag"],
        "format": ["CFormat"],
        "auflage": ["Ausgabe"],
        "sprache": ["Sprache"],
        "stichwörter": ["Thematik"],
        "autor/in": ["Autor"],
        "vom autor signiert": ["Signiert_von"],
        "einband": ["Produktart"],
        "produktart": ["Produktart"],
        "erschienen": ["Erscheinungsjahr"],
        "versanddauer": ["Max_dispatch_time"],
    }

    PRODUCTART_MAP = {
        "paperback": "Taschenbuch",
        "taschenbuch": "Taschenbuch",
        "broschiert": "Taschenbuch",
        "kartoniert": "Taschenbuch",
        "softcover": "Taschenbuch",
        "hardcover": "Hardcover",
        "gebunden": "Hardcover",
        "gebundene ausgabe": "Hardcover",
        "leinen": "Hardcover",
    }

    YEAR_RE = re.compile(r"^(19|20)\d{2}$")

    # Maße-Erkennung (cm/mm)
    DIM_PAIR_CM = re.compile(r"^\s*(\d{1,3}(?:[.,]\d+)?)\s*[x×]\s*(\d{1,3}(?:[.,]\d+)?)\s*cm\s*$", re.IGNORECASE)
    DIM_PAIR_MM = re.compile(r"^\s*(\d{1,4})\s*[x×]\s*(\d{1,4})\s*mm\s*$", re.IGNORECASE)
    DIM_SINGLE_CM = re.compile(r"^\s*(\d{1,3}(?:[.,]\d+)?)\s*cm\s*$", re.IGNORECASE)
    DIM_SINGLE_MM = re.compile(r"^\s*(\d{1,4})\s*mm\s*$", re.IGNORECASE)

    # 3D-Erkennung
    DIM_TRIPLE_MM = re.compile(r"^\s*(\d{1,4})\s*[x×]\s*(\d{1,4})\s*[x×]\s*(\d{1,4})\s*mm\s*$", re.IGNORECASE)
    DIM_TRIPLE_CM = re.compile(r"^\s*(\d{1,3}(?:[.,]\d+)?)\s*[x×]\s*(\d{1,3}(?:[.,]\d+)?)\s*[x×]\s*(\d{1,3}(?:[.,]\d+)?)\s*cm\s*$", re.IGNORECASE)

    # Ausgabe-Erkennung
    AUSGABE_NUM_RE = re.compile(r"^\s*(\d{1,2})\s*\.?\s*(auflage)?\s*$", re.IGNORECASE)

    # Stopwortliste für Thematik-Heuristik
    STOPWORDS = {
        "und", "oder", "mit", "aus", "der", "die", "das", "den", "dem", "des", "ein", "eine", "einem", "einer",
        "im", "am", "vom", "zum", "zur", "für", "auf", "an", "bei", "von", "bis", "nach", "über", "ohne",
        "erleben", "entdecken", "bummeln", "trinken", "übernachten", "ausflüge", "wanderungen", "sehenswertes",
        "seiten", "neu", "aktuell", "jetzt", "bestellen"
    }

    @staticmethod
    def _normalize_key(s: str) -> str:
        s = (s or "").replace("\xa0", " ").strip()
        s = s.rstrip(":").strip()
        s = s.lower()
        return s

    @staticmethod
    def build_ebay_title(props_norm: dict, max_len: int = 80) -> str:
        titel = str(props_norm.get("titel", "")).strip()
        autor = str(props_norm.get("autor/in", "")).strip()
        produktart = str(props_norm.get("produktart", props_norm.get("einband", ""))).strip()

        # Fallback: wenn der Titel ansich schon extrem lang ist, sauber am Wortende kappen (max 80)
        def clean_truncate(s: str, m: int) -> str:
            if len(s) <= m:
                return s
            tmp = s[:m]
            return tmp.rsplit(" ", 1)[0] if " " in tmp else tmp

        components = []
        if titel and titel.lower() != "keine angabe": components.append(titel)
        if autor and autor.lower() != "keine angabe": components.append(f"- {autor}")
        if produktart and produktart.lower() != "keine angabe": components.append(f"({produktart})")

        if not components:
            return "Buch"

        # Von links nach rechts aufbauen, solang < 80 Zeichen
        final_str = components[0]
        # Wenn nur der Titel schon zu lang ist:
        if len(final_str) > max_len:
            return clean_truncate(final_str, max_len)
            
        for c in components[1:]:
            test_str = final_str + " " + c
            if len(test_str) <= max_len:
                final_str = test_str
            else:
                break
                
        return final_str

    @staticmethod
    def truncate_to_max_length(text: str, max_length: int = 65) -> str:
        if len(text) <= max_length:
            return text
        truncated = text[:max_length]
        return truncated.rsplit(" ", 1)[0] if " " in truncated else truncated

    # ---------- CFORMAT (2D/3D) ----------
    @staticmethod
    def _to_mm(value_cm: float) -> int:
        return int(round(value_cm * 10))

    @staticmethod
    def _parse_dimension_pair(text: str) -> Optional[Tuple[int, int]]:
        t = (text or "").strip()
        m = PropertyToDatabase.DIM_PAIR_CM.match(t)
        if m:
            w = PropertyToDatabase._to_mm(float(m.group(1).replace(",", ".")))
            h = PropertyToDatabase._to_mm(float(m.group(2).replace(",", ".")))
            return w, h
        m = PropertyToDatabase.DIM_PAIR_MM.match(t)
        if m:
            return int(m.group(1)), int(m.group(2))
        return None

    @staticmethod
    def _parse_dimension_single(text: str) -> Optional[int]:
        t = (text or "").strip()
        m = PropertyToDatabase.DIM_SINGLE_CM.match(t)
        if m:
            return PropertyToDatabase._to_mm(float(m.group(1).replace(",", ".")))
        m = PropertyToDatabase.DIM_SINGLE_MM.match(t)
        if m:
            return int(m.group(1))
        return None

    @staticmethod
    def _parse_dimension_triple(text: str) -> Optional[Tuple[int, int, int]]:
        t = (text or "").strip()
        m = PropertyToDatabase.DIM_TRIPLE_CM.match(t)
        if m:
            a = PropertyToDatabase._to_mm(float(m.group(1).replace(",", ".")))
            b = PropertyToDatabase._to_mm(float(m.group(2).replace(",", ".")))
            c = PropertyToDatabase._to_mm(float(m.group(3).replace(",", ".")))
            return a, b, c
        m = PropertyToDatabase.DIM_TRIPLE_MM.match(t)
        if m:
            return int(m.group(1)), int(m.group(2)), int(m.group(3))
        return None

    @staticmethod
    def normalize_cformat(raw_value: str) -> Optional[str]:
        """
        Normalisiert Maße:
        - 3D: 'L x B x H mm'
        - 2D: 'Breite x Höhe mm'
        - 1D: 'Höhe mm'
        - sonst: None
        Konsistente Abstände um 'x'.
        """
        if not raw_value:
            return None
        val = re.sub(r"\s+", " ", raw_value).strip()
        val = re.sub(r"\s*[x×]\s*", " x ", val)

        tri = PropertyToDatabase._parse_dimension_triple(val)
        if tri:
            a, b, c = tri
            return f"{a} x {b} x {c} mm"

        pair = PropertyToDatabase._parse_dimension_pair(val)
        if pair:
            w, h = pair
            return f"{w} x {h} mm"

        single = PropertyToDatabase._parse_dimension_single(val)
        if single:
            return f"{single} mm"

        return None

    # ---------- THEMATIK ----------
    @staticmethod
    def normalize_thematik(raw_value: str) -> str:
        """
        Entfernt überflüssige Leerzeichen/Leer-Tokens.
        Wenn keine Kommas vorhanden und Text sehr lang:
        heuristische Keyword-Extraktion (Stopwörter, nur Buchstaben, Länge>=3, Deduplizierung, Limit 12).
        """
        if not raw_value:
            return "Keine Angabe"
        s = str(raw_value).strip()

        # Normalfall mit Kommas
        if "," in s:
            s = re.sub(r"\s*,\s*", ",", s)
            tokens = [t.strip() for t in s.split(",")]
            tokens = [t for t in tokens if t]
            return ", ".join(tokens) if tokens else "Keine Angabe"

        # Heuristik: Fließtext ohne Kommas und lang
        if len(s) >= 60:
            words = re.findall(r"[A-Za-zÄÖÜäöüß]+", s)
            seen = set()
            cleaned = []
            for w in words:
                wl = w.lower()
                if wl in PropertyToDatabase.STOPWORDS:
                    continue
                if len(wl) < 3:
                    continue
                if wl not in seen:
                    seen.add(wl)
                    cleaned.append(w.capitalize())
                if len(cleaned) >= 12:
                    break
            return ", ".join(cleaned) if cleaned else "Keine Angabe"

        # Kurzer Einzelbegriff ohne Kommas -> unverändert
        return s

    # ---------- JAHR ----------
    @staticmethod
    def normalize_year(raw_value: str) -> str:
        """
        Extrahiert eine vierstellige Jahreszahl (1900–2099) aus beliebigem Text.
        Gibt die reine Jahreszahl als String zurück oder 'Keine Angabe'.
        """
        if not raw_value:
            return "Keine Angabe"
        s = str(raw_value).strip()

        m = re.search(r"(19|20)\d{2}", s)
        if not m:
            return "Keine Angabe"

        year = m.group(0)
        try:
            y = int(year)
            if 1900 <= y <= 2099:
                return str(y)
        except ValueError:
            pass
        return "Keine Angabe"

    # ---------- AUSGABE ----------
    @staticmethod
    def normalize_ausgabe(raw_value: str) -> str:
        """
        Vereinheitlicht 'Auflage':
        - '1' / '1.' / '1. Auflage' -> '1. Auflage'
        - 'Neuauflage'/'neu' -> 'Neuauflage'
        - 'Überarbeitet'/'aktualisiert' -> 'Überarbeitete Auflage'
        - kurze freie Texte (<=25 Zeichen) bleiben, Kapitalisierung freundlich
        - Datums-/Mischfragmente -> 'Keine Angabe'
        """
        if not raw_value:
            return "Keine Angabe"
        s = str(raw_value).strip().lower()

        if re.search(r"neu(auflage)?", s):
            return "Neuauflage"

        if re.search(r"(überarbeitet|aktualisiert)", s):
            return "Überarbeitete Auflage"

        m = PropertyToDatabase.AUSGABE_NUM_RE.match(s)
        if m:
            num = m.group(1)
            try:
                n = int(num)
                if 1 <= n <= 50:
                    return f"{n}. Auflage"
            except ValueError:
                pass

        if len(s) <= 25 and not re.search(r"\d{1,2}/\d{4}", s):
            return s.capitalize()

        return "Keine Angabe"

    # ---------- PRODUKTART ----------
    @staticmethod
    def normalize_productart(raw_value: str) -> str:
        if not raw_value:
            return "Keine Angabe"
        s = str(raw_value).strip().lower()

        # Maße nicht als Produktart behandeln
        if (PropertyToDatabase._parse_dimension_triple(s) or
            PropertyToDatabase._parse_dimension_pair(s) or
            PropertyToDatabase._parse_dimension_single(s)):
            return raw_value

        return PropertyToDatabase.PRODUCTART_MAP.get(s, raw_value)

    @staticmethod
    def infer_productart_if_missing(current_productart: str, cformat_value: Optional[str], title: Optional[str], thematik: Optional[str]) -> str:
        pa = (current_productart or "").strip()
        if pa and pa.lower() != "keine angabe":
            return current_productart

        if cformat_value and "mm" in cformat_value:
            return "Taschenbuch"

        hints: List[str] = []
        if title:
            hints.append(title.lower())
        if thematik:
            hints.append(thematik.lower())

        hint_text = " ".join(hints)
        keywords = ["reiseführer", "reise", "guide", "pocket", "merian", "lonely planet", "dumont", "polyglott"]
        if any(k in hint_text for k in keywords):
            return "Taschenbuch"

        return current_productart or "Keine Angabe"

    @staticmethod
    async def process_and_save(soup: BeautifulSoup, num: int, db_pool, extra_props: dict = None):
        properties = PropertyExtractor.extract_property_items(soup)
        if extra_props:
            properties.update(extra_props)
            
        if not properties:
            logger.info(f"[{num}] Keine PropertyItems gefunden – überspringe Update.")
            return False
        return await PropertyToDatabase.insert_properties_to_db(properties, num, db_pool)

    @staticmethod
    async def insert_properties_to_db(properties: dict, num: int, db_pool):
        try:
            db_columns = []
            db_values = []

            normalized_props = {}
            for raw_key, val in properties.items():
                key_norm = PropertyToDatabase._normalize_key(raw_key)
                normalized_props[key_norm] = val

            # Check seller rating
            val_bewertung = normalized_props.get("verkaeufer_bewertung")
            if val_bewertung:
                m = re.search(r"(\d+[.,]\d+)", val_bewertung)
                if m:
                    pct = float(m.group(1).replace(",", "."))
                    if pct < 98.0:
                        return "schlechte_bewertung"
                else:
                    m = re.search(r"(\d+)", val_bewertung)
                    if m:
                        pct = float(m.group(1))
                        if pct < 98.0:
                            return "schlechte_bewertung"

            unmapped = []
            temp_values = {}

            for key_norm, val in normalized_props.items():

                # Thematik
                if key_norm == "stichwörter":
                    val = PropertyToDatabase.normalize_thematik(val)

                # Buchtitel explizit speichern (wird am Ende nochmal für generierten Titel genutzt)
                if key_norm == "titel":
                    temp_values["Buchtitel"] = val
                    db_columns += ["Buchtitel"]
                    db_values  += [temp_values["Buchtitel"]]
                    continue

                # Zustand
                if key_norm == "zustand":
                    val = PropertyToDatabase._map_condition(val)

                # Produktart
                if key_norm in ("produktart", "einband"):
                    val = PropertyToDatabase.normalize_productart(val)

                # Erscheinungsjahr
                if key_norm == "erschienen":
                    val = PropertyToDatabase.normalize_year(val)

                target_cols = PropertyToDatabase.NORMALIZED_MAPPING.get(key_norm)
                if target_cols:
                    target_col = target_cols[0]

                    # cformat – Maße normalisieren
                    if target_col == "CFormat":
                        cf = PropertyToDatabase.normalize_cformat(str(val))
                        if cf:
                            val = cf
                        temp_values["CFormat"] = val

                    if target_col == "Thematik":
                        temp_values["Thematik"] = val

                    db_columns.append(target_col)
                    db_values.append(val)
                else:
                    unmapped.append((key_norm, val))

            # Produktart-Heuristik
            current_pa = None
            if "Produktart" in db_columns:
                idx = max(i for i, c in enumerate(db_columns) if c == "Produktart")
                current_pa = db_values[idx]
                inferred = PropertyToDatabase.infer_productart_if_missing(
                    current_pa, temp_values.get("CFormat"),
                    temp_values.get("Buchtitel") or temp_values.get("Title"),
                    temp_values.get("Thematik")
                )
                if inferred != current_pa:
                    db_values[idx] = inferred
            else:
                inferred = PropertyToDatabase.infer_productart_if_missing(
                    "Keine Angabe", temp_values.get("CFormat"),
                    temp_values.get("Buchtitel") or temp_values.get("Title"),
                    temp_values.get("Thematik")
                )
                if inferred and inferred != "Keine Angabe":
                    db_columns.append("Produktart")
                    db_values.append(inferred)

            # Ebay Title erzeugen
            ebay_title = PropertyToDatabase.build_ebay_title(normalized_props)
            db_columns.append("Title")
            db_values.append(ebay_title)

            # Beschreibung
            description_html = PropertyToDatabase.build_description_html(normalized_props)
            db_columns.append("Description")
            db_values.append(description_html)

            if not db_columns:
                logger.info(f"[{num}] Keine gültigen Spalten – kein Update.")
                return False

            set_clause = ", ".join(f"{col} = ${i+1}" for i, col in enumerate(db_columns))
            sql = f"""
                UPDATE library
                   SET {set_clause}
                 WHERE id = ${len(db_columns)+1}
            """
            async with db_pool.acquire() as conn:
                await conn.execute(sql, *db_values, num)

            if unmapped:
                logger.debug(f"[{num}] UNMAPPED properties: {unmapped[:8]}{' ...' if len(unmapped) > 8 else ''}")

            logger.info(f"[{num}] PropertyItems gespeichert ({len(db_columns)} Spalten aktualisiert).")
            return True

        except Exception as e:
            logger.error(f"[{num}] Fehler beim Speichern der PropertyItems: {e}")
            return False

    @staticmethod
    def _map_condition(condition: str) -> str:
        if not condition:
            return "5000-Gut"  # Fallback

        norm = PropertyToDatabase._normalize_key(condition)

        # Heuristik mit Synonymen
        if any(k in norm for k in ["neuwertig", "wie neu", "neuware"]):
            return "1000-Neuwertig"
        if any(k in norm for k in ["sehr gut", "kaum gelesen", "nur geringe spuren", "sehr guter zustand"]):
            return "4000-Sehr gut"
        if any(k in norm for k in ["gut", "leichte gebrauchsspuren", "gebrauchsspuren", "guter zustand"]):
            return "5000-Gut"
        if any(k in norm for k in ["akzeptabel", "deutliche gebrauchsspuren", "stärkere spuren", "befriedigend"]):
            return "6000-Akzeptabel"
        if any(k in norm for k in ["stark abgenutzt", "stark gebraucht", "beschädigt", "wasserschaden", "mangelhaft"]):
            return "7000-Stark gebraucht"

        # Fallback-Mapping
        mapping = {
            "neu, aktuelle ausgabe": "1000-Neuwertig",
            "neuware": "1000-Neuwertig",
            "wie neu": "1000-Neuwertig",
            "leichte gebrauchsspuren": "5000-Gut",
            "deutliche gebrauchsspuren": "6000-Akzeptabel",
            "stark abgenutzt": "7000-Stark gebraucht",
        }
        return mapping.get(norm, "5000-Gut")

    @staticmethod
    def build_description_html(properties_norm: dict) -> str:
        def val(k):
            v = properties_norm.get(k)
            return str(v).strip() if v and str(v).strip().lower() != "keine angabe" else ""

        titel   = val("titel")
        autor   = val("autor/in")
        verlag  = val("verlag")
        ausgabe = val("auflage")
        sprache = val("sprache")
        zustand = val("zustand")
        
        orig_beschr = val("beschreibung") or val("beschreibungstext") or val("artikelbeschreibung")
        zustands_text = val("erhaltungszustand_detail")
        seitenanzahl = val("seitenanzahl")
        abstract = val("abstract")

        html = f"""
        <div style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 800px; margin: 0 auto; line-height: 1.6; color: #333;">
            <div style="background-color: #f8f9fa; border: 1px solid #e9ecef; border-left: 5px solid #3498db; padding: 20px; border-radius: 4px; margin-bottom: 25px;">
                <h2 style="margin-top: 0; color: #2c3e50; font-size: 22px; border-bottom: 1px solid #ddd; padding-bottom: 10px; margin-bottom: 15px;">{titel if titel else "Buchdetails"}</h2>
                <table style="width: 100%; border-collapse: collapse; font-size: 15px;">
                    <tbody>
        """
        if autor: html += f"<tr><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea; width: 30%; color: #555;'><strong>Autor/in:</strong></td><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea;'>{autor}</td></tr>"
        if verlag: html += f"<tr><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea; width: 30%; color: #555;'><strong>Verlag:</strong></td><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea;'>{verlag}</td></tr>"
        if ausgabe: html += f"<tr><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea; width: 30%; color: #555;'><strong>Auflage:</strong></td><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea;'>{ausgabe}</td></tr>"
        if sprache: html += f"<tr><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea; width: 30%; color: #555;'><strong>Sprache:</strong></td><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea;'>{sprache}</td></tr>"
        if seitenanzahl: html += f"<tr><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea; width: 30%; color: #555;'><strong>Seiten:</strong></td><td style='padding: 8px 0; border-bottom: 1px solid #eaeaea;'>{seitenanzahl}</td></tr>"
        
        html += """
                    </tbody>
                </table>
            </div>
            
            <div style="margin-bottom: 25px;">
                <h3 style="color: #2c3e50; font-size: 18px; border-bottom: 2px solid #3498db; display: inline-block; padding-bottom: 3px; margin-bottom: 15px;">Informationen zum Zustand</h3>
        """
        
        if zustand:
            html += f"<p style='margin-bottom: 8px;'><strong>Erhaltungszustand:</strong> <span style='background-color: #e8f4fd; padding: 3px 10px; border-radius: 4px; color: #2980b9;'>{zustand}</span></p>"
        if zustands_text:
            html += f"<div style='background-color: #fdfbf7; border-left: 4px solid #f39c12; padding: 12px; margin-top: 10px; font-style: italic;'><strong style='color: #d35400;'>Hinweis des Verkäufers:</strong> {zustands_text}</div>"
            
        html += "</div>"
        
        beschreibungs_html = ""
        if abstract:
            beschreibungs_html += f"<div style='margin-bottom: 15px;'><h4 style='color: #2c3e50; margin-bottom: 5px; font-size: 16px; margin-top: 0;'>Klappentext / Inhalt</h4><p style='margin-top:0;'>{abstract}</p></div>"
        if orig_beschr:
            beschreibungs_html += f"<div><h4 style='color: #2c3e50; margin-bottom: 5px; font-size: 16px; margin-top: 0;'>Zusatzinformationen des Verkäufers</h4><p style='margin-top:0;'>{orig_beschr}</p></div>"

        if beschreibungs_html:
            html += f"""
            <div style="margin-bottom: 25px;">
                <h3 style="color: #2c3e50; font-size: 18px; border-bottom: 2px solid #3498db; display: inline-block; padding-bottom: 3px; margin-bottom: 15px;">Beschreibung</h3>
                <div style="background-color: #fff; border: 1px solid #e9ecef; padding: 15px; border-radius: 4px; text-align: justify; font-size: 15px; color: #444;">
                    {beschreibungs_html}
                </div>
            </div>
            """
            
        html += f"""
            <div style="text-align: center; margin-top: 30px; padding-top: 15px; border-top: 1px dashed #ccc; color: #7f8c8d; font-size: 14px;">
                Viel Freude beim Schmökern!
            </div>
        </div>
        """
        
        return html


class PropertyExtractor:
    @staticmethod
    def extract_property_items(soup: BeautifulSoup) -> dict:
        try:
            props = {}
            items = soup.find_all(class_=re.compile(r"propertyItem_\d+"))
            for item in items:
                name_elem = item.find(class_="propertyName")
                value_elem = item.find(class_="propertyValue")
                if not name_elem or not value_elem:
                    continue

                raw_name = name_elem.get_text(separator=" ").strip().replace("\xa0", " ")
                if raw_name and not raw_name.endswith(":"):
                    raw_name = raw_name + ":"

                value = value_elem.get_text(separator=" ").strip()
                props[raw_name] = value

            # Extract Seller Rating
            seller_rating = soup.find(string=re.compile(r'% positiv'))
            if seller_rating:
                props["verkaeufer_bewertung:"] = seller_rating.strip()
                
            # Extract handling time
            dispatch = soup.find(string=re.compile(r'Versandfertig'))
            if dispatch:
                props["versanddauer:"] = dispatch.strip()

            # Condition detailed note (Mängel) - typically found in the standard property items as "Beschreibung:" or "Zustand:"
            # But we can also look for description div if we want
            desc_div = soup.find("div", class_="description")
            if desc_div:
                props["erhaltungszustand_detail:"] = desc_div.text.strip()
            
            return props
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren der PropertyItems: {e}")
            return {}

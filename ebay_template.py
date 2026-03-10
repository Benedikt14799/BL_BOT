def get_condition_metadata(bl_condition: str) -> dict:
    """
    Mapping von Booklooker-Zuständen auf eBay-Anzeige und Farben.
    """
    if not bl_condition:
        return {"text": "Gebraucht", "color": "#6c757d"}
        
    c = bl_condition.lower().strip()
    
    # Mapping-Logik
    if any(x in c for x in ["wie neu", "neu"]):
        return {"text": "Wie neu", "color": "#28a745"}  # Grün
    if "sehr gut" in c:
        return {"text": "Sehr gut", "color": "#28a745"}  # Grün
    if any(x in c for x in ["leichte gebrauchsspuren", "gut"]):
        return {"text": "Gut", "color": "#ffc107"}       # Orange/Gelb
    if any(x in c for x in ["deutliche gebrauchsspuren", "akzeptabel", "stark"]):
        return {"text": "Akzeptabel", "color": "#dc3545"} # Rot
        
    return {"text": bl_condition.capitalize(), "color": "#6c757d"}


def generate_description(data: dict) -> str:
    """
    Generiert ein professionelles, eBay-konformes HTML-Template für Buchbeschreibungen.
    Unterstützt Responsive Design und Fallback-Inline-Styles.
    
    Variablen in data:
    - title: Buchtitel
    - author: Autor
    - publisher: Verlag
    - language: Sprache
    - condition: Erhaltungszustand (Text)
    - condition_color: Badge-Farbe (z.B. #28a745, #ffc107, #dc3545)
    - extra_notes: Zusätzliche Zustandsinfos
    - shipping_cost: Versandkosten Text
    - delivery_time: Lieferzeit Text
    """
    
    # Platzhalter mit Defaults füllen, falls Keys fehlen
    t = data.get('title', 'Unbekannter Titel')
    a = data.get('author', 'Unbekannt')
    p = data.get('publisher', 'Unbekannter Verlag')
    l = data.get('language', 'Deutsch')
    c = data.get('condition', 'Gebraucht')
    cc = data.get('condition_color', '#6c757d') # Default grau
    n = data.get('extra_notes', '')
    sc = data.get('shipping_cost', 'Kostenloser Versand')
    dt = data.get('delivery_time', '1-3 Werktage')

    # Styling-Konstanten für Wiederverwendbarkeit (Inline-Fallback)
    font_family = "font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;"
    main_blue = "#0053a0"
    header_gradient = "background: linear-gradient(135deg, #0053a0 0%, #0073e6 100%);"
    
    html = f"""
<div class="ebay-container" style="{font_family} max-width: 900px; margin: 20px auto; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden; background-color: #ffffff; color: #333; line-height: 1.6;">
    
    <!-- HEADER MIT GRADIENT -->
    <header style="{header_gradient} color: #ffffff; padding: 30px 20px; text-align: center;">
        <h1 style="margin: 0; font-size: 24px; font-weight: 600; text-shadow: 1px 1px 2px rgba(0,0,0,0.2);">{t}</h1>
        <p style="margin: 10px 0 0 0; font-size: 16px; opacity: 0.9;">von {a}</p>
    </header>

    <div style="padding: 25px;">
        
        <!-- SEKTION: BUCHDETAILS -->
        <div style="margin-bottom: 30px;">
            <h2 style="color: {main_blue}; font-size: 18px; border-bottom: 2px solid #f0f0f0; padding-bottom: 8px; margin-bottom: 15px; text-transform: uppercase; letter-spacing: 1px;">Buchdetails</h2>
            <table style="width: 100%; border-collapse: collapse; font-size: 15px;">
                <tr>
                    <td style="padding: 8px 0; font-weight: bold; width: 30%; color: #666;">Verlag:</td>
                    <td style="padding: 8px 0;">{p}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; font-weight: bold; color: #666;">Sprache:</td>
                    <td style="padding: 8px 0;">{l}</td>
                </tr>
            </table>
        </div>

        <!-- SEKTION: ZUSTAND MIT BADGE -->
        <div style="margin-bottom: 30px;">
            <h2 style="color: {main_blue}; font-size: 18px; border-bottom: 2px solid #f0f0f0; padding-bottom: 8px; margin-bottom: 15px; text-transform: uppercase; letter-spacing: 1px;">Zustand</h2>
            <div style="margin-bottom: 10px;">
                <span class="badge" style="background-color: {cc}; color: white; padding: 5px 12px; border-radius: 20px; font-size: 14px; font-weight: bold; display: inline-block;">
                    {c}
                </span>
            </div>
            {f'<p style="margin: 10px 0; font-style: italic; color: #555;">{n}</p>' if n else ''}
        </div>

        <!-- SEKTION: VERSAND & SERVICE -->
        <div style="background-color: #f9f9f9; padding: 20px; border-radius: 6px; border-left: 4px solid {main_blue};">
            <h2 style="color: {main_blue}; font-size: 18px; margin-top: 0; margin-bottom: 15px; text-transform: uppercase; letter-spacing: 1px;">Versand & Service</h2>
            <ul style="margin: 0; padding-left: 20px; list-style-type: square; color: #444;">
                <li style="margin-bottom: 8px;"><strong>Versand:</strong> {sc}</li>
                <li style="margin-bottom: 8px;"><strong>Lieferzeit:</strong> {dt}</li>
                <li style="margin-bottom: 0;"><strong>Rückgabe:</strong> 30 Tage Rückgaberecht</li>
            </ul>
        </div>

    </div>

    <!-- FOOTER -->
    <footer style="background-color: #f1f1f1; padding: 15px; text-align: center; font-size: 12px; color: #888; border-top: 1px solid #e0e0e0;">
        Vielen Dank für Ihren Einkauf! Bei Fragen stehen wir Ihnen jederzeit gerne zur Verfügung.
    </footer>
</div>

<style>
    /* Responsive Optimierungen */
    @media (max-width: 600px) {{
        .ebay-container {{
            margin: 0 !important;
            border-radius: 0 !important;
            border: none !important;
        }}
        header {{
            padding: 20px 15px !important;
        }}
        h1 {{
            font-size: 20px !important;
        }}
    }}
</style>
"""
    return html.strip()

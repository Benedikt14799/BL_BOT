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

    # Styling-Konstanten (kompakter)
    font = "font-family:'Segoe UI',Roboto,Arial,sans-serif;"
    blue = "#0053a0"
    grad = "background:linear-gradient(135deg,#0053a0 0%,#0073e6 100%);"
    
    # Kompaktes HTML (ohne unnötige Leerzeichen/Kommentare)
    html = f"""<div style="{font}max-width:800px;margin:10px auto;border:1px solid #ddd;border-radius:8px;overflow:hidden;background:#fff;color:#333;line-height:1.5;">
<header style="{grad}color:#fff;padding:20px 15px;text-align:center;">
<h1 style="margin:0;font-size:20px;font-weight:600;">{t}</h1>
<p style="margin:5px 0 0;font-size:14px;opacity:0.9;">von {a}</p>
</header>
<div style="padding:20px;">
<div style="margin-bottom:20px;">
<h2 style="color:{blue};font-size:16px;border-bottom:2px solid #eee;padding-bottom:5px;margin-bottom:10px;text-transform:uppercase;letter-spacing:1px;">Details</h2>
<table style="width:100%;border-collapse:collapse;font-size:14px;">
<tr><td style="padding:5px 0;font-weight:bold;width:30%;color:#666;">Verlag:</td><td style="padding:5px 0;">{p}</td></tr>
<tr><td style="padding:5px 0;font-weight:bold;color:#666;">Sprache:</td><td style="padding:5px 0;">{l}</td></tr>
</table>
</div>
<div style="margin-bottom:20px;">
<h2 style="color:{blue};font-size:16px;border-bottom:2px solid #eee;padding-bottom:5px;margin-bottom:10px;text-transform:uppercase;letter-spacing:1px;">Zustand</h2>
<div style="margin-bottom:8px;"><span style="background:{cc};color:#fff;padding:4px 10px;border-radius:15px;font-size:13px;font-weight:bold;display:inline-block;">{c}</span></div>
{f'<p style="margin:5px 0;font-style:italic;color:#555;font-size:14px;">{n}</p>' if n else ''}
</div>
<div style="background:#f9f9f9;padding:15px;border-radius:6px;border-left:4px solid {blue};font-size:14px;">
<h2 style="color:{blue};font-size:16px;margin:0 0 10px;text-transform:uppercase;letter-spacing:1px;">Versand & Service</h2>
<ul style="margin:0;padding-left:18px;color:#444;">
<li style="margin-bottom:5px;"><strong>Versand:</strong> {sc}</li>
<li style="margin-bottom:5px;"><strong>Lieferzeit:</strong> {dt}</li>
<li><strong>Rückgabe:</strong> 30 Tage Rückgaberecht</li>
</ul>
</div>
</div>
<footer style="background:#eee;padding:12px;text-align:center;font-size:11px;color:#777;border-top:1px solid #ddd;">
Vielen Dank für Ihren Einkauf! Bei Fragen stehen wir Ihnen jederzeit gerne zur Verfügung.
</footer>
</div>"""
    return html.strip()

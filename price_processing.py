import logging
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class PriceProcessing:
    """
    Extraktion, Berechnung und Speicherung von Preisen.

    Neue Logik:
    - Gebühren werden auf den finalen eBay-Endpreis p berechnet.
    - Versandkosten von BL werden in p einkalkuliert (eBay-Angebot versandkostenfrei).
    - Mindestmargen-Staffel (p-basiert):
        * p < 12: mindestens 2,50 €
        * 12 ≤ p < 30: mindestens max(3,60 €, 20% von p)
        * p ≥ 30: mindestens 30% von p
    - AdditionalCosts: 0,50 € (Low/Mid), 1,75 € (High)
    - Psychologisches Runden auf x,99 mit anschließendem Re-Check
    - Kein Cap, kein Best Offer (Felder werden auf Startpreis gesetzt)
    - Zusätzlich: Einkaufspreis (Purchase_price) und BL-Versand (Purchase_shipping) werden gespeichert.
    """

    # eBay-Gebühren (Deutschland, gewerblich)
    EBAY_PERCENTAGE_FEE = Decimal('0.128')  # 12.8%
    EBAY_FIXED_FEE      = Decimal('0.35')

    # Rundung
    DECIMAL_PLACES = Decimal('0.01')

    # Segmentschwellen auf Basis des finalen Endpreises p
    PRICE_LOW_MAX  = Decimal('12.00')
    PRICE_MID_MAX  = Decimal('30.00')

    # Zielmargen-Parameter
    LOW_MIN_ABS_MARGIN      = Decimal('2.50')   # p < 12
    MID_MIN_ABS_MARGIN      = Decimal('3.60')   # Untergrenze im Mid-Segment
    MID_MIN_REL_MARGIN      = Decimal('0.20')   # 20% von p
    HIGH_MIN_REL_MARGIN     = Decimal('0.30')   # 30% von p

    # AdditionalCosts je Segment
    ADDCOST_LOW_MID = Decimal('0.50')
    ADDCOST_HIGH    = Decimal('1.75')

    # eBay Browse API Settings
    EBAY_MARKETPLACE_ID = "EBAY_DE"
    EBAY_CATEGORY_BOOKS = "267"

    # Zustandsmapping BL -> eBay Condition IDs
    CONDITION_MAP = {
        'wie neu': ['4000', '2750'],
        'leichte gebrauchsspuren': ['5000', '4000', '2750'],
        'deutliche gebrauchsspuren': ['6000', '5000', '4000', '2750'],
        'akzeptabel': ['6000', '5000', '4000', '2750'],
        'gut': ['5000', '4000', '2750'],
        'sehr gut': ['4000', '2750']
    }

    @staticmethod
    def _safe_extract_condition(soup) -> str:
        """Extrahiert den Zustand (z.B. 'leichte Gebrauchsspuren') aus dem BL-HTML."""
        try:
            import re
            property_items = soup.find_all(class_=re.compile(r"propertyItem_\d+"))
            for item in property_items:
                name_elem = item.find(class_="propertyName")
                val_elem = item.find(class_="propertyValue")
                if name_elem and val_elem:
                    if "Erhaltungszustand" in name_elem.text:
                        return val_elem.text.strip()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Fehler bei Zustands-Extraktion: {e}")
        return 'unbekannt'

    @staticmethod
    async def get_price(
        session: ClientSession,
        soup: BeautifulSoup,
        num: int,
        db_pool,
        token: Optional[str] = None,
        base_url: Optional[str] = None,
        fixed_costs_monthly: Decimal = Decimal('79.95'),
        total_listings: int = 2500,
        min_margin_req: Decimal = Decimal('2.50'),
        addcost_low_mid: Decimal = Decimal('0.50'),
        addcost_high: Decimal = Decimal('1.75')
    ) -> Optional[dict]:
        """
        Ermittelt final_price und Marge und speichert beides. 
        Integriert den Konkurrenz-Check und die Profitabilitäts-Prüfung.
        Gibt das Profitabilitäts-Dict zurück oder None bei Fehlern.
        """
        try:
            # 1. BL-Produkt- und Versandpreis extrahieren
            ek = PriceProcessing._safe_clean_price(soup)
            bl_shipping = PriceProcessing._safe_extract_shipping(soup)
            
            if ek <= Decimal('0.00'):
                logger.warning(f"[{num}] Fehlerhaftes Angebot (Preis 0.00). Überspringe.")
                return None
                
            isbn = PriceProcessing._safe_extract_isbn(soup)
            bl_cond = PriceProcessing._safe_extract_condition(soup)
            
            if not isbn:

                logger.warning(f"[{num}] Keine ISBN auf Booklooker-Seite gefunden. Springe Konkurrenz-Check über.")

            # 2. Konkurrenz-Check (falls Token vorhanden)
            comp_data = {}
            recommended_p = None
            if token and isbn and base_url:
                comp_data = await PriceProcessing.get_competitor_prices(session, isbn, token, base_url, condition=bl_cond)
                if comp_data.get("gefunden"):
                    recommended_p = Decimal(str(comp_data.get("empfohlener_preis", "0")))
                    
                    # Empfohlener Preis wird basierend auf Faktoren (unten) berechnet
                    pass

            # 3. Finalen eBay-Preis p bestimmen
            # Falls Empfehlung vorhanden, nehmen wir diese, sonst Standard-Kalkulation
            if recommended_p and recommended_p > 0:
                final_price = PriceProcessing._round_x99_up(recommended_p)
            else:
                # Dynamischer Faktor für Seltenheit/Monopol
                ek_total = ek + bl_shipping
                strategy = comp_data.get("strategie")
                
                if strategy in ("Seltenheits-Bonus", "Monopol-Stellung"):
                    factor = PriceProcessing._get_rarity_factor(ek_total, strategy)
                    final_price = PriceProcessing._round_x99_up(ek_total * factor)
                else:
                    final_price = PriceProcessing._compute_final_price(ek, bl_shipping, addcost_low_mid, addcost_high)

            if final_price is None:
                raise ValueError("Finalpreis konnte nicht berechnet werden.")

            # 4. Profitabilitäts-Check
            prof = PriceProcessing.calculate_profitability(
                ek=ek,
                bl_shipping=bl_shipping,
                ebay_p=final_price,
                monthly_fixed_costs=fixed_costs_monthly,
                total_listings=total_listings,
                min_margin=min_margin_req,
                addcost_low_mid=addcost_low_mid,
                addcost_high=addcost_high
            )

            # 5. Speichern in DB
            await PriceProcessing._save_to_db(
                db_pool=db_pool,
                num=num,
                final_price=final_price,
                margin=Decimal(str(prof['marge'])),
                purchase_price=ek,
                purchase_shipping=bl_shipping,
                comp_data=comp_data,
                prof_data=prof
            )

            status_str = "✅ Rentabel" if prof['rentabel'] else f"❌ Nicht rentabel (fehlt {prof['fehlende_marge']}€)"
            logger.info(f"[{num}] {status_str} | Preis: {final_price}€ | Marge: {prof['marge']}€ | Strategie: {comp_data.get('strategie', 'Sicherheits-Modus')}")
            
            prof['ebay_p'] = float(final_price)
            return prof

        except Exception as e:
            logger.error(f"[{num}] Kritischer Fehler in PriceProcessing: {e}")
            return None

    @staticmethod
    def _safe_extract_isbn(soup) -> str | None:
        try:
            import re
            from isbn_processing import pick_isbn
            property_items = soup.find_all(class_=re.compile(r"propertyItem_\d+"))
            for item in property_items:
                name_elem = item.find(class_="propertyName")
                val_elem = item.find(class_="propertyValue")
                if name_elem and val_elem:
                    name = name_elem.text.strip()
                    if "ISBN" in name or "EAN" in name:
                        raw = val_elem.text.strip()
                        isbn = pick_isbn(raw)
                        if isbn: return isbn
            return None
        except Exception as e:
            logger.error(f"Fehler bei ISBN-Extraktion: {e}")
            return None

    @staticmethod
    def _safe_clean_price(soup) -> Decimal:
        try:
            import re
            text = soup.find(class_="priceValue").text
            cleaned = re.sub(r'[^\d,]', '', text).replace(',', '.')
            return Decimal(cleaned)
        except Exception:
            logger.warning("Preis-Parsing fehlgeschlagen, setze auf 0.00")
            return Decimal('0.00')


    @staticmethod
    def _safe_extract_shipping(soup) -> Decimal:
        try:
            import re
            text = soup.find(class_="shippingCosts").text
            match = re.search(r'([\d,]+)', text)
            return Decimal(match.group(1).replace(',', '.')) if match else Decimal('0.00')
        except Exception:
            logger.warning("Versandkosten-Parsing fehlgeschlagen, setze auf 0.00")
            return Decimal('0.00')

    @staticmethod
    def _fee_on_price(p: Decimal) -> Decimal:
        fee = (p * PriceProcessing.EBAY_PERCENTAGE_FEE) + PriceProcessing.EBAY_FIXED_FEE
        return fee.quantize(PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP)

    @staticmethod
    def _additional_costs_for_price(p: Decimal, addcost_low_mid: Decimal, addcost_high: Decimal) -> Decimal:
        if p < PriceProcessing.PRICE_LOW_MAX:
            return addcost_low_mid
        if p < PriceProcessing.PRICE_MID_MAX:
            return addcost_low_mid
        return addcost_high

    @staticmethod
    def _get_rarity_factor(ek_total: Decimal, strategy: str) -> Decimal:
        """
        Gibt den degressiven Faktor basierend auf EK+Versand und Strategie zurück.
        Staffelung: <7€, 7-15€, 15-30€, 30-60€, 60-100€, >100€
        """
        if strategy == "Monopol-Stellung":
            if ek_total < Decimal('7.00'):   return Decimal('2.7')
            if ek_total < Decimal('15.00'):  return Decimal('2.3')
            if ek_total < Decimal('30.00'):  return Decimal('2.0')
            if ek_total < Decimal('60.00'):  return Decimal('1.7')
            if ek_total < Decimal('100.00'): return Decimal('1.5')
            return Decimal('1.3')
        
        else: # Seltenheits-Bonus
            if ek_total < Decimal('7.00'):   return Decimal('2.2')
            if ek_total < Decimal('15.00'):  return Decimal('1.9')
            if ek_total < Decimal('30.00'):  return Decimal('1.7')
            if ek_total < Decimal('60.00'):  return Decimal('1.5')
            if ek_total < Decimal('100.00'): return Decimal('1.3')
            return Decimal('1.2')

    @staticmethod
    def _target_margin_for_price(p: Decimal) -> Decimal:
        """
        Ermittelt die Zielmarge M(p) gem. Staffel auf Basis des (aktuellen) Preises p.
        """
        if p < PriceProcessing.PRICE_LOW_MAX:
            return PriceProcessing.LOW_MIN_ABS_MARGIN
        if p < PriceProcessing.PRICE_MID_MAX:
            # max(3,60 €, 20% von p)
            abs_req = PriceProcessing.MID_MIN_ABS_MARGIN
            rel_req = (p * PriceProcessing.MID_MIN_REL_MARGIN).quantize(
                PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP
            )
            return abs_req if abs_req >= rel_req else rel_req
        # p ≥ 30 -> 30%
        return (p * PriceProcessing.HIGH_MIN_REL_MARGIN).quantize(
            PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP
        )

    @staticmethod
    def _compute_final_price(ek: Decimal, bl_shipping: Decimal, addcost_low_mid: Decimal, addcost_high: Decimal) -> Optional[Decimal]:
        """
        Bestimmt den kleinstmöglichen Endpreis p, der die Zielmarge erfüllt,
        wendet psychologisches Runden (x,99) an und prüft danach erneut die Marge.
        """
        try:
            # 1) Grobe Startschätzung
            p_guess = (ek + bl_shipping + addcost_low_mid + Decimal('5.00'))

            # 2) Iterativ p lösen, da Zielmarge im Mid/High von p abhängt
            p = PriceProcessing._solve_price(ek, bl_shipping, p_guess, addcost_low_mid, addcost_high)

            # 3) Psychologisches Runden auf nächste x,99
            p = PriceProcessing._round_x99_up(p)

            # SICHERHEITSNETZ: Verhindert Endlos-Schleife bei sehr teuren Büchern mit hohen Fixkosten 
            ek_total = ek + bl_shipping
            
            if ek_total <= Decimal('5.00'):
                max_allowed_price = min(ek_total * Decimal('6.0'), Decimal('29.99'))
            elif ek_total <= Decimal('10.00'):
                max_allowed_price = min(ek_total * Decimal('5.0'), Decimal('39.99'))
            elif ek_total <= Decimal('15.00'):
                max_allowed_price = min(ek_total * Decimal('4.5'), Decimal('49.99'))
            elif ek_total <= Decimal('20.00'):
                max_allowed_price = min(ek_total * Decimal('4.0'), Decimal('59.99'))
            else:
                max_allowed_price = min(ek_total * Decimal('3.0'), Decimal('149.99'))
            
            while not PriceProcessing._meets_margin(ek, bl_shipping, p, addcost_low_mid, addcost_high):
                p = PriceProcessing._round_x99_up(p + Decimal('0.01'))
                if p > max_allowed_price:
                    logger.warning(f"Preis-Obergrenze ({max_allowed_price}€) während Margen-Check bei EK {ek_total}€ erreicht. Breche Endlos-Schleife ab.")
                    return max_allowed_price # Return capped price, which will fail calculate_profitability if margin is too low, marking as "Unrentabel"
                    
            return p.quantize(PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP)
        except Exception as e:
            logger.error(f"Berechnung final_price schlug fehl. {e}")
            import traceback
            traceback.print_exc()
            return None

    @staticmethod
    def _solve_price(ek: Decimal, bl_shipping: Decimal, p_init: Decimal, addcost_low_mid: Decimal, addcost_high: Decimal) -> Decimal:
        """
        Löst p für die jeweilige Segmentregel. Es gibt drei Fälle:
        - Low (feste Marge M): p >= (M + EK + Versand + AddCosts + fee_fixed) / (1 - fee_rate)
        - Mid (max(3,60, 20% p)): prüfe beide und nimm die strengere
        - High (30% p): p >= (EK + Versand + AddCosts + fee_fixed) / (1 - fee_rate - 0.30)
        Danach wird segmentabhängiger AdditionalCosts-Wert eingesetzt.
        """
        fee_rate = PriceProcessing.EBAY_PERCENTAGE_FEE
        fee_fixed = PriceProcessing.EBAY_FIXED_FEE

        p = p_init
        for _ in range(100):  # Konvergenz-Obergrenze
            add_costs = PriceProcessing._additional_costs_for_price(p, addcost_low_mid, addcost_high)
            base = ek + bl_shipping + add_costs + fee_fixed

            if p < PriceProcessing.PRICE_LOW_MAX:
                # feste Marge
                M = PriceProcessing.LOW_MIN_ABS_MARGIN
                p_new = (M + base) / (Decimal('1.0') - fee_rate)
            elif p < PriceProcessing.PRICE_MID_MAX:
                # zwei Bedingungen: fix und prozentual
                p_fix = (base + PriceProcessing.MID_MIN_ABS_MARGIN) / (Decimal('1.0') - fee_rate)
                denom = (Decimal('1.0') - fee_rate - PriceProcessing.MID_MIN_REL_MARGIN)
                if denom <= 0:
                    denom = Decimal('0.0001')
                p_rel = base / denom
                p_new = p_fix if p_fix >= p_rel else p_rel
            else:
                # High: 30% p
                denom = (Decimal('1.0') - fee_rate - PriceProcessing.HIGH_MIN_REL_MARGIN)
                if denom <= 0:
                    denom = Decimal('0.0001')
                p_new = base / denom

            # Konvergenz prüfen (Segmentwechsel möglich)
            if abs(p_new - p) < Decimal('0.01'):
                p = p_new
                break
            p = p_new

        return p

    @staticmethod
    def _meets_margin(ek: Decimal, bl_shipping: Decimal, p: Decimal, addcost_low_mid: Decimal, addcost_high: Decimal) -> bool:
        # Greift nun für die finale Überprüfung exakt auf calculate_profitability zu, 
        # um eine Drift zwischen Formeln (Fixkosten, Retourepuffer, Add Costs) auszuschließen.
        from decimal import Decimal
        target = PriceProcessing._target_margin_for_price(p)
        
        prof = PriceProcessing.calculate_profitability(
            ek, bl_shipping, p, 
            min_margin=target, # Wir übergeben die hier berechnete, dynamische Zielmarge
            addcost_low_mid=addcost_low_mid,
            addcost_high=addcost_high
        )
        return prof["rentabel"]

    @staticmethod
    def _round_x99_up(p: Decimal) -> Decimal:
        """Rundet auf die nächste .99-Stufe nach oben (z. B. 18.20 -> 18.99, 18.99 -> 19.99)."""
        euros = int(p)
        target = Decimal(euros) + Decimal('0.99')
        if p <= target:
            return target
        else:
            return Decimal(euros + 1) + Decimal('0.99')
    @staticmethod
    async def get_competitor_prices(session: aiohttp.ClientSession, isbn: str, token: str, base_url: str, condition: str = 'unbekannt') -> dict:
        """
        Ruft Konkurrenzpreise über die eBay Browse API ab.
        v2: Zustandsfilterung, Versandkosten-Obergrenze (5€), gestaffelter Seriositätsfilter, Ausreißer-Kappung.
        """
        if not isbn:
            return {"gefunden": False, "grund": "Keine ISBN"}

        # Zustandsmapping
        ebay_condition_ids = PriceProcessing.CONDITION_MAP.get(condition.lower())
        filter_str = "buyingOptions:{FIXED_PRICE}"
        if ebay_condition_ids:
            cond_str = "|".join(ebay_condition_ids)
            filter_str += f",conditionIds:{{{cond_str}}}"

        search_url = f"{base_url}/buy/browse/v1/item_summary/search"
        params = {
            "q": isbn,
            "category_ids": PriceProcessing.EBAY_CATEGORY_BOOKS,
            "filter": filter_str,
            "limit": "50"
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": PriceProcessing.EBAY_MARKETPLACE_ID,
            "Accept": "application/json"
        }

        try:
            async with session.get(search_url, params=params, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.error(f"eBay Browse API Error {resp.status}: {text}")
                    return {"gefunden": False, "status": resp.status}

                data = await resp.json()
                items = data.get("itemSummaries", [])
                
                import logging
                logger = logging.getLogger(__name__)

                if not items:
                    logger.info(f"Keine Konkurrenzangebote auf eBay für ISBN {isbn} gefunden.")
                    return {"gefunden": False, "anzahl_gesamt": 0}

                logger.info(f"eBay Browse API: {len(items)} Angebote für ISBN {isbn} gefunden (Zustand: {condition}).")
                
                parsed_listings = []

                from decimal import Decimal
                for item in items:
                    # Preis + Versand
                    price_val = Decimal(item.get("price", {}).get("value", "0"))
                    shipping_opt = item.get("shippingOptions", [{}])
                    shipping_val = Decimal('0.00')
                    if shipping_opt:
                        ship_cost = shipping_opt[0].get("shippingCost")
                        if ship_cost:
                            shipping_val = Decimal(ship_cost.get("value", "0"))
                    
                    if shipping_val > Decimal('5.00'):
                        continue # Ausreißer ignorieren
                        
                    total = price_val + shipping_val

                    seller = item.get("seller", {})
                    feedback_pct = float(seller.get("feedbackPercentage", "0"))
                    feedback_score = int(seller.get("feedbackScore", "0"))

                    parsed_listings.append({
                        "total": total,
                        "feedback_pct": feedback_pct,
                        "feedback_score": feedback_score
                    })

                # Gestaffelter Seriositäts-Filter
                serious_listings = []
                filter_level = 'none'
                
                for min_reviews, min_score, level_name in [(100, 98.0, 'primary'), (50, 95.0, 'secondary'), (10, 90.0, 'fallback')]:
                    filtered = [lst for lst in parsed_listings if lst['feedback_score'] >= min_reviews and lst['feedback_pct'] >= min_score]
                    if len(filtered) >= 3:
                        serious_listings = filtered
                        filter_level = level_name
                        break
                        
                if not serious_listings:
                    serious_listings = parsed_listings
                    filter_level = 'none'
                
                prices = sorted([lst['total'] for lst in serious_listings])
                
                if not prices:
                    return {"gefunden": False, "anzahl_gesamt": len(parsed_listings), "grund": "Alle ignoriert"}

                # Ausreißer kappen (Median * 3)
                import statistics
                median_preis = Decimal(str(statistics.median([float(p) for p in prices])))
                outlier_limit = median_preis * Decimal('3.0')
                
                preise_bereinigt = [p for p in prices if p <= outlier_limit]
                outlier_removed_count = len(prices) - len(preise_bereinigt)
                
                if not preise_bereinigt:
                     preise_bereinigt = prices
                     outlier_removed_count = 0
                     
                min_price = min(preise_bereinigt)
                median_bereinigt = Decimal(str(statistics.median([float(p) for p in preise_bereinigt])))

                # Strategie-Logik
                count = len(preise_bereinigt)
                strategy = ""
                recommended = Decimal('0.00')

                if count > 10:
                    strategy = "Konkurrenz-Druck"
                    recommended = min_price - Decimal('0.01')
                elif count >= 3:
                    strategy = "Markt-Orientierung"
                    recommended = median_bereinigt * Decimal('0.95')
                else:
                    strategy = "Seltenheits-Bonus" if count > 0 else "Monopol-Stellung"
                    # EK_effektiv wird in der aufrufenden Funktion verarbeitet
                    recommended = Decimal('0.00') 

                return {
                    "gefunden": True,
                    "anzahl_gesamt": count,
                    "anzahl_serioes": len(serious_listings),
                    "min_preis": float(min_price),
                    "median_preis": float(median_bereinigt),
                    "empfohlener_preis": float(recommended),
                    "strategie": strategy,
                    "filter_level": filter_level,
                    "outlier_count": outlier_removed_count,
                    "condition_filter": "|".join(ebay_condition_ids) if ebay_condition_ids else "none"
                }

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Fehler bei get_competitor_prices: {e}")
            return {"gefunden": False, "error": str(e)}
    @staticmethod
    def calculate_profitability(
        ek: Decimal, 
        bl_shipping: Decimal, 
        ebay_p: Decimal,
        monthly_fixed_costs: Decimal = Decimal('79.95'),
        total_listings: int = 2500,
        min_margin: Decimal = Decimal('2.50'),
        addcost_low_mid: Decimal = Decimal('0.50'),
        addcost_high: Decimal = Decimal('1.75')
    ) -> dict:
        """
        Berechnet die Profitabilität basierend auf den Vorgaben:
        - eBay Gebühr 12.8% + 0.35€
        - Fixkosten pro Listing
        - Mindestmarge
        - Retouren-Puffer v2 (2% vk + 2% Retourenkosten)
        """
        if ebay_p <= 0:
            return {"rentabel": False, "grund": "Verkaufspreis 0"}

        # 1. eBay Gebühren (12.8% + 0.35€)
        fee_rate = PriceProcessing.EBAY_PERCENTAGE_FEE
        fee_fixed = PriceProcessing.EBAY_FIXED_FEE
        ebay_fees = (ebay_p * fee_rate) + fee_fixed

        # 2. Fixkosten pro Listing
        if total_listings <= 0: total_listings = 1
        fix_cost_per_item = monthly_fixed_costs / Decimal(str(total_listings))

        # 2b. Add Costs (Verpackung, Etiketten etc.) - Dynamisch nach Preis
        add_costs = PriceProcessing._additional_costs_for_price(ebay_p, addcost_low_mid, addcost_high)

        # 3. Marge berechnen (ohne AdditionalCosts als Teil des Einkaufs)
        # gewinn_brutto = vk - ebay_gebühr - (ek + versand_bl + fixanteil + additional_costs)
        gewinn_brutto = ebay_p - (ek + bl_shipping + ebay_fees + fix_cost_per_item + add_costs)
        
        # 4. Retouren-Puffer (v2)
        from decimal import ROUND_HALF_UP
        retouren_quote = Decimal('0.02')
        retouren_kosten = Decimal('3.50')
        retouren_puffer = (ebay_p * retouren_quote) + (retouren_kosten * retouren_quote)
        
        gewinn_real = gewinn_brutto - retouren_puffer

        needed_diff = min_margin - gewinn_real
        is_rentable = gewinn_real >= min_margin

        return {
            "rentabel": is_rentable,
            "marge": float(gewinn_real.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            "gewinn_brutto": float(gewinn_brutto.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            "retouren_puffer": float(retouren_puffer.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            "fehlende_marge": float(max(Decimal('0'), needed_diff).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            "gebuehren": float(ebay_fees.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            "fixkosten_pro_item": float(fix_cost_per_item.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP))
        }


    @staticmethod
    async def _save_to_db(
        db_pool,
        num: int,
        final_price: Decimal,
        margin: Decimal,
        purchase_price: Decimal,
        purchase_shipping: Decimal,
        comp_data: dict = None,
        prof_data: dict = None
    ):
        """Speichert alle Kalkulations- und Konkurrenzdaten in der Datenbank."""
        # Best Offer Logik: 5% Rabatt erlauben
        from decimal import ROUND_HALF_UP
        auto_accept_p = (final_price * Decimal('0.95')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        async with db_pool.acquire() as conn:
            # Standard-Updates
            sql = """
                UPDATE library
                SET Start_price                  = $1,
                    Minimum_Best_Offer_Price     = $2,
                    Best_Offer_Auto_Accept_Price = $2,
                    Margin                       = $3,
                    Purchase_price               = $4,
                    Purchase_shipping            = $5
            """
            params = [final_price, auto_accept_p, margin, purchase_price, purchase_shipping]
            
            # Konkurrenz-Daten ergänzen
            if comp_data:
                sql += """,
                    competitor_min_preis      = $6,
                    competitor_median_preis   = $7,
                    empfohlener_ebay_preis    = $8,
                    anzahl_konkurrenzangebote = $9,
                    last_competitor_check     = NOW(),
                    ebay_condition_filter     = $10,
                    competitor_filter_level   = $11,
                    outlier_removed_count     = $12
                """
                params.extend([
                    comp_data.get("min_preis"),
                    comp_data.get("median_preis"),
                    comp_data.get("empfohlener_preis"),
                    comp_data.get("anzahl_gesamt"),
                    comp_data.get("condition_filter"),
                    comp_data.get("filter_level"),
                    comp_data.get("outlier_count")
                ])
            
            # Profitabilitäts-Daten ergänzen
            if prof_data:
                idx = len(params) + 1
                sql += f""",
                    rentabel       = ${idx},
                    fehlende_marge = ${idx+1},
                    gewinn_real    = ${idx+2}
                """
                params.extend([
                    prof_data.get("rentabel"), 
                    prof_data.get("fehlende_marge"),
                    prof_data.get("marge") # In v2 ist prof_data['marge'] der gewinn_real
                ])

            # WHERE Clause
            idx = len(params) + 1
            sql += f" WHERE id = ${idx}"
            params.append(num)

            await conn.execute(sql, *params)

import re
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

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
    EBAY_PERCENTAGE_FEE = Decimal('0.12')  # 12%
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

    @staticmethod
    async def get_price(
        session: ClientSession,
        soup: BeautifulSoup,
        num: int,
        db_pool
    ) -> Optional[Decimal]:
        """
        Ermittelt final_price und Marge und speichert beides. Best Offer wird aktuell nicht verwendet.
        Zusätzlich werden Purchase_price (EK) und Purchase_shipping (BL-Versand) gespeichert.
        Gibt final_price als Decimal zurück oder None bei kritischem Fehler.
        """
        try:
            # BL-Produkt- und Versandpreis extrahieren (EK und Versandkostenbasis)
            ek = PriceProcessing._safe_clean_price(soup)
            bl_shipping = PriceProcessing._safe_extract_shipping(soup)

            # Finalen eBay-Preis p mit neuer Logik bestimmen (inkl. psychologischer Rundung)
            final_price = PriceProcessing._compute_final_price(ek, bl_shipping)
            if final_price is None:
                raise ValueError("Finalpreis konnte nicht berechnet werden.")

            # Gebühren und Marge auf Basis des finalen Preises berechnen
            fee = PriceProcessing._fee_on_price(final_price)
            add_costs = PriceProcessing._additional_costs_for_price(final_price)
            margin = (final_price - (ek + bl_shipping + add_costs + fee)).quantize(
                PriceProcessing.DECIMAL_PLACES,
                rounding=ROUND_HALF_UP
            )

            # Speichern in DB: Preis-/Marge-Felder sowie EK/BL-Versand
            await PriceProcessing._save_to_db(
                db_pool=db_pool,
                num=num,
                final_price=final_price,
                min_offer=final_price,       # Best Offer deaktiviert => Felder = Startpreis
                auto_accept=final_price,     # Best Offer deaktiviert => Felder = Startpreis
                margin=margin,
                purchase_price=ek,
                purchase_shipping=bl_shipping
            )

            logger.info(f"[{num}] Preis: {final_price} €, Marge: {margin} € (EK={ek}, BL-Versand={bl_shipping}, AddCosts={add_costs}, Fee={fee})")
            return final_price

        except Exception as e:
            logger.error(f"[{num}] Kritischer Fehler in PriceProcessing: {e}")
            return None

    @staticmethod
    def _safe_clean_price(soup: BeautifulSoup) -> Decimal:
        try:
            text = soup.find(class_="priceValue").text
            cleaned = re.sub(r'[^\d,]', '', text).replace(',', '.')
            return Decimal(cleaned)
        except Exception:
            logger.warning("Preis-Parsing fehlgeschlagen, setze auf 0.00")
            return Decimal('0.00')

    @staticmethod
    def _safe_extract_shipping(soup: BeautifulSoup) -> Decimal:
        try:
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
    def _additional_costs_for_price(p: Decimal) -> Decimal:
        if p < PriceProcessing.PRICE_LOW_MAX:
            return PriceProcessing.ADDCOST_LOW_MID
        if p < PriceProcessing.PRICE_MID_MAX:
            return PriceProcessing.ADDCOST_LOW_MID
        return PriceProcessing.ADDCOST_HIGH

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
    def _compute_final_price(ek: Decimal, bl_shipping: Decimal) -> Optional[Decimal]:
        """
        Bestimmt den kleinstmöglichen Endpreis p, der die Zielmarge erfüllt,
        wendet psychologisches Runden (x,99) an und prüft danach erneut die Marge.
        """
        try:
            # 1) Grobe Startschätzung
            p_guess = (ek + bl_shipping + PriceProcessing.ADDCOST_LOW_MID + Decimal('5.00'))

            # 2) Iterativ p lösen, da Zielmarge im Mid/High von p abhängt
            p = PriceProcessing._solve_price(ek, bl_shipping, p_guess)

            # 3) Psychologisches Runden auf nächste x,99
            p = PriceProcessing._round_x99_up(p)

            # 4) Re-Check nach Rundung; wenn Zielmarge verfehlt, nächste x,99-Stufe
            while not PriceProcessing._meets_margin(ek, bl_shipping, p):
                p = PriceProcessing._round_x99_up(p + Decimal('0.01'))

            return p.quantize(PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP)
        except Exception:
            logger.warning("Berechnung final_price schlug fehl.")
            return None

    @staticmethod
    def _solve_price(ek: Decimal, bl_shipping: Decimal, p_init: Decimal) -> Decimal:
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
            add_costs = PriceProcessing._additional_costs_for_price(p)
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
    def _meets_margin(ek: Decimal, bl_shipping: Decimal, p: Decimal) -> bool:
        fee = PriceProcessing._fee_on_price(p)
        add_costs = PriceProcessing._additional_costs_for_price(p)
        margin = p - (ek + bl_shipping + add_costs + fee)
        target = PriceProcessing._target_margin_for_price(p)
        return margin >= target

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
    async def _save_to_db(
        db_pool,
        num: int,
        final_price: Decimal,
        min_offer: Decimal,
        auto_accept: Decimal,
        margin: Decimal,
        purchase_price: Decimal,
        purchase_shipping: Decimal
    ):
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE library
                SET Start_price                  = $1,
                    Minimum_Best_Offer_Price     = $2,
                    Best_Offer_Auto_Accept_Price = $3,
                    Margin                       = $4,
                    Purchase_price               = $5,
                    Purchase_shipping            = $6
                WHERE id = $7
                """,
                final_price, min_offer, auto_accept, margin,
                purchase_price, purchase_shipping, num
            )

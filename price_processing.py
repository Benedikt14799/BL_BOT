# price_processing.py
import re
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from aiohttp import ClientSession
from bs4 import BeautifulSoup

from database import DatabaseManager

logger = logging.getLogger(__name__)

class PriceProcessing:
    """
    Extraktion, Berechnung und Speicherung von Preisen und
    relevanten Feldern wie 'Minimum Best Offer Price' etc.
    """

    # eBay-Gebühren und zusätzliche Kosten
    EBAY_PERCENTAGE_FEE = Decimal('0.12')
    EBAY_FIXED_FEE      = Decimal('0.35')
    ADDITIONAL_COSTS    = Decimal('1.75')

    # Gewinnmarge
    PROFIT_MARGIN_PERCENT = Decimal('0.30')
    MINIMUM_PROFIT        = Decimal('2.00')

    # Angebotspreise
    OFFER_MIN_DISCOUNT    = Decimal('0.10')
    OFFER_ACCEPT_DISCOUNT = Decimal('0.05')

    # Rundung
    DECIMAL_PLACES = Decimal('0.01')

    @staticmethod
    async def get_price(
        session: ClientSession,
        soup: BeautifulSoup,
        num: int,
        db_pool
    ) -> Optional[Decimal]:
        """
        Berechnet final_price, min_offer_price, auto_accept_price und speichert sie.
        Gibt final_price als Decimal zurück oder None bei kritischem Fehler.
        """
        try:
            # Produkt- und Versandpreis extrahieren
            price = PriceProcessing._safe_clean_price(soup)
            shipping = PriceProcessing._safe_extract_shipping(soup)

            # Gebühren berechnen
            total_fee = PriceProcessing._calculate_ebay_fee(price, shipping)
            net_purchase = price + shipping + total_fee

            # finalen Verkaufspreis berechnen
            final_price = PriceProcessing._calculate_price(net_purchase)
            if final_price is None:
                raise ValueError("Finalpreis konnte nicht berechnet werden.")

            # Angebotspreise ermitteln
            min_offer     = PriceProcessing._calc_min_offer(final_price)
            auto_accept   = PriceProcessing._calc_auto_accept(final_price)
            if min_offer > auto_accept:
                min_offer = auto_accept

            # Margen-Berechnung (optional, kann bei Bedarf ausgegeben werden)
            margin = (final_price - net_purchase).quantize(
                PriceProcessing.DECIMAL_PLACES,
                rounding=ROUND_HALF_UP
            )

            # Speichern in DB
            await PriceProcessing._save_to_db(
                db_pool, num, final_price, min_offer, auto_accept, margin
            )

            logger.info(f"[{num}] Preis: {final_price} €, Marge: {margin} €")
            return final_price

        except Exception as e:
            # Nur kritische Fehler gelangen hierher
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
    def _calculate_ebay_fee(price: Decimal, shipping: Decimal) -> Decimal:
        fee = (price + shipping) * PriceProcessing.EBAY_PERCENTAGE_FEE
        return (fee + PriceProcessing.EBAY_FIXED_FEE).quantize(
            PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP
        )

    @staticmethod
    def _calculate_price(net_purchase: Decimal) -> Optional[Decimal]:
        try:
            total_costs   = net_purchase + PriceProcessing.ADDITIONAL_COSTS
            profit        = max(
                net_purchase * PriceProcessing.PROFIT_MARGIN_PERCENT,
                PriceProcessing.MINIMUM_PROFIT
            )
            final_price   = (total_costs + profit).quantize(
                PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP
            )
            return final_price
        except Exception:
            logger.warning("Berechnung final_price schlug fehl.")
            return None

    @staticmethod
    def _calc_min_offer(final_price: Decimal) -> Decimal:
        return (final_price * (1 - PriceProcessing.OFFER_MIN_DISCOUNT)).quantize(
            PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP
        )

    @staticmethod
    def _calc_auto_accept(final_price: Decimal) -> Decimal:
        return (final_price * (1 - PriceProcessing.OFFER_ACCEPT_DISCOUNT)).quantize(
            PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP
        )

    @staticmethod
    async def _save_to_db(
        db_pool, num: int,
        final_price: Decimal,
        min_offer: Decimal,
        auto_accept: Decimal,
        margin: Decimal
    ):
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE library
                SET Start_price                 = $1,
                    Minimum_Best_Offer_Price    = $2,
                    Best_Offer_Auto_Accept_Price= $3,
                    Margin                      = $4
                WHERE id = $5
                """,
                final_price, min_offer, auto_accept, margin, num
            )


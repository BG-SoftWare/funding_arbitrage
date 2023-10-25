import decimal
import logging
from decimal import Decimal


def calculate_delta(funding_1: Decimal, funding_2: Decimal, fee_1: Decimal, fee_2: Decimal) -> Decimal:
    delta = 0
    if funding_1 < 0 and funding_2 < 0:
        delta = abs(abs(funding_1) - abs(funding_2))
    elif funding_1 > 0 > funding_2:
        delta = funding_1 - funding_2
    elif funding_1 < 0 < funding_2:
        delta = abs(funding_1 - funding_2)
    elif funding_1 > 0 and funding_2 > 0:
        delta = abs(funding_1 - funding_2)
    return delta - (fee_1 + fee_2) * 2


def long_short_router(ex1, funding_ex1: Decimal, ex2, funding_ex2: Decimal) -> dict:
    if funding_ex1 > funding_ex2:
        return {ex2: "long", ex1: "short"}
    else:
        return {ex1: "long", ex2: "short"}


def calculate_crypto_amount_for_usdt(ex_1, ex_2, ex_1_price: Decimal, ex_2_price: Decimal, usdt_amount: Decimal,
                                     ex1_ticker: str = None, ex2_ticker: str = None,
                                     multiplier_ex1: Decimal = None, multiplier_ex2: Decimal = None):
    if multiplier_ex1 is None:
        if ex1_ticker is None:
            multiplier_ex_1 = ex_1.get_multiplier()
        else:
            multiplier_ex_1 = ex_1.get_multiplier(ex1_ticker)
    else:
        multiplier_ex_1 = multiplier_ex1
    if multiplier_ex2 is None:
        if ex2_ticker is None:
            multiplier_ex_2 = ex_2.get_multiplier()
        else:
            multiplier_ex_2 = ex_2.get_multiplier(ex2_ticker)
    else:
        multiplier_ex_2 = multiplier_ex2
    token_amount_ex_1 = usdt_amount / ex_1_price
    token_amount_ex_2 = usdt_amount / ex_2_price
    if token_amount_ex_1 < multiplier_ex_1 or token_amount_ex_2 < multiplier_ex_2:
        return -1

    else:
        if multiplier_ex_1 > multiplier_ex_2:
            token_amount_ex_1_rounded = (Decimal(token_amount_ex_1).quantize(
                Decimal(str(multiplier_ex_1).rstrip('0').rstrip('.') if "." in str(multiplier_ex_1) else
                        str(multiplier_ex_1)), rounding=decimal.ROUND_DOWN) // multiplier_ex_1) * multiplier_ex_1

            token_amount_ex_2_rounded = (Decimal(token_amount_ex_2).quantize(
                Decimal(str(multiplier_ex_1).rstrip('0').rstrip('.') if "." in str(multiplier_ex_1) else
                        str(multiplier_ex_1)),rounding=decimal.ROUND_DOWN) // multiplier_ex_1) * multiplier_ex_1

            return token_amount_ex_2_rounded if token_amount_ex_1_rounded > token_amount_ex_2_rounded else \
                token_amount_ex_1_rounded

        elif multiplier_ex_1 < multiplier_ex_2:
            token_amount_ex_1_rounded = (Decimal(token_amount_ex_1).quantize(
                Decimal(str(multiplier_ex_2).rstrip('0').rstrip('.') if "." in str(multiplier_ex_2) else
                        str(multiplier_ex_2)),rounding=decimal.ROUND_DOWN) // multiplier_ex_2) * multiplier_ex_2
            token_amount_ex_2_rounded = (Decimal(token_amount_ex_2).quantize(
                Decimal(str(multiplier_ex_2).rstrip('0').rstrip('.') if "." in str(multiplier_ex_2) else
                        str(multiplier_ex_2)),rounding=decimal.ROUND_DOWN) // multiplier_ex_2) * multiplier_ex_2

            return token_amount_ex_2_rounded if token_amount_ex_1_rounded > token_amount_ex_2_rounded else \
                token_amount_ex_1_rounded

        else:
            token_amount_ex_1_rounded = (Decimal(token_amount_ex_1).quantize(
                Decimal(str(multiplier_ex_1).rstrip('0').rstrip('.') if "." in str(multiplier_ex_1) else
                        str(multiplier_ex_1)),rounding=decimal.ROUND_DOWN) // multiplier_ex_1) * multiplier_ex_1
            token_amount_ex_2_rounded = (Decimal(token_amount_ex_2).quantize(
                Decimal(str(multiplier_ex_2).rstrip('0').rstrip('.') if "." in str(multiplier_ex_2) else
                        str(multiplier_ex_2)), rounding=decimal.ROUND_DOWN) // multiplier_ex_2) * multiplier_ex_2

            return token_amount_ex_2_rounded if token_amount_ex_1_rounded > token_amount_ex_2_rounded else \
                token_amount_ex_1_rounded


def calculate_estimate_pnl_percent(funding_ex_1, funding_ex_2, position_amount_ex1,
                                   position_amount_ex2, fee_ex1,
                                   fee_ex2, token_amount, price_long, price_short, leverage):
    logging.info("calculate estimate pnl")
    funding_fee_1 = funding_ex_1 * position_amount_ex1
    funding_fee_2 = funding_ex_2 * position_amount_ex2
    if Decimal(funding_fee_1) < 0 and Decimal(funding_fee_2) < 0 \
            or Decimal(funding_fee_1) > 0 and Decimal(funding_fee_2) > 0:
        sum_funding_fee = abs(abs(Decimal(funding_fee_1)) - abs(Decimal(funding_fee_2)))
    elif Decimal(funding_fee_1) < 0 < Decimal(funding_fee_2) or Decimal(funding_fee_1) > 0 > Decimal(funding_fee_2):
        sum_funding_fee = abs(abs(Decimal(funding_fee_1)) + abs(Decimal(funding_fee_2)))
    else:
        return None
    pnl_usdt = (sum_funding_fee - 2 * (fee_ex1 * position_amount_ex1) - 2 *
                (fee_ex2 * position_amount_ex2) + token_amount * (price_short - price_long))
    pnl_percent = (pnl_usdt / ((position_amount_ex1 + position_amount_ex2) / leverage)) * 100
    return pnl_percent

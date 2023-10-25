import json
from decimal import Decimal


class Position:
    def __init__(self, entry_price: Decimal, margin_type: str, leverage: Decimal, liquidation_price: Decimal,
                 mark_price: Decimal, cum_pnl: Decimal, position_value: Decimal):
        self.entry_price = entry_price
        self.margin_type = margin_type
        self.leverage = leverage
        self.liquidation_price = liquidation_price
        self.mark_price = mark_price
        self.cum_pnl = cum_pnl
        self.position_value = position_value

    def __repr__(self):
        return json.dumps({
            "entry_price": self.entry_price,
            "margin_type": self.margin_type,
            "leverage": self.leverage,
            "liquidation_price": self.liquidation_price,
            "mark_price": self.mark_price,
            "cum_pnl": self.cum_pnl,
            "position_value": self.position_value,
        }, default=str)

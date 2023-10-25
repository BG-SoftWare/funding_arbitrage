import json
from decimal import Decimal


class Income:
    def __init__(self, symbol: str, income_type: str, income: str, asset: str,
                 datetime: int, info: str, tran_id: str, trade_id: str):
        self.symbol = symbol
        self.income_type = income_type
        self.income = Decimal(income)
        self.asset = asset
        self.time = datetime
        self.info = info
        self.tran_id = tran_id
        self.trade_id = trade_id

    def __repr__(self):
        return json.dumps({
            "symbol": self.symbol,
            "incomeType": self.income_type,
            "income": self.income,
            "asset": self.asset,
            "time": self.time,
            "info": self.info,
            "tranId": self.tran_id,
            "tradeId": self.trade_id
        }, default=str)

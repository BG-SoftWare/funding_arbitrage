import logging

from sqlalchemy import MetaData, Table, Integer, DECIMAL, Column, String, DateTime, BigInteger, ForeignKey, Enum
from sqlalchemy import create_engine, insert


class DatabaseConnector:
    meta = MetaData()
    orders = Table(
        "orders", meta,
        Column("id", BigInteger, primary_key=True),
        Column("exchange", String(50)),
        Column("ex_order_id", String(255)),
        Column("side", Enum("BUY", "SELL")),
        Column("contract_quantity", DECIMAL(26, 16)),
        Column("leverage", Integer),
        Column("avg_order_price", DECIMAL(26, 16)),
        Column("fee_amount", DECIMAL(26, 16)),
        Column("usdt_amount", DECIMAL(26, 16)),
        Column("trade_time", DateTime)
    )

    position = Table(
        "position", meta,
        Column("id", BigInteger, primary_key=True),
        Column("position_side", Enum("LONG", "SHORT")),
        Column("entry_order_id", ForeignKey("orders.id")),
        Column("close_order_id", ForeignKey("orders.id")),
        Column("funding_rate", DECIMAL(26, 16)),
        Column("funding_fee", DECIMAL(26, 16))
    )

    trades = Table(
        "trades", meta,
        Column("id", BigInteger, primary_key=True),
        Column("ticker", String(100)),
        Column("position_id_1", BigInteger, ForeignKey("position.id")),
        Column("position_id_2", BigInteger, ForeignKey("position.id")),
        Column("pnl", DECIMAL(10, 5)),
        Column("entry_time", DateTime),
        Column("close_time", DateTime)
    )

    db_match = {"orders": orders,
                "position": position,
                "trades": trades}

    def __init__(self, database_connection_string):
        self.engine = create_engine(database_connection_string)
        self.meta.bind = self.engine
        self.meta.create_all()

    def insert_data(self, table_name: str, data: dict):
        """
        Insert data in required table into database

        :param table_name: str, it is key in db_match; must be orders, position or trades
        :param data: dict that content keys equal to table columns, values -- required values in the right format
        """
        conn = self.engine.connect()
        try:
            conn.execute(insert(self.db_match[table_name]), data)
            print(f"Insert in table {table_name}. Status: SUCCESS")
        except Exception as inserting_exc:
            print(f"Insert in table {table_name}. Status: FAILED")
            print(inserting_exc)
        finally:
            conn.close()
            self.engine.dispose()

    def insert_trade(self, orders_exchange_1, orders_exchange_2, position_side_exchange_1,
                     position_side_exchange_2, pnl, leverage, funding_ex1, funding_ex2, name_ex1, name_ex2,
                     entry_time, close_time, ticker):
        conn = self.engine.connect()
        tx = conn.begin()
        try:
            order_open_exchange_1 = conn.execute(insert(self.orders), [
                {
                    "exchange": name_ex1,
                    "ex_order_id": orders_exchange_1[0].order_id,
                    "side": orders_exchange_1[0].route.upper(),
                    "contract_quantity": orders_exchange_1[0].qty,
                    "leverage": leverage,
                    "avg_order_price": orders_exchange_1[0].avg_order_price,
                    "fee_amount": orders_exchange_1[0].fee,
                    "usdt_amount": orders_exchange_1[0].quote_qty,
                    "trade_time": orders_exchange_1[0].order_time
                }
            ])
            order_close_exchange_1 = conn.execute(insert(self.orders), [
                {
                    "exchange": name_ex1,
                    "ex_order_id": orders_exchange_1[1].order_id,
                    "side": orders_exchange_1[1].route.upper(),
                    "contract_quantity": orders_exchange_1[1].qty,
                    "leverage": leverage,
                    "avg_order_price": orders_exchange_1[1].avg_order_price,
                    "fee_amount": orders_exchange_1[1].fee,
                    "usdt_amount": orders_exchange_1[1].quote_qty,
                    "trade_time": orders_exchange_1[1].order_time
                }
            ])
            position_exchange_1 = conn.execute(insert(self.position), [
                {
                    "position_side": position_side_exchange_1,
                    "entry_order_id": order_open_exchange_1.lastrowid,
                    "close_order_id": order_close_exchange_1.lastrowid,
                    "funding_rate": funding_ex1[0],
                    "funding_fee": funding_ex1[1],
                }
            ])

            order_open_exchange_2 = conn.execute(insert(self.orders), [
                {
                    "exchange": name_ex2,
                    "ex_order_id": orders_exchange_2[0].order_id,
                    "side": orders_exchange_2[0].route.upper(),
                    "contract_quantity": orders_exchange_2[0].qty,
                    "leverage": leverage,
                    "avg_order_price": orders_exchange_2[0].avg_order_price,
                    "fee_amount": orders_exchange_2[0].fee,
                    "usdt_amount": orders_exchange_2[0].quote_qty,
                    "trade_time": orders_exchange_2[0].order_time
                }
            ])
            order_close_exchange_2 = conn.execute(insert(self.orders), [
                {
                    "exchange": name_ex2,
                    "ex_order_id": orders_exchange_2[1].order_id,
                    "side": orders_exchange_2[1].route.upper(),
                    "contract_quantity": orders_exchange_2[1].qty,
                    "leverage": leverage,
                    "avg_order_price": orders_exchange_2[1].avg_order_price,
                    "fee_amount": orders_exchange_2[1].fee,
                    "usdt_amount": orders_exchange_2[1].quote_qty,
                    "trade_time": orders_exchange_2[1].order_time
                }
            ])
            position_exchange_2 = conn.execute(insert(self.position), [
                {
                    "position_side": position_side_exchange_2,
                    "entry_order_id": order_open_exchange_2.lastrowid,
                    "close_order_id": order_close_exchange_2.lastrowid,
                    "funding_rate": funding_ex2[0],
                    "funding_fee": funding_ex2[1],
                }
            ])
            conn.execute(insert(self.trades), [{
                "ticker": ticker,
                "position_id_1": position_exchange_1.lastrowid,
                "position_id_2": position_exchange_2.lastrowid,
                "pnl": pnl,
                "entry_time": entry_time,
                "close_time": close_time,
            }])
            tx.commit()
            logging.info("[DatabaseAdapter] Insert to database was SUCCESSFUL")
        except Exception as e:
            tx.rollback()
            logging.critical("[DatabaseAdapter] Insert to database was FAILED")
            logging.critical(e)
            logging.critical("stacktrace ", exc_info=e)
            raise RuntimeError
        conn.close()
        self.engine.dispose()

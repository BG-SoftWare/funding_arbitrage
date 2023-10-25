import json
import logging
import time
from decimal import Decimal

from libs.screener.screener import ArbitrageChecker
from libs.telegram_bot import BotAlert
from libs.thread_with_return_value import ThreadWithReturnValue
from libs.trade_executor import TradeLogic

main_config = json.load(open("main_config.json", "r"))
credentials = json.load(open(main_config["credentials_json"], "r"))

logging.basicConfig(filename=f"main_thread_{time.strftime('%d-%m-%Y_%H_%M_%S', time.gmtime())}.txt",
                    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())
logging.Formatter(fmt='%(asctime)s.%(msecs)03d', datefmt='%Y-%m-%d,%H:%M:%S')

USDT_AMOUNT = Decimal(main_config["usdt_amount"])
LEVERAGE = Decimal(main_config["leverage"])
ESTIMATED_OPPORTUNITY_THRESHOLD = Decimal(main_config["estimated_pnl"])

try:
    arbitrage_checker = ArbitrageChecker()
    arbitrage_opportunities = arbitrage_checker.find_arbitrage(USDT_AMOUNT, LEVERAGE, credentials)
    logging.info(arbitrage_opportunities)
except BaseException as e:
    logging.exception("something went wrong while checking arbitrage")
    exit()

trade_logics = []
trade_logics_threads = []

bot_alert = BotAlert(main_config["chatid"], main_config["bot_token"])

for opportunity in arbitrage_opportunities:
    if opportunity[13] > Decimal(ESTIMATED_OPPORTUNITY_THRESHOLD):
        trade_logics.append(TradeLogic(opportunity[0], opportunity[1], opportunity[10], opportunity[14],
                                       int(main_config["funding_timeout_secs"]), opportunity[5], opportunity[6],
                                       main_config["db_connection_string"], bot_alert))
        trade_logics_threads.append(ThreadWithReturnValue(
            name=f"{opportunity[0][0]}_{opportunity[1][0]}",
            target=trade_logics[-1].execute_trade,
            args=(opportunity[15], {opportunity[0][0]: opportunity[11], opportunity[1][0]: opportunity[12]})))
        trade_logics_threads[-1].start()


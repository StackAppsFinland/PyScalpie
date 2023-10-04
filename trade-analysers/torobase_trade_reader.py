import logging

import utils
from torobase_event_reader import TorobaseEventReader
from trade import Trade


class TorobaseTradeReader:
    def __init__(self):
        self.filename = utils.PYSCALPIE_PATH / "torobase" / "trades.txt"
        self.trades = []

    def read_trades(self):
        event_reader = TorobaseEventReader()
        events = event_reader.read_events()

        with open(self.filename, 'r') as file:
            while True:
                try:
                    transaction_type = file.readline().strip()
                    if not transaction_type:
                        break
                    currency_pair = file.readline().strip()
                    amount = float(file.readline().strip().split(' ')[0])
                    open_price = float(file.readline().strip())
                    close_price = float(file.readline().strip())
                    status = file.readline().strip()

                    profit_loss = file.readline().strip().split('\t')
                    profit_loss_pips = float(profit_loss[0].split(' ')[0])
                    currency_1 = profit_loss[1].split(' ')
                    profit_loss_curr_1_value = float(currency_1[0])
                    profit_loss_curr_1 = currency_1[1]
                    currency_2 = profit_loss[2].split(' ')
                    profit_loss_curr_2_value = float(currency_2[0])
                    profit_loss_curr_2 = currency_2[1]

                    settlement_id = file.readline().strip()
                    updated = utils.get_datetime_iso(file.readline().strip())
                    created = utils.get_datetime_iso(file.readline().strip())
                    uid = file.readline().strip()

                    # marry up event with UID.
                    event_info = events.get(uid, None)

                    stop_loss = None
                    take_profit = None
                    event_timestamp = None

                    if event_info is None:
                        logging.error(f"Missing event info for UID: {uid}")
                    else:
                        stop_loss = event_info["stop_loss"]
                        take_profit = event_info["take_profit"]
                        event_timestamp = event_info["timestamp"]

                    extracted_trade = Trade(transaction_type, currency_pair, amount, open_price, close_price, status,
                                            profit_loss_pips, profit_loss_curr_1_value, profit_loss_curr_1,
                                            profit_loss_curr_2_value, profit_loss_curr_2, stop_loss, take_profit,
                                            event_timestamp, settlement_id, updated, created, uid)
                    self.trades.append(extracted_trade)
                except Exception as e:
                    print(f"Error occurred: {str(e)}")
                    break

            logging.info(f"Found {len(self.trades)} trades")
            return self.trades


if __name__ == '__main__':
    trade_reader = TorobaseTradeReader()
    trades = trade_reader.read_trades()

    # Displaying the trades
    for trade in trade_reader.trades:
        print(vars(trade))

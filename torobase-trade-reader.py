import logging
from datetime import datetime

import utils
from trade import Trade


class TorobaseTradeReader:
    def __init__(self):
        self.filename = utils.PYSCALPIE_PATH / "torobase" / "trades.txt"
        self.trades = []

    def read_trades(self):
        date_format = "%Y-%m-%d %H:%M:%S.%f"

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
                    updated = datetime.strptime(file.readline().strip(), date_format)
                    created = datetime.strptime(file.readline().strip(), date_format)
                    uid = file.readline().strip()

                    extracted_trade = Trade(transaction_type, currency_pair, amount, open_price, close_price, status,
                                            profit_loss_pips, profit_loss_curr_1_value, profit_loss_curr_1,
                                            profit_loss_curr_2_value, profit_loss_curr_2, settlement_id, updated,
                                            created, uid)
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

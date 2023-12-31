class Trade:
    def __init__(self, transaction_type, currency_pair, amount, open_price, close_price, status, profit_loss_pips,
                 profit_loss_curr_1_value, profit_loss_curr_1, profit_loss_curr_2_value, profit_loss_curr_2,
                 stop_loss, take_profit, event_timestamp, settlement_id, updated, created, uid):
        self.transaction_type = transaction_type
        self.currency_pair = currency_pair
        self.amount = amount
        self.open_price = open_price
        self.close_price = close_price
        self.status = status
        self.profit_loss_pips = profit_loss_pips
        self.profit_loss_curr_1_value = profit_loss_curr_1_value
        self.profit_loss_curr_1 = profit_loss_curr_1
        self.profit_loss_curr_2_value = profit_loss_curr_2_value
        self.profit_loss_curr_2 = profit_loss_curr_2
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.event_timestamp = event_timestamp
        self.trade_duration = updated - event_timestamp
        self.settlement_id = settlement_id
        self.updated = updated
        self.created = created
        self.uid = uid
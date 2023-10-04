import matplotlib.pyplot as plt
import logging

import utils
from torobase_trade_reader import TorobaseTradeReader
from trade import Trade


class TorobaseAnalyzer:

    def analyze(self):
        trade_reader = TorobaseTradeReader()
        trades = trade_reader.read_trades()
        durations_in_seconds = [trade.trade_duration.total_seconds() for trade in trades if
                                trade.profit_loss_curr_1_value > 0]
        max_duration = 120
        bins = [i for i in range(0, int(max_duration) + 1)]
        plt.hist(durations_in_seconds, bins=bins, edgecolor="k", alpha=0.7)
        # Add titles and labels
        plt.title('Trade Duration Histogram')
        plt.xlabel('Duration (seconds)')
        plt.ylabel('Number of Trades')

        # Display the plot
        plt.show()


if __name__ == '__main__':
    analyzer = TorobaseAnalyzer()
    analyzer.analyze()

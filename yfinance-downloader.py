import ccxt
import datetime


class ForexDataFetcher:
    def __init__(self, symbol='EUR/USDT', timeframe='15m', since='2023-09-01T00:00:00Z'):
        self.exchange = ccxt.binance({
            'rateLimit': 1200,
            'enableRateLimit': True,
        })
        self.symbol = symbol
        self.timeframe = timeframe
        self.since = self.exchange.parse8601(since)
        self.data = []

    def fetch_data(self):
        try:
            self.data = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, self.since)
        except ccxt.NetworkError as e:
            print(exchange.id, 'fetch_ohlcv failed due to a network error:', str(e))
        except ccxt.ExchangeError as e:
            print(exchange.id, 'fetch_ohlcv failed due to exchange error:', str(e))
        except Exception as e:
            print(exchange.id, 'fetch_ohlcv failed with:', str(e))

    def display_data(self):
        for candle in self.data:
            print(datetime.datetime.utcfromtimestamp(candle[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'), candle[1:])


if __name__ == '__main__':
    fetcher = ForexDataFetcher()
    fetcher.fetch_data()
    fetcher.display_data()

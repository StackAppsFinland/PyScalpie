import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import time


class BinanceKlineHistoryLoader:
    def __init__(self, connection, symbol: str, interval: str, start_date: str, end_date: str):
        self.base_url = f"{connection['host']}/api/v3/klines"
        self.symbol = symbol
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        logger.info(f"Symbol: {self.symbol}, Interval: {self.interval}, "
                    f"Start Date: {self.start_date}, End Date: {self.end_date}")

    def get_unix_time(self, date_str: str) -> int:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
        return int(dt.timestamp() * 1000)

    def fetch_data(self) -> pd.DataFrame:
        start_time = self.get_unix_time(self.start_date)
        end_time = self.get_unix_time(self.end_date)

        df = pd.DataFrame()
        while start_time < end_time:
            url = f"{self.base_url}?symbol={self.symbol}&interval={self.interval}" \
                  f"&startTime={start_time}&endTime={end_time}"
            logger.info(
                f"Fetching data from {datetime.fromtimestamp(start_time / 1000)} to "
                f"{datetime.fromtimestamp(end_time / 1000)}")

            response = requests.get(url)

            if response.status_code != 200:
                error = f"Failed to fetch data. Response code: {response.status_code}, content: {response.content}"
                logger.error(error)
                raise ConnectionError(error)

            data = response.json()
            if not data:
                logger.info("Failed response from Binance kline, exiting fetch_data loop")
                break

            fetched_rows = len(data)
            logger.info(f"Fetched {fetched_rows} rows of data")

            temp_df = pd.DataFrame(data, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                                  'quote_asset_volume', 'number_of_trades',
                                                  'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                                  'ignore'])

            df = pd.concat([df, temp_df], ignore_index=True)
            start_time = int(temp_df.iloc[-1]['close_time']) + 1  # Get the next candle's start time
            time.sleep(1)  # Delay between requests to avoid being rate-limited

        logger.info(f"Data fetching completed. Total rows fetched: {len(df)}")

        # Convert columns to appropriate data types
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume',
                           'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, axis=1)

        return df

    def find_earliest_candle(self):
        base_url = "https://api.binance.com/api/v3/klines"

        # Start from a very early timestamp
        start_time = datetime(2010, 1, 1)
        while True:
            start_time_unix = int(start_time.timestamp() * 1000)
            url = f"{base_url}?symbol={self.symbol}&interval={self.interval}&startTime={start_time_unix}&limit=1"
            response = requests.get(url)

            if response.status_code != 200:
                raise ConnectionError(f"Failed to fetch data. Response code: {response.status_code}")

            data = response.json()
            if data:  # if data is not empty, then we found the earliest available klines data
                earliest_time = datetime.fromtimestamp(data[0][0] / 1000)
                logger.info(f"Found earliest available klines data for {self.symbol} is from {self.earliest_time}")
                return earliest_time

            start_time += timedelta(days=365)  # increment start time by 365 days and try again


def main():
    symbol = "BTCUSDT"
    interval = "15m"
    start_date = "2023-01-01 00:00:00"
    end_date = "2023-09-22 00:00:00"

    downloader = BinanceKlineHistoryLoader(symbol, interval, start_date, end_date)
    start_date = downloader.find_earliest_candle('BTCUSDT', '1d')
    data = downloader.fetch_data()
    print(data.head())


if __name__ == "__main__":
    main()

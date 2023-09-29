import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import time

from utils import get_unix_time


class BinanceKlineHistoryLoader:
    def __init__(self, connection: str, symbol: str, interval: str):
        self.next_open_time = None
        self.base_url = f"{connection}/api/v3/klines"
        self.symbol = symbol
        self.interval = interval
        interval_mapping = {
            '1m': pd.Timedelta(minutes=1),
            '3m': pd.Timedelta(minutes=3),
            '5m': pd.Timedelta(minutes=5),
            '15m': pd.Timedelta(minutes=15),
            '30m': pd.Timedelta(minutes=30),
            '1h': pd.Timedelta(hours=1)
        }

        time_delta = interval_mapping.get(interval)
        if time_delta is None:
            raise ValueError("Invalid interval. Please choose from '1m', '3m', '5m', '15m', '30m', '1h'")

        self.time_delta_millis = int(time_delta.total_seconds() * 1000)

        logger.info(f"Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_data(self, str_start_date: str) -> pd.DataFrame:
        start_time = get_unix_time(str_start_date)
        retry_count = 0
        current_time_millis = int(time.time() * 1000)

        df = pd.DataFrame()
        while start_time < current_time_millis :
            end_time = start_time + (self.time_delta_millis * 500) - 1
            url = f"{self.base_url}?symbol={self.symbol}&interval={self.interval}" \
                  f"&startTime={start_time}&endTime={end_time}"

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
            logger.info(
                f"Fetched {fetched_rows} rows of data. "
                f"From {datetime.fromtimestamp(start_time / 1000)} to "
                f"{datetime.fromtimestamp(end_time / 1000)}")

            temp_df = pd.DataFrame(data, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                                  'quote_asset_volume', 'number_of_trades',
                                                  'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                                  'ignore'])

            if not df.empty:
                expected_open_time = df.iloc[-1]['open_time'] + self.time_delta_millis
                actual_open_time = temp_df.iloc[0]['open_time']
                if actual_open_time != expected_open_time:
                    #logger.error(
                    #    f"Data gap detected. Expected next open_time: {expected_open_time}, "
                    #    f"but got: {actual_open_time}, will retry in 5 seconds, (attempt number: {retry_count})")
                    retry_count += 1
                    time.sleep(1)
                    if retry_count > 3:
                        logger.info(
                            f"Data gap error here: {datetime.fromtimestamp(start_time / 1000)} to "
                            f"{datetime.fromtimestamp(end_time / 1000)}")
                    else:
                        continue

            if not self.validate(temp_df):
                #logger.error("Found errors in the result, will retry in 5 seconds")
                retry_count += 1
                time.sleep(1)
                if retry_count > 3:
                    logger.info(
                        f"Inconsistent gap error here: {datetime.fromtimestamp(start_time / 1000)} to "
                        f"{datetime.fromtimestamp(end_time / 1000)}")
                else:
                    continue

            retry_count = 0
            df = pd.concat([df, temp_df], ignore_index=True)
            start_time: int = int(temp_df.iloc[-1]['close_time']) + 1  # Get the next candle's start time
            self.next_open_time = start_time
            time.sleep(1)  # Delay between requests to avoid being rate-limited

        logger.info(f"Data fetching completed. Total rows fetched: {len(df)}")

        # Convert columns to appropriate data types
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume',
                           'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, axis=1)

        return df

    def get_last_time(self):
        return self.next_open_time

    def find_earliest_candle(self):
        # Start from a very early timestamp
        start_time = datetime(2010, 1, 1)
        while True:
            start_time_unix = int(start_time.timestamp() * 1000)
            url = f"{self.base_url}?symbol={self.symbol}&interval={self.interval}&startTime={start_time_unix}&limit=1"
            response = requests.get(url)

            if response.status_code != 200:
                raise ConnectionError(f"Failed to fetch data. Response code: {response.status_code}")

            data = response.json()
            if data:  # if data is not empty, then we found the earliest available klines data
                earliest_time = datetime.fromtimestamp(data[0][0] / 1000)
                logger.info(f"Found earliest available klines data for {self.symbol} is from {earliest_time}")
                return earliest_time

            start_time += timedelta(days=365)  # increment start time by 365 days and try again

    def validate(self, df: pd.DataFrame) -> bool:
        for i in range(1, len(df)):
            # Directly compare without type conversion
            current_time_millis = df.iloc[i]['open_time']
            previous_time_millis = df.iloc[i - 1]['open_time']

            if current_time_millis - previous_time_millis != self.time_delta_millis:
                # logger.error(f"Inconsistency found between rows {i - 1} and {i}.")
                return False

        return True


def main():
    symbol = "BTCUSDT"
    interval = "5m"
    downloader = BinanceKlineHistoryLoader("https://api.binance.com", symbol, interval)
    start_date = downloader.find_earliest_candle().strftime("%Y-%m-%d %H:%M")
    data = downloader.fetch_data(start_date)
    print(data.head())


if __name__ == "__main__":
    main()

import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import time

from utils import get_unix_time


class BybitKlineHistoryLoader:
    def __init__(self, connection: str, symbol: str, interval: str):
        self.next_open_time = None
        self.base_url = f"{connection}/public/linear/kline"
        self.symbol = symbol
        self.interval = interval
        self.interval_as_int = int(interval[:-1])
        self.time_delta_seconds = int(self.interval_as_int * 60)

        logger.info(f"Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_data(self, str_start_date: str) -> pd.DataFrame:
        start_time = int(get_unix_time(str_start_date) / 1000)
        retry_count = 0
        current_time_millis = int(time.time())

        df = pd.DataFrame()
        while start_time < current_time_millis:
            params = {
                'symbol': self.symbol,
                'interval': self.interval_as_int,
                'from': start_time,
                'limit': 200
            }
            response = requests.get(self.base_url, params=params)

            if response.status_code != 200:
                error = f"Failed to fetch data. Response code: {response.status_code}, content: {response.content}"
                logger.error(error)
                raise ConnectionError(error)

            data = response.json().get('result', [])
            if not data:
                logger.info("Failed response from Binance kline, exiting fetch_data loop")
                break

            fetched_rows = len(data)
            logger.info(
                f"Fetched {fetched_rows} rows of data. "
                f"From {datetime.fromtimestamp(start_time)}")

            temp_df = pd.DataFrame(data, columns=['open_time', 'open', 'high', 'low', 'close', 'volume',
                                                  'turnover'])

            if not df.empty:
                expected_open_time = int(df.iloc[-1]['open_time'] + self.time_delta_seconds)
                actual_open_time = int(temp_df.iloc[0]['open_time'])
                if actual_open_time != expected_open_time:
                    retry_count += 1
                    time.sleep(1)
                    if retry_count > 3:
                        logger.info(f"Data gap error here: {datetime.fromtimestamp(start_time)}")
                    else:
                        continue

            if not self.validate(temp_df):
                retry_count += 1
                time.sleep(1)
                if retry_count > 3:
                    logger.info(
                        f"Inconsistent gap error here: {datetime.fromtimestamp(start_time)}")
                else:
                    continue

            retry_count = 0
            df = pd.concat([df, temp_df], ignore_index=True)
            start_time = int(temp_df.iloc[-1]['open_time']) + self.time_delta_seconds  # Get the next candle's start time
            self.next_open_time = start_time
            time.sleep(1)  # Delay between requests to avoid being rate-limited

        logger.info(f"Data fetching completed. Total rows fetched: {len(df)}")

        # Convert columns to appropriate data types
        df['open_time'] = pd.to_datetime(df['open_time'], unit='s')
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'turnover']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, axis=1)
        return df

    @property
    def get_last_time(self) -> str:
        dt = datetime.utcfromtimestamp(self.next_open_time)
        next_open_time = dt.strftime("%Y-%m-%d %H:%M")
        return next_open_time

    def find_earliest_candle(self):
        # Start from a very early timestamp
        start_time = datetime(2020, 1, 1)
        while True:
            start_time_unix = int(start_time.timestamp())  # UNIX timestamp in seconds
            params = {
                'symbol': self.symbol,
                'interval': self.interval_as_int,
                'from': start_time_unix,
                'limit': 1
            }
            response = requests.get(self.base_url, params=params)

            if response.status_code != 200:
                error_message = f"Failed to fetch data. Response code: {response.status_code}, content: {response.content}"
                logger.error(error_message)
                raise ConnectionError(error_message)

            data = response.json().get('result', [])
            if data:  # if data is not empty, then we found the earliest available klines data
                earliest_time = datetime.fromtimestamp(data[0]['open_time'])
                logger.info(f"Found earliest available klines data for {self.symbol} is from {earliest_time}")
                return earliest_time

            start_time += timedelta(days=30)  # increment start time by 365 days and try again

    def validate(self, df: pd.DataFrame) -> bool:
        for i in range(1, len(df)):
            # Directly compare without type conversion
            current_time_millis = int(df.iloc[i]['open_time'])
            previous_time_millis = int(df.iloc[i - 1]['open_time'])

            if current_time_millis - previous_time_millis != self.time_delta_seconds:
                return False

        return True


def main():
    symbol = "BTCUSDT"
    interval = "5m"
    downloader = BybitKlineHistoryLoader("https://api.bybit.com", symbol, interval)
    start_date = downloader.find_earliest_candle().strftime("%Y-%m-%d %H:%M")
    data = downloader.fetch_data(start_date)
    print(data.head())


if __name__ == "__main__":
    main()

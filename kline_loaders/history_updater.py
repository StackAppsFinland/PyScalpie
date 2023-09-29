import importlib
import json
import os

from loguru import logger

from kline_loaders.file_handler import FileHandler
import utils


class HistoryUpdater:
    def __init__(self):
        self.connections = self.load_connections() or []
        self.file_handler = FileHandler()

    def load_connections(self):
        filepath = utils.PYSCALPIE_PATH / "connections.json"
        content = utils.load_file(filepath)

        connections_map = {}
        if content:
            try:
                connections_list = json.loads(content)
                for connection in connections_list:
                    name = connection.get('name')
                    if name:
                        connections_map[name] = connection.get('details', {})
                    else:
                        logger.warning("Connection without a name found!")
            except json.JSONDecodeError:
                logger.error(f"Error parsing JSON from {filepath}")

        return connections_map if connections_map else None

    def main(self):
        for connection_name, connection_details in self.connections.items():
            if not connection_details.get('enabled', True):
                continue

            history_symbols = connection_details.get('historySymbols', [])  # might not want history for symbol
            history_intervals = connection_details.get('historyIntervals',
                                                       ["5m"])  # defaulting to 5 mins if not provided

            for symbol in history_symbols:
                for interval in history_intervals:
                    try:
                        class_name = connection_name.capitalize() + 'KlineHistoryLoader'
                        module_name = connection_name + '_kline_history_loader'
                        module = importlib.import_module(module_name)
                        class_ = getattr(module, class_name)

                        latest_date_file = utils.PYSCALPIE_PATH / connection_name / f"{symbol}-{interval}-latest-date"
                        kline_filename = utils.PYSCALPIE_PATH / connection_name / f"{symbol}-{interval}.csv"
                        os.makedirs(utils.PYSCALPIE_PATH / connection_name, exist_ok=True)
                        latest_date_content = utils.file_exists(latest_date_file) and utils.load_file(latest_date_file)

                        start_date = None
                        if latest_date_content:
                            start_date = latest_date_content

                        instance = class_(connection_details['host'], symbol, interval)
                        logger.info(
                            f"Successfully created an instance of {module_name}.{class_name}")

                        if start_date is None:
                            start_date = instance.find_earliest_candle().strftime('%Y-%m-%d %H:%M')
                            logger.info(f"Initial date of {start_date} found")

                        df = instance.fetch_data(start_date)
                        write_header = not utils.file_exists(kline_filename)
                        df.to_csv(kline_filename, mode='a', index=False, header=write_header)
                        with open(latest_date_file, 'w') as file:
                            file.write(instance.get_last_time)
                    except ImportError:
                        logger.error(f"Failed to import {module_name}")
                    except AttributeError as e:
                        logger.error(f"{module_name} does not have a class named {class_name}")
                        raise e
                    except Exception as e:
                        logger.exception(
                            f"An unexpected error occurred while creating an instance of {module_name}.{class_name}", e)


if __name__ == "__main__":
    updater = HistoryUpdater()
    updater.main()

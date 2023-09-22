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
            history_symbols = connection_details.get('historySymbols', [])  # might not want history for symbol
            history_intervals = connection_details.get('historyIntervals', ["5m"])  # defaulting to 5 mins if not provided

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
                        latest_date_content = utils.load_file(latest_date_file)

                        end_date = utils.get_current_datetime()
                        start_date = "2023-09-19 00:00"
                        if latest_date_content is not None:
                            start_date = latest_date_content

                        instance = class_(connection_details, symbol, interval, start_date, end_date)
                        logger.info(
                            f"Successfully created an instance of {module_name}.{class_name}")
                        df = instance.fetch_data()
                        write_header = not utils.file_exists(kline_filename)
                        response = df.to_csv(kline_filename, mode='a', index=False)

                        with open(latest_date_file, 'w') as file:
                            file.write(df.iloc[-1]['close_time'].strftime('%Y-%m-%d %H:%M'))

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

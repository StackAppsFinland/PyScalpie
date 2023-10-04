import os
import zipfile
from datetime import datetime
from pathlib import Path

from loguru import logger

PYSCALPIE_PATH = os.getenv('PYSCALPIE_DIR', Path.home() / ".pyScalpie")
ISO_DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def get_current_datetime():
    now = datetime.now()
    formatted_now = now.strftime("%Y-%m-%d %H:%M")
    return formatted_now


def load_file(filepath):
    content = None
    try:
        if zipfile.is_zipfile(f"{filepath}.zip"):
            with zipfile.ZipFile(f"{filepath}.zip", 'r') as z:
                with z.open(os.path.basename(filepath)) as file:
                    content = file.read().decode('utf-8')
        else:
            with open(filepath, 'r') as file:
                content = file.read()
    except FileNotFoundError:
        logger.error(f"{filepath} not found.")
    except zipfile.BadZipFile:
        logger.error(f"{filepath}.zip is not a valid zip file.")
    except IOError:
        logger.error(f"Could not read {filepath}.")
    except Exception as e:
        logger.exception("An unexpected error occurred")

    return content


def get_unix_time(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
    return int(dt.timestamp() * 1000)


def get_datetime_iso(date_str: str) -> datetime:
    return datetime.strptime(date_str, ISO_DATE_FORMAT)


def save_file(filepath, content, zip_mode=False):
    try:
        if zip_mode:
            with zipfile.ZipFile(f"{filepath}.zip", 'w') as z:
                z.writestr(os.path.basename(filepath), content)
        else:
            with open(filepath, 'w') as file:
                file.write(content)

        logger.info(f"File saved at {filepath}")
    except IOError:
        logger.error(f"Could not write to {filepath}.")
    except Exception as e:
        logger.exception("An unexpected error occurred")


def file_exists(filepath):
    file = Path(filepath)
    zip_file = Path(f"{filepath}.zip")
    return file.exists() or zip_file.exists()


def get_path(filename):
    return None

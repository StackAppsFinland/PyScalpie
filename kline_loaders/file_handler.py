import os
import zipfile
from pathlib import Path

from loguru import logger


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


class FileHandler:
    def __init__(self):
        pass


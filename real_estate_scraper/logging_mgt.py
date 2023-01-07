import logging
import pathlib
from pathlib import Path

LOGS_FILENAME = "{}_logs.txt"
LOGS_FOLDER = "logs"
DEFAULT_LOGS_PATH = Path.cwd()
LOGS_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# dictionary with the COLORS for each log level
COLORS = {
    "TIMER_ACTIVE": "\033[92m",  # green
    "INFO": "\033[94m",  # blue
    "WARNING": "\033[93m",  # yellow
    "ERROR": "\033[91m",  # red
    "CRITICAL": "\033[95m",  # purple
}


# custom log formatter
class ColoredFormatter(logging.Formatter):
    def format(self, record):
        level_name = record.levelname
        color = COLORS[level_name]
        message = logging.Formatter.format(self, record)
        message = f"{color}{message}\033[0m"
        return message


def create_logger(
    name: str = __name__,
    log_level: int = logging.INFO,
    filename: str = None,
    folder_path: pathlib.Path = DEFAULT_LOGS_PATH,
    logs_format: str = LOGS_FORMAT,
) -> logging.Logger:

    if filename is None:
        filename = LOGS_FILENAME.format(name)
    filepath = folder_path / filename

    formatter = logging.Formatter(logs_format)

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(filepath)

    console_handler.setFormatter(ColoredFormatter(logs_format))
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(name)

    logger.setLevel(log_level)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


if __name__ == "__main__":
    logger_test = create_logger(name="funda_logger", filename="funda_logs.txt")

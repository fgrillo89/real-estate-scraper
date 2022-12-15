import logging
from pathlib import Path

LOGS_FILENAME = 'logs.txt'
LOGS_FOLDER = 'logs'
logs_path = Path.cwd().parent / LOGS_FOLDER / LOGS_FILENAME

# Create a custom log formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Create a log handler
handler = logging.StreamHandler()
file_handler = logging.FileHandler(logs_path)

# Set the log formatter for the handler
handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
# Create a logger
logger = logging.getLogger(__name__)

# Set the log level
logger.setLevel(logging.INFO)

# Add the log handler to the logger
logger.addHandler(handler)
logger.addHandler(file_handler)

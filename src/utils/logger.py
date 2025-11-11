import logging, os
from .env_loader import *

LOGS_PATH = os.getenv("LOGS_PATH")

def setup_logger(filename="app.log"):
    path = os.getenv("LOGS_PATH", "./logs")
    os.makedirs(path, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(f"{path}/{filename}", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(filename)
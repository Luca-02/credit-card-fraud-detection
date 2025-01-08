import logging
import os
from config import OUTPUT_DIR


def set_global_logger():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    log_file = os.path.join(OUTPUT_DIR, 'app.log')

    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)


logger = logging.getLogger()

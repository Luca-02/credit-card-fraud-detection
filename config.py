import os


CUSTOMERS_NUM = 5000
"""Represents the number of customer to generate."""

TERMINALS_NUM = 3000
"""Represents the number of terminals to generate."""

START_DATE = '2025-01-01'
"""Represents the starting date of the transactions."""

R = 5
"""Represents the radius within which customers must be located in order to be associated with a terminal."""

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

OUTPUT_DIR = os.path.join(PROJECT_ROOT, ".output")

DATASET_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "dataset")

LOG_DIR = os.path.join(OUTPUT_DIR, "log")

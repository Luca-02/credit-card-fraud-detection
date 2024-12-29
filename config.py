import os

CUSTOMERS_NUM = 5000
"""Represents the number of customer to generate."""

TERMINALS_NUM = 3000
"""Represents the number of terminals to generate."""

START_DATE = '2018-04-01'
"""Represents the starting date of the transactions."""

R = 5
"""Represents the radius within which customers must be located in order to be associated with a terminal."""

project_root = os.path.dirname(os.path.abspath(__file__))

DATASET_OUTPUT_DIR = os.path.join(project_root, ".output", "dataset")

DATASET_TEST_OUTPUT_DIR = os.path.join(project_root, ".output", "dataset_test")

LOG_DIR = os.path.join(project_root, ".output", "log")

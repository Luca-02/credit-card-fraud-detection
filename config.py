import os

CUSTOMERS_NUM = 5000

TERMINALS_NUM = 3000

START_DATE = '2025-01-01'

R = 5

NB_DAYS = 500

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

OUTPUT_DIR = os.path.join(PROJECT_ROOT, ".output")

DATASET_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "dataset")

ANALYSIS_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "analysis")

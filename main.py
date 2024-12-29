from script.generator import Generator
from config import DATASET_OUTPUT_DIR, CUSTOMERS_NUM, TERMINALS_NUM, START_DATE, R
from common.logger import logger


def main():
    generator = Generator(
        dataset_output_path=DATASET_OUTPUT_DIR,
        n_customers=CUSTOMERS_NUM,
        n_terminals=TERMINALS_NUM,
        start_date=START_DATE,
        r=R
    )

    logger.info(f"[START DATASETS GENERATION]")
    nb_days = 100
    generator.generate(arr_nb_days=[nb_days, nb_days * 2, nb_days * 4])
    logger.info(f"[FINISH DATASETS GENERATION]")


if __name__ == "__main__":
    main()

from script.generator import Generator
from config import DATASET_OUTPUT_DIR
from common.logger import logger


def main():
    generator = Generator(
        dataset_output_path=DATASET_OUTPUT_DIR,
        n_customers=10,
        n_terminals=5,
        start_date='2018-04-01',
        r=50
    )

    logger.info(f"[START DATASETS GENERATION]")
    nb_days = 5
    generator.generate(arr_nb_days=[nb_days, nb_days * 2, nb_days * 4])
    logger.info(f"[FINISH DATASETS GENERATION]")


if __name__ == "__main__":
    main()

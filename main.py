from script.generator import Generator
from config import DATASET_OUTPUT_DIR, CUSTOMERS_NUM, TERMINALS_NUM, START_DATE, R
from common.logger import logger


def main():
    generator = Generator(
        n_customers=CUSTOMERS_NUM,
        n_terminals=TERMINALS_NUM,
        start_date=START_DATE,
        r=R
    )

    initial_nb_days = 100
    initial_estimated_mb_size = 50
    for multiplier in [1, 2, 4]:
        nb_days = initial_nb_days * multiplier
        estimated_mb_size = initial_estimated_mb_size * multiplier
        result = generator.generate(DATASET_OUTPUT_DIR, nb_days, dataset_name=f"dataset_{estimated_mb_size}")
        logger.info(f"Time to generate dataset '{result["dataset_name"]}': {result["generation_time"]:.2f}s")


if __name__ == "__main__":
    main()

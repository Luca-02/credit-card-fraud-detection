import os

from dotenv import load_dotenv

from common.logger import logger, set_global_logger
from common.utils import clear_dir_path
from config import OUTPUT_DIR, DATASET_OUTPUT_DIR, CUSTOMERS_NUM, TERMINALS_NUM, START_DATE, R
from script.database import DatabaseInstance
from script.generator import Generator
from script.loader import Loader
from script.operation import Operations


def generate_datasets(generator: Generator,
                      initial_nb_days: int,
                      initial_estimated_mb_size: int,
                      multipliers: list[int]):
    datasets_name = []
    for multiplier in multipliers:
        nb_days = initial_nb_days * multiplier
        estimated_mb_size = initial_estimated_mb_size * multiplier
        dataset_name = f"dataset_{estimated_mb_size}MB"
        datasets_name.append(dataset_name)
        logger.info(f"[GENERATING '{dataset_name}']")

        generation_time = generator.generate(DATASET_OUTPUT_DIR, nb_days, dataset_name)
        logger.info(f"Time to generate dataset '{dataset_name}': {generation_time:.3f}s")

    return datasets_name


def loading_and_operating_datasets(loader: Loader, operations: Operations):
    for dataset_name in os.listdir(DATASET_OUTPUT_DIR):
        logger.info(f"[LOADING and OPERATING '{dataset_name}']")
        dataset_path = os.path.join(DATASET_OUTPUT_DIR, dataset_name)
        loading_time = loader.load_dataset(dataset_path)
        logger.info(f"Time to load dataset '{dataset_name}': {loading_time:.3f}s")

        queries = {
            "a": operations.operation_a,
            "b": operations.operation_b,
            "c": lambda: operations.operation_c(0, 3),
            "d.i": operations.operation_d_i,
            "d.ii": operations.operation_d_ii,
            "e": operations.operation_e
        }

        for name, operation in queries.items():
            execution_time = operation()
            logger.info(f"Time to execute operation [{name}] on dataset '{dataset_name}': {execution_time:.3f}s")


def main():
    load_dotenv()
    clear_dir_path(OUTPUT_DIR)
    set_global_logger()

    db = DatabaseInstance(
        uri=os.getenv("DATABASE_URI"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        database=os.getenv("DATABASE_NAME"),
    )
    generator = Generator(
        n_customers=CUSTOMERS_NUM,
        n_terminals=TERMINALS_NUM,
        start_date=START_DATE,
        r=R
    )
    loader = Loader(db)
    operations = Operations(db)

    generate_datasets(
        generator,
        initial_nb_days=100,
        initial_estimated_mb_size=50,
        multipliers=[1, 2, 4]
    )
    loading_and_operating_datasets(loader, operations)

    db.close()


if __name__ == "__main__":
    main()

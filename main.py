import os
from dotenv import load_dotenv
from common.logger import logger
from common.utils import clear_dir_path
from config import DATASET_OUTPUT_DIR, CUSTOMERS_NUM, TERMINALS_NUM, START_DATE, R
from script.database import DatabaseInstance
from script.generator import Generator
from script.loader import Loader
from script.operation import Operations


def main():
    load_dotenv()
    clear_dir_path(DATASET_OUTPUT_DIR)

    db = DatabaseInstance(
        uri=os.getenv("DATABASE_URI"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        database=os.getenv("DATABASE_NAME")
    )

    # generator = Generator(
    #     n_customers=CUSTOMERS_NUM,
    #     n_terminals=TERMINALS_NUM,
    #     start_date=START_DATE,
    #     r=R
    # )
    generator = Generator(
        n_customers=100,
        n_terminals=50,
        start_date='2024-12-01',
        r=50
    )
    loader = Loader(db)
    operations = Operations(db)

    initial_nb_days = 10
    initial_estimated_mb_size = 50
    for multiplier in [1, 2, 4]:
        nb_days = initial_nb_days * multiplier
        estimated_mb_size = initial_estimated_mb_size * multiplier
        dataset_name = f"dataset_{estimated_mb_size}"
        logger.info(f"[{dataset_name}]")

        # Generation
        res_generator = generator.generate(DATASET_OUTPUT_DIR, nb_days, dataset_name)
        generation_time = res_generator["generation_time"]
        logger.info(f"Time to generate dataset '{dataset_name}': {generation_time:.3f}s")

        # Loading
        dataset_path = os.path.join(DATASET_OUTPUT_DIR, dataset_name)
        loading_time = loader.load_dataset(dataset_path)
        logger.info(f"Time to load dataset '{dataset_name}': {loading_time:.3f}s")

        # Operation
        queries = {
            "a": operations.operation_a,
            "b": operations.operation_b,
            "c": lambda: operations.operation_c(0, 3),
            "d.i": operations.operation_d_i,
            "d.ii": operations.operation_d_ii,
            "e": operations.operation_e
        }
        for name, operation in queries.items():
            execution_time = operation()["execution_time"]
            logger.info(f"Time execute operation [{name}] on dataset '{dataset_name}': {execution_time:.3f}s")

    # TODO: create a plot for time loading/operation

    db.close()


if __name__ == "__main__":
    main()

import os

from dotenv import load_dotenv

from common.logger import logger, set_global_logger
from common.utils import clear_dir_path, create_plot
from config import OUTPUT_DIR, DATASET_OUTPUT_DIR, ANALYSIS_OUTPUT_DIR, \
    CUSTOMERS_NUM, TERMINALS_NUM, START_DATE, R, NB_DAYS
from script.database import DatabaseInstance
from script.generator import Generator
from script.loader import Loader
from script.operations import Operations


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
    time_results = {}
    for dataset_name in ['dataset_50MB', 'dataset_200MB']:
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

        operation_times = {}
        for name, operation in queries.items():
            execution_time = operation()
            operation_times[name] = execution_time
            logger.info(f"Time to execute operation [{name}] on dataset '{dataset_name}': {execution_time:.3f}s")

        time_results[dataset_name] = {
            "loading_time": loading_time,
            "operations": operation_times
        }

    return time_results


def main():
    load_dotenv()
    clear_dir_path(OUTPUT_DIR)
    set_global_logger()

    db = DatabaseInstance(
        uri=os.getenv("DBMS_URI"),
        user=os.getenv("DBMS_USER"),
        password=os.getenv("DBMS_PASSWORD"),
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
        initial_nb_days=NB_DAYS,
        initial_estimated_mb_size=50,
        multipliers=[1, 2, 4]
    )
    time_results = loading_and_operating_datasets(loader, operations)
    create_plot(ANALYSIS_OUTPUT_DIR, time_results)

    db.close()


if __name__ == "__main__":
    main()

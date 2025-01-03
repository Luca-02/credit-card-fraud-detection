import os
import script.operation as operation
from dotenv import load_dotenv
from script.generator import Generator
from script.database import DatabaseInstance
from script.loader import load_dataset
from config import OUTPUT_DIR, DATASET_OUTPUT_DIR
from common.logger import logger
from common.utils import clear_dir_path


def main():
    # generator = Generator(
    #     n_customers=10,
    #     n_terminals=5,
    #     start_date='2024-12-01',
    #     r=50
    # )
    #
    # clear_dir_path(OUTPUT_DIR)
    #
    # nb_days = 50
    # result = generator.generate(DATASET_OUTPUT_DIR, nb_days)
    # logger.info(f"Time to generate dataset '{result["dataset_name"]}': {result["generation_time"]:.2f}s")
    # logger.info(f"Time to save datasets '{result["dataset_name"]}' .csv files locally: {result["csv_saving_time"]:.2f}s")

    dataset_path = os.path.join(DATASET_OUTPUT_DIR, os.listdir(DATASET_OUTPUT_DIR)[0])

    db = DatabaseInstance(
        uri=os.getenv("DATABASE_URI"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        database=os.getenv("DATABASE_NAME")
    )

    load_dataset(db, dataset_path)

    operation.query_c(db, 0, 3)

    db.close()


if __name__ == "__main__":
    load_dotenv()
    main()

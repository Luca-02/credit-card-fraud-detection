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
    clear_dir_path(OUTPUT_DIR)

    generator = Generator(
        n_customers=100,
        n_terminals=50,
        start_date='2024-12-01',
        r=5
    )

    nb_days = 50
    res_generator = generator.generate(
        dataset_output_path=DATASET_OUTPUT_DIR,
        nb_days=nb_days
    )

    logger.info(f"Time to generate dataset '{res_generator["dataset_name"]}': {res_generator["generation_time"]:.2f}s")

    dataset_path = os.path.join(DATASET_OUTPUT_DIR, os.listdir(DATASET_OUTPUT_DIR)[0])

    db = DatabaseInstance(
        uri=os.getenv("DATABASE_URI"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        database=os.getenv("DATABASE_NAME")
    )

    load_dataset(db, dataset_path)

    print(operation.operation_c(db, 31, 5))

    db.close()


if __name__ == "__main__":
    load_dotenv()
    main()

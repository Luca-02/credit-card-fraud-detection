import os
from dotenv import load_dotenv
from script.generator import Generator
from script.database import DatabaseInstance
from script.loader import Loader
from script.operation import Operations
from config import OUTPUT_DIR, DATASET_OUTPUT_DIR
from common.logger import logger
from common.utils import clear_dir_path


def main():
    load_dotenv()
    # clear_dir_path(OUTPUT_DIR)

    db = DatabaseInstance(
        uri=os.getenv("DATABASE_URI"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        database=os.getenv("DATABASE_NAME"),
    )
    generator = Generator(
        n_customers=100,
        n_terminals=50,
        start_date='2024-12-01',
        r=5
    )
    loader = Loader(db)
    operations = Operations(db)

    # generator.generate(
    #     dataset_output_path=DATASET_OUTPUT_DIR,
    #     nb_days=100,
    #     dataset_name="dataset_test"
    # )
    # dataset_path = os.path.join(DATASET_OUTPUT_DIR, "dataset_50MB")
    # loading_time = loader.load_dataset(dataset_path)
    # print(f"Time to load dataset: {loading_time:.3f}s")

    operations.operation_b()

    db.close()


if __name__ == "__main__":
    main()
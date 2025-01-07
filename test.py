import os
from dotenv import load_dotenv
from script.generator import Generator
from script.database import DatabaseInstance
from script.loader import Loader
from script.operation import Operations
from config import OUTPUT_DIR, DATASET_OUTPUT_DIR, ANALYSIS_OUTPUT_DIR
from common.logger import logger
from common.utils import clear_dir_path, create_plot


def main():
    # load_dotenv()
    # # clear_dir_path(OUTPUT_DIR)
    #
    # db = DatabaseInstance(
    #     uri=os.getenv("DATABASE_URI"),
    #     user=os.getenv("DATABASE_USER"),
    #     password=os.getenv("DATABASE_PASSWORD"),
    #     database=os.getenv("DATABASE_NAME"),
    # )
    # generator = Generator(
    #     n_customers=100,
    #     n_terminals=50,
    #     start_date='2024-12-01',
    #     r=5
    # )
    # loader = Loader(db)
    # operations = Operations(db)

    # generator.generate(
    #     dataset_output_path=DATASET_OUTPUT_DIR,
    #     nb_days=100,
    #     dataset_name="dataset_test"
    # )
    # dataset_path = os.path.join(DATASET_OUTPUT_DIR, "dataset_50MB")
    # loading_time = loader.load_dataset(dataset_path)
    # print(f"Time to load dataset: {loading_time:.3f}s")

    # db.close()

    create_plot(ANALYSIS_OUTPUT_DIR, {
        "dataset_50MB": {
            "loading_time": 112.217,
            "operations": {
                "a": 182.398,
                "b": 2.433,
                "c": 4.406,
                "d.i": 8.121,
                "d.ii": 1160.105,
                "e": 0.607
            }
        },
        "dataset_100MB": {
            "loading_time": 316.927,
            "operations": {
                "a": 520.335,
                "b": 4.433,
                "c": 16.392,
                "d.i": 18.482,
                "d.ii": 3494.616,
                "e": 1.090
            }
        },
        "dataset_200MB": {
            "loading_time": 1082.563,
            "operations": {
                "a": 2211.917,
                "b": 7.204,
                "c": 57.730,
                "d.i": 31.476,
                "d.ii": 7550.757,
                "e": 1.816
            }
        }
    })



if __name__ == "__main__":
    main()
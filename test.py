import os
import pprint
from dotenv import load_dotenv
from neo4j import GraphDatabase
from script.generator import Generator
from script.loader import Loader
from config import OUTPUT_DIR, DATASET_OUTPUT_DIR
from common.logger import logger
from common.utils import clear_dir_path


def main():
    generator = Generator(
        n_customers=10,
        n_terminals=5,
        start_date='2018-04-01',
        r=50
    )

    clear_dir_path(OUTPUT_DIR)

    nb_days = 5
    result = generator.generate(DATASET_OUTPUT_DIR, nb_days)
    logger.info(f"Time to generate dataset '{result["dataset_name"]}': {result["generation_time"]:.2f}s")

    dataset_path = os.path.join(DATASET_OUTPUT_DIR, os.listdir(DATASET_OUTPUT_DIR)[0])

    loader = Loader(
        uri=os.getenv("DATABASE_URI"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        database=os.getenv("DATABASE_NAME")
    )

    loader.load_dataset(dataset_path)


def test():
    uri = os.getenv("DATABASE_URI")
    user = os.getenv("DATABASE_USER")
    password = os.getenv("DATABASE_PASSWORD")
    database = os.getenv("DATABASE_NAME")

    print(uri)
    print(user)
    print(password)
    print(database)

    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        driver.verify_connectivity()

        records, summary, keys = driver.execute_query(
            "match (n) return n",
            database_="movie",
        )

        pprint.pprint(records)
        pprint.pprint(summary)
        pprint.pprint(keys)


if __name__ == "__main__":
    load_dotenv()
    main()

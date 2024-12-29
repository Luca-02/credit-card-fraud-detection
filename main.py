from src.generator import Generator
from config import DATASET_OUTPUT_DIR, CUSTOMERS_NUM, TERMINALS_NUM, START_DATE, R


def main():
    generator = Generator(
        dataset_output_path=DATASET_OUTPUT_DIR,
        n_customers=CUSTOMERS_NUM,
        n_terminals=TERMINALS_NUM,
        start_date=START_DATE,
        r=R
    )

    nb_days = 100
    generator.generate(arr_nb_days=[nb_days, nb_days * 2, nb_days * 4])


if __name__ == "__main__":
    main()

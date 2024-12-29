import unittest
import os
import shutil
from src.generator import Generator
from config import DATASET_TEST_OUTPUT_DIR


class TestGenerator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_output_path = DATASET_TEST_OUTPUT_DIR

        if os.path.exists(cls.test_output_path):
            shutil.rmtree(cls.test_output_path)
        os.makedirs(cls.test_output_path)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_output_path):
            shutil.rmtree(cls.test_output_path)

    def test_generate_datasets(self):
        arr_nb_days = [5, 10]

        generator = Generator(
            dataset_output_path=self.test_output_path,
            n_customers=10,
            n_terminals=5,
            start_date='2018-04-01',
            r=50
        )
        generator.generate(arr_nb_days=arr_nb_days)

        for index in range(1, len(arr_nb_days)):
            dataset_subdir = os.path.join(self.test_output_path, str(index))
            self.assertTrue(os.path.exists(dataset_subdir))

            customer_profiles_path = os.path.join(dataset_subdir, 'customer_profiles.csv')
            terminal_profiles_path = os.path.join(dataset_subdir, 'terminal_profiles.csv')
            transactions_path = os.path.join(dataset_subdir, 'transactions.csv')

            self.assertTrue(os.path.exists(customer_profiles_path))
            self.assertTrue(os.path.exists(terminal_profiles_path))
            self.assertTrue(os.path.exists(transactions_path))


if __name__ == '__main__':
    unittest.main()

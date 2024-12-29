import os
import time
import script.data_simulator as data_simulator
from common.utils import clear_dir_path
from common.logger import logger


class Generator:
    def __init__(self, dataset_output_path, n_customers, n_terminals, start_date, r):
        self.dataset_output_path = dataset_output_path
        self.n_customers = n_customers
        self.n_terminals = n_terminals
        self.start_date = start_date
        self.r = r

    def create_dataset(self, nb_days):
        """Generate the dataset."""

        start_time = time.time()
        customer_profiles_table = data_simulator.generate_customer_profiles_table(self.n_customers, random_state=0)
        print(f"Time to generate customer profiles table: {time.time() - start_time:.2f}s")

        terminal_profiles_table = data_simulator.generate_terminal_profiles_table(self.n_terminals, random_state=1)
        print(f"Time to generate terminal profiles table: {time.time() - start_time:.2f}s")

        x_y_terminals = terminal_profiles_table[['x_terminal_id', 'y_terminal_id']].values.astype(float)
        customer_profiles_table['available_terminals'] = customer_profiles_table.apply(
            lambda x: data_simulator.get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=self.r), axis=1)
        customer_profiles_table['nb_terminals'] = customer_profiles_table.available_terminals.apply(len)
        print(f"Time to associate terminals to customers: {time.time() - start_time:.2f}s")

        transactions_df = (customer_profiles_table.groupby('CUSTOMER_ID').apply(
            lambda x: data_simulator.generate_transactions_table(
                x.iloc[0], self.start_date, nb_days)).reset_index(drop=True))
        print(f"Time to generate transactions: {time.time() - start_time:.2f}s")

        # Sort transactions chronologically
        transactions_df = transactions_df.sort_values('TX_DATETIME')
        # Reset indices, starting from 0
        transactions_df.reset_index(inplace=True, drop=True)
        transactions_df.reset_index(inplace=True)
        # TRANSACTION_ID are the dataframe indices, starting from 0
        transactions_df.rename(columns={'index': 'TRANSACTION_ID'}, inplace=True)

        # Adds fraudulent transactions to the dataset
        transactions_df = data_simulator.add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)

        return customer_profiles_table, terminal_profiles_table, transactions_df

    def generate(self, arr_nb_days):
        clear_dir_path(self.dataset_output_path)

        for index, nb_days in enumerate(arr_nb_days):
            start_time = time.time()
            customer_profiles, terminal_profiles, transactions_df = self.create_dataset(nb_days)
            logger.info(f"Time to generate dataset '{index}' [nb_days: {nb_days}]: {time.time() - start_time:.2f}s")

            dataset_subdir = os.path.join(self.dataset_output_path, str(index + 1))
            if not os.path.exists(dataset_subdir):
                os.makedirs(dataset_subdir)

            customer_profiles_path = os.path.join(dataset_subdir, "customer_profiles.csv")
            terminal_profiles_path = os.path.join(dataset_subdir, "terminal_profiles.csv")
            transactions_path = os.path.join(dataset_subdir, "transactions.csv")

            customer_profiles.to_csv(customer_profiles_path, index=False)
            terminal_profiles.to_csv(terminal_profiles_path, index=False)
            transactions_df.to_csv(transactions_path, index=False)

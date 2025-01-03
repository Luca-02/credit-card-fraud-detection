import os
import time
import script.data_simulator as data_simulator


class Generator:
    def __init__(self, n_customers: int, n_terminals: int, start_date: str, r: int):
        self.__n_customers = n_customers
        self.__n_terminals = n_terminals
        self.__start_date = start_date
        self.__r = r

    def __create_dataset(self, nb_days: int):
        """
        Generate a synthetic dataset including customer profiles, terminal profiles, and transaction data over a
        specified number of days.

        :param nb_days: Number of days for which transactions will be simulated.
        :return: A tuple containing customer_profiles_table, terminal_profiles_table and transactions_df
        """

        start_time = time.time()
        customer_profiles_table = data_simulator.generate_customer_profiles_table(self.__n_customers, random_state=0)
        print(f"Time to generate customer profiles table: {time.time() - start_time:.2}s")

        start_time = time.time()
        terminal_profiles_table = data_simulator.generate_terminal_profiles_table(self.__n_terminals, random_state=1)
        print(f"Time to generate terminal profiles table: {time.time() - start_time:.2}s")

        start_time = time.time()
        x_y_terminals = terminal_profiles_table[['x_terminal_id', 'y_terminal_id']].values.astype(float)
        customer_profiles_table['available_terminals'] = customer_profiles_table.apply(
            lambda x: data_simulator.get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=self.__r), axis=1)
        customer_profiles_table['nb_terminals'] = customer_profiles_table.available_terminals.apply(len)
        print(f"Time to associate terminals to customers: {time.time() - start_time:.2}s")

        start_time = time.time()
        transactions_df = (customer_profiles_table.groupby('CUSTOMER_ID').apply(
            lambda x: data_simulator.generate_transactions_table(
                x.iloc[0], self.__start_date, nb_days)).reset_index(drop=True))
        print(f"Time to generate transactions: {time.time() - start_time:.2}s")

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

    def generate(self, dataset_output_path: str, nb_days: int, dataset_name: str = None) -> dict:
        """
        Generates a dataset for a specific number of days and saves it to disk at the specified path.

        :param dataset_output_path: Output path where the dataset will be stored.
        :param nb_days: Number of days to generate data for.
        :param dataset_name: Dataset name. If not specified, it will be based on the number of days.
        :return: dictionary that will contain the name of the dataset, the paths where the dataset is stored and
            the generation time.
        """

        if not dataset_name:
            dataset_name = f'nb_days_{nb_days}'

        start_time1 = time.time()
        customer_profiles, terminal_profiles, transactions_df = self.__create_dataset(nb_days)

        dataset_subdir = os.path.join(dataset_output_path, dataset_name)
        if not os.path.exists(dataset_subdir):
            os.makedirs(dataset_subdir)

        print(f"Time to generate datasets {dataset_name}: {time.time() - start_time1:.2}s")

        start_time2 = time.time()
        customer_profiles_path = os.path.join(dataset_subdir, "customer_profiles.csv")
        terminal_profiles_path = os.path.join(dataset_subdir, "terminal_profiles.csv")
        transactions_path = os.path.join(dataset_subdir, "transactions.csv")

        customer_profiles.to_csv(customer_profiles_path, index=False)
        terminal_profiles.to_csv(terminal_profiles_path, index=False)
        transactions_df.to_csv(transactions_path, index=False)

        print(f"Time to save datasets {dataset_name} .csv files locally: {time.time() - start_time2:.2}s")

        return {
            "dataset_name": dataset_name,
            "dataset_paths": dataset_subdir,
            "generation_time": time.time() - start_time1,
            "csv_saving_time": time.time() - start_time2
        }
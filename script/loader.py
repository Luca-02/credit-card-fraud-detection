import os
import ast
import pandas as pd
from neo4j import GraphDatabase


class Loader:
    def __init__(self, uri, user, password, database):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.__database = database
        self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))

    @staticmethod
    def __load_terminals(tx, terminal_profiles_df):
        query = """
        UNWIND $rows AS row
        MERGE (t:Terminal {
            terminal_id: row.TERMINAL_ID,
            x_location: row.x_terminal_id,
            y_location: row.y_terminal_id
        })
        """

        tx.run(query, rows=terminal_profiles_df.to_dict(orient="records"))

    @staticmethod
    def __load_customers(tx, customer_profiles_df):
        query = """
        UNWIND $rows AS row
        MERGE (c:Customer {
            customer_id: row.CUSTOMER_ID,
            x_location: row.x_customer_id,
            y_location: row.y_customer_id,
            mean_amount: row.mean_amount,
            std_amount: row.std_amount,
            mean_nb_tx_per_day: row.mean_nb_tx_per_day,
            nb_terminals: row.nb_terminals
        })

        WITH c, row.available_terminals as available_terminals
        UNWIND available_terminals AS terminal_id
        MATCH (t:Terminal {terminal_id: terminal_id})
        MERGE (c)-[:ACCESS_TO]->(t)
        """

        # Convert the 'available_terminals' column from string to a list of integers
        customer_profiles_df['available_terminals'] = \
            customer_profiles_df['available_terminals'].apply(ast.literal_eval)

        tx.run(query, rows=customer_profiles_df.to_dict(orient="records"))

    @staticmethod
    def __load_transactions(tx, transactions_path_df):
        query = """
        UNWIND $rows AS row
        MERGE (tx:Transaction {
            transaction_id: row.TRANSACTION_ID,
            datetime: row.TX_DATETIME,
            amount: row.TX_AMOUNT,
            fraudulent: row.TX_FRAUD
        })
        
        WITH tx, row.CUSTOMER_ID AS customer_id, row.TERMINAL_ID AS terminal_id
        MATCH (c:Customer {customer_id: customer_id}), (t:Terminal {terminal_id: terminal_id})
        MERGE (c)-[:MAKE]->(tx)
        MERGE (tx)-[:FROM]->(t)
        """

        tx.run(query, rows=transactions_path_df.to_dict(orient="records"))


    def __close(self):
        if self.__driver is not None:
            self.__driver.close()

    def load_dataset(self, dataset_path: str):
        with self.__driver.session(database=self.__database) as session:
            session.run("MATCH (n) DETACH DELETE n")

            customer_profiles_path = os.path.join(dataset_path, "customer_profiles.csv")
            terminal_profiles_path = os.path.join(dataset_path, "terminal_profiles.csv")
            transactions_path = os.path.join(dataset_path, "transactions.csv")

            customer_profiles_df = pd.read_csv(customer_profiles_path)
            terminal_profiles_df = pd.read_csv(terminal_profiles_path)
            transactions_path_df = pd.read_csv(transactions_path)

            session.execute_write(self.__load_terminals, terminal_profiles_df)
            session.execute_write(self.__load_customers, customer_profiles_df)
            session.execute_write(self.__load_transactions, transactions_path_df)

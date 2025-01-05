import os
import time
from script.database import DatabaseInstance


class Loader:
    def __init__(self, db: DatabaseInstance):
        self.db = db

    @staticmethod
    def __load_terminals(tx, terminal_profiles_path):
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        MERGE (t:Terminal {
            terminal_id: toInteger(row.TERMINAL_ID),
            x_location: toFloat(row.x_terminal_id),
            y_location: toFloat(row.y_terminal_id)
        })
        """

        tx.run(query, file_path=f"file:///{terminal_profiles_path.replace(os.sep, '/')}")

    @staticmethod
    def __load_customers(tx, customer_profiles_path):
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        MERGE (c:Customer {
            customer_id: toInteger(row.CUSTOMER_ID),
            x_location: toFloat(row.x_customer_id),
            y_location: toFloat(row.y_customer_id),
            mean_amount: toFloat(row.mean_amount),
            std_amount: toFloat(row.std_amount),
            mean_nb_tx_per_day: toFloat(row.mean_nb_tx_per_day),
            nb_terminals: toInteger(row.nb_terminals)
        })
    
        WITH c, apoc.convert.fromJsonList(row.available_terminals) as available_terminals
        UNWIND available_terminals AS terminal_id
        MATCH (t:Terminal {terminal_id: terminal_id})
        MERGE (c)-[:ACCESS_TO]->(t)
        """

        tx.run(query, file_path=f"file:///{customer_profiles_path.replace(os.sep, '/')}")

    @staticmethod
    def __load_transactions(tx, transactions_path):
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        MATCH (c:Customer {customer_id: toInteger(row.CUSTOMER_ID)})
        MATCH (t:Terminal {terminal_id: toInteger(row.TERMINAL_ID)})
        MERGE (c)-[:TRANSACTION {
            transaction_id: toInteger(row.TRANSACTION_ID),
            datetime: datetime(replace(row.TX_DATETIME, ' ', 'T')),
            amount: toFloat(row.TX_AMOUNT),
            fraudulent: toInteger(row.TX_FRAUD) = 1
        }]->(t)
        """

        tx.run(query, file_path=f"file:///{transactions_path.replace(os.sep, '/')}")

    def load_dataset(self, dataset_path: str) -> float:
        with self.db.get_session() as session:
            # Delete all existing nodes and relationships in the database
            session.run("MATCH (n) DETACH DELETE n")

            terminal_profiles_path = os.path.join(dataset_path, "terminal_profiles.csv")
            customer_profiles_path = os.path.join(dataset_path, "customer_profiles.csv")
            transactions_path = os.path.join(dataset_path, "transactions.csv")

            start_time = time.time()

            session.execute_write(self.__load_terminals, terminal_profiles_path)
            session.execute_write(self.__load_customers, customer_profiles_path)
            session.execute_write(self.__load_transactions, transactions_path)

            print(f"Time to load dataset: {time.time() - start_time:.3f}s")

            return time.time() - start_time

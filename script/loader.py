import os

from script.database import DatabaseInstance


class Loader:
    def __init__(self, db: DatabaseInstance):
        """
        A class to handle datasets load in the database.

        :param db: Database instance.
        """

        self.__db = db

    def __clear_database(self):
        self.__db.execute_query("MATCH (n) DETACH DELETE n")
        self.__db.execute_query("DROP INDEX customer_index IF EXISTS")
        self.__db.execute_query("DROP INDEX terminal_index IF EXISTS")

    def __load_customers(self, customer_profiles_path: str) -> float:
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        CALL (row) {
            MERGE (c:Customer {
                customer_id: toInteger(row.CUSTOMER_ID),
                x_location: toFloat(row.x_customer_id),
                y_location: toFloat(row.y_customer_id),
                mean_amount: toFloat(row.mean_amount),
                std_amount: toFloat(row.std_amount),
                mean_nb_tx_per_day: toFloat(row.mean_nb_tx_per_day),
                nb_terminals: toInteger(row.nb_terminals)
            })
        } IN TRANSACTIONS OF 1000 ROWS
        """

        return self.__db.execute_query(query, file_path=f"file:///{customer_profiles_path.replace(os.sep, '/')}")

    def __load_terminals(self, terminal_profiles_path: str) -> float:
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        CALL (row) {
            MERGE (t:Terminal {
                terminal_id: toInteger(row.TERMINAL_ID),
                x_location: toFloat(row.x_terminal_id),
                y_location: toFloat(row.y_terminal_id)
            })
        } IN TRANSACTIONS OF 1000 ROWS
        """

        return self.__db.execute_query(query, file_path=f"file:///{terminal_profiles_path.replace(os.sep, '/')}")

    def __load_customer_index(self) -> float:
        query = """
        CREATE INDEX customer_index IF NOT EXISTS FOR (c:Customer) ON (c.customer_id)
        """

        return self.__db.execute_query(query)

    def __load_terminal_index(self) -> float:
        query = """
        CREATE INDEX terminal_index IF NOT EXISTS FOR (t:Terminal) ON (t.terminal_id)
        """

        return self.__db.execute_query(query)

    def __load_transactions(self, transactions_path: str) -> float:
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        CALL (row) {
            MATCH (c:Customer {customer_id: toInteger(row.CUSTOMER_ID)})
            MATCH (t:Terminal {terminal_id: toInteger(row.TERMINAL_ID)})
            MERGE (c)-[:TRANSACTION {
                transaction_id: toInteger(row.TRANSACTION_ID),
                datetime: datetime(replace(row.TX_DATETIME, ' ', 'T')),
                amount: toFloat(row.TX_AMOUNT),
                fraudulent: toInteger(row.TX_FRAUD) = 1
            }]->(t)
        } IN TRANSACTIONS OF 1000 ROWS
        """

        return self.__db.execute_query(query, file_path=f"file:///{transactions_path.replace(os.sep, '/')}")

    def __load_available_terminals(self, customer_profiles_path: str) -> float:
        query = """
        LOAD CSV WITH HEADERS FROM $file_path AS row
        CALL (row) {
            WITH toInteger(row.CUSTOMER_ID) AS customer_id,
                apoc.convert.fromJsonList(row.available_terminals) as available_terminals
            UNWIND available_terminals AS terminal_id
            MATCH (c:Customer {customer_id: customer_id})
            MATCH (t:Terminal {terminal_id: terminal_id})
            CREATE (c)-[:ACCESS_TO]->(t)
        } IN TRANSACTIONS OF 1000 ROWS
        """

        return self.__db.execute_query(query, file_path=f"file:///{customer_profiles_path.replace(os.sep, '/')}")

    def load_dataset(self, dataset_path: str) -> float:
        """
        Load the datasets located in the specified dataset_path in the database.

        :param dataset_path: path where the datasets to load is stored.
        :return: Loading execution time in seconds.
        """

        customer_profiles_path = os.path.join(dataset_path, "customer_profiles.csv")
        terminal_profiles_path = os.path.join(dataset_path, "terminal_profiles.csv")
        transactions_path = os.path.join(dataset_path, "transactions.csv")

        # Delete all existing nodes and relationships in the database
        self.__clear_database()

        customer_time = self.__load_customers(customer_profiles_path)
        print(f"Time to load customer profiles table: {customer_time:.3f}s")

        terminal_time = self.__load_terminals(terminal_profiles_path)
        print(f"Time to load terminal profiles table: {terminal_time:.3f}s")

        customer_index_time = self.__load_customer_index()
        print(f"Time to load customer index: {customer_index_time:.3f}s")

        terminal_index_time = self.__load_terminal_index()
        print(f"Time to load terminal index: {terminal_index_time:.3f}s")

        transactions_time = self.__load_transactions(transactions_path)
        print(f"Time to load transactions: {transactions_time:.3f}s")

        available_terminals_time = self.__load_available_terminals(customer_profiles_path)
        print(f"Time to load available terminal: {available_terminals_time:.3f}s")

        return (customer_time +
                terminal_time +
                customer_index_time +
                terminal_index_time +
                transactions_time +
                available_terminals_time)

import os
import ast
import pandas as pd
from script.database import DatabaseInstance


def __load_terminals(tx, dataset_path):
    query = """
    LOAD CSV WITH HEADERS FROM $file_path AS row
    MERGE (t:Terminal {
        terminal_id: toInteger(row.TERMINAL_ID),
        x_location: toFloat(row.x_terminal_id),
        y_location: toFloat(row.y_terminal_id)
    })
    """

    terminal_profiles_path = os.path.join(dataset_path, "terminal_profiles.csv")
    tx.run(query, file_path=f"file:///{terminal_profiles_path.replace(os.sep, '/')}")


def __load_customers(tx, dataset_path):
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

    customer_profiles_path = os.path.join(dataset_path, "customer_profiles.csv")
    tx.run(query, file_path=f"file:///{customer_profiles_path.replace(os.sep, '/')}")


def __load_transactions(tx, dataset_path):
    query = """
    LOAD CSV WITH HEADERS FROM $file_path AS row
    MERGE (tx:Transaction {
        transaction_id: toInteger(row.TRANSACTION_ID),
        datetime: datetime(replace(row.TX_DATETIME, ' ', 'T')),
        amount: toFloat(row.TX_AMOUNT),
        fraudulent: toInteger(row.TX_FRAUD) = 1
    })

    WITH tx, toInteger(row.CUSTOMER_ID) AS customer_id, toInteger(row.TERMINAL_ID) AS terminal_id
    MATCH (c:Customer {customer_id: customer_id})
    MATCH (t:Terminal {terminal_id: terminal_id})
    MERGE (c)-[:MAKE]->(tx)
    MERGE (tx)-[:FROM]->(t)
    """

    transactions_path = os.path.join(dataset_path, "transactions.csv")
    tx.run(query, file_path=f"file:///{transactions_path.replace(os.sep, '/')}")


def load_dataset(db: DatabaseInstance, dataset_path: str):
    with db.get_session() as session:
        # Delete all existing nodes and relationships in the database
        session.run("MATCH (n) DETACH DELETE n")

        # Load data into the database
        session.execute_write(__load_terminals, dataset_path)
        session.execute_write(__load_customers, dataset_path)
        session.execute_write(__load_transactions, dataset_path)

from script.database import DatabaseInstance


def __execute_query(db: DatabaseInstance, query: str, parameters=None):
    with db.get_session() as session:
        result = session.run(query, parameters or {})
        return result.data()


def query_a(db: DatabaseInstance):
    """
    For each customer X, identify the customer Y (or the costumers) that share at least 3
    terminals in which Y executes transactions and the spending amount of Y differs less than
    the 10% with respect to that of X. Return the name of X, the spending amount of X, the
    spending amount of the related costumer Y and the spending amount of Y.
    """

    query = """
    MATCH (x:Customer)-[:ACCESS_TO]->(t:Terminal)<-[:FROM]-(:Transaction)<-[:MAKE]-(y:Customer)
    WHERE x <> y
    WITH x, y, COUNT(DISTINCT t) AS shared_terminals
    WHERE shared_terminals >= 3
    WITH x, y
    
    MATCH (x)-[:MAKE]->(tx_x:Transaction)-[:FROM]->(t:Terminal)<-[:FROM]-(tx_y:Transaction)<-[:MAKE]-(y)
    WITH x, y, SUM(tx_x.amount) AS amount_x, SUM(tx_y.amount) AS amount_y
    WHERE ABS(amount_x - amount_y) / amount_x < 0.1
    RETURN x.customer_id AS customer_x, amount_x, y.customer_id AS customer_y, amount_y
    """

    return __execute_query(db, query)


def query_b(db: DatabaseInstance):
    """
    For each terminal identify the possible fraudulent transactions of the current month. The
    fraudulent transactions are those whose import is higher than 20% of the average import of
    the transactions executed on the same terminal in the previous month.
    """

    query = """
    WITH date() AS current_date, date() - duration({months: 1}) AS prev_month_date

    MATCH (t:Terminal)<-[:FROM]-(tx:Transaction)
    WITH t, tx, date(datetime(replace(tx.datetime, ' ', 'T'))) as tx_datetime, current_date
    WHERE tx_datetime >= prev_month_date AND tx_datetime < current_date
    WITH t, AVG(tx.amount) AS avg_amount_last_month, current_date
    
    MATCH (t)<-[:FROM]-(tx:Transaction)
    WITH t, avg_amount_last_month, current_date, tx, date(datetime(replace(tx.datetime, ' ', 'T'))) as tx_datetime
    WHERE tx_datetime = current_date
    WITH t, avg_amount_last_month, tx
    
    WHERE tx.amount - avg_amount_last_month / avg_amount_last_month > 0.2
    RETURN t.terminal_id AS terminal_id, COLLECT(tx.transaction_id) AS possible_fraudulent_transaction
    """

    return __execute_query(db, query)

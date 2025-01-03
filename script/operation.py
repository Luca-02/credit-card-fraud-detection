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

def query_c(db: DatabaseInstance, u_customer_id: int, degree: int):
    """
    Given a user u, determine the "co-customer-relationships CC of degree k". A user u' is a co-customer of u if you
    can determine a chain "u1-t1-u2-t2-…tk-1-uk" such that u1=u, uk=u', and for each 1<=I,j<=k, ui <> uj, and
    t1,...tk-1 are the terminals on which a transaction has been executed.
    Therefore, CCk(u)={u'| a chain exists between u and u' of degree k}.
    """

    # If degree is less than 1 I cannot find solutions because in the case of degree < 0 it is impossible and
    # instead in the case degree = 1 the only possible co-customer could be u, but with the condition ui <> uj
    # for every i and j it would not be possible
    if degree < 2:
        return []

    # u0 ->.-> t0 <-.<- u1 ->.-> t1 <-.<- u2 ->.-> t2 <-.<- u3

    # u1 ->.-> t1 <-.<- u2
    # u2 ->.-> t2 <-.<- u3
    # u3 ->.-> t4 <-.<- u4
    match_clauses = []
    for i in range(1, degree):
        if i == 0:
            match_clauses.append(f"(u{i}:Customer {{customer_id: {u_customer_id}}})-[:MAKE]->()-[:FROM]->(t{i})<-[:FROM]-()<-[:MAKE]-(u{i+1})")
        elif i != 0:
            match_clauses.append(f"(u{i})-[:MAKE]->()-[:FROM]->(t{i})<-[:FROM]-()<-[:MAKE]-(u{i+1})")

    print(match_clauses)

    """
    (u1:Customer {{customer_id: $user_id}})-[:MAKE]->(tx1:Transaction)-[:FROM]->(t1:Terminal)
    <-[:FROM]-(tx2:Transaction)-[:MAKE]->(u2:Customer)-[:MAKE]->(tx3:Transaction)-[:FROM]->(t2:Terminal)
    <-[:FROM]-(tx4:Transaction)-[:MAKE]->(u3:Customer)
    """

    query = ""

    # return __execute_query(db, query)

    # query = f"""
    # MATCH path = (u1:Customer {{customer_id: $user_id}})-[:MAKE]->()-[:FROM]->(t1:Terminal)
    # <-[:FROM]-()-[:MAKE]-(u2:Customer)-[:MAKE*0..{degree - 1}]->()-[:FROM]->(t2:Terminal)
    # <-[:FROM]-()
    # WHERE ALL(i IN range(0, length(nodes(path)) - 2) WHERE
    #     (i % 3 = 0 AND nodes(path)[i]:Customer) AND
    #     (i % 3 = 1 AND nodes(path)[i]:Transaction) AND
    #     (i % 3 = 2 AND nodes(path)[i]:Terminal))
    # RETURN DISTINCT u2.customer_id AS reachable_users
    # """
    #
    # query = f"""
    # MATCH path = (u1:Customer {{customer_id: $user_id}})-[:MAKE]->(tx1:Transaction)-[:FROM]->(t1:Terminal)
    # <-[:FROM]-(tx2:Transaction)-[:MAKE]->(u2:Customer)-[:MAKE]->(tx3:Transaction)-[:FROM]->(t2:Terminal)
    # <-[:FROM]-(tx4:Transaction)-[:MAKE]->(u3:Customer)
    # WITH u2, path
    # UNWIND range(0, {degree}-1) AS i
    # MATCH path = (u1)-[:MAKE]->(tx1)-[:FROM]->(t1)-[:FROM]->(tx2)-[:MAKE]->(u2)-[:MAKE]->(tx3)-[:FROM]->(t2)
    # RETURN DISTINCT u2.customer_id AS reachable_users
    # """
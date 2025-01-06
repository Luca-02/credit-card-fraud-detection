from script.database import DatabaseInstance


class Operations:
    def __init__(self, db: DatabaseInstance):
        """
        A class to handle some operations on the dataset loaded in the database.

        :param db: Database instance.
        """

        self.__db = db

    def operation_a(self):
        """
        For each customer X, identify the customer Y (or the costumers) that share at least 3
        terminals in which Y executes transactions and the spending amount of Y differs less than
        the 10% with respect to that of X. Return the name of X, the spending amount of X, the
        spending amount of the related costumer Y and the spending amount of Y.
        """

        query = """
        MATCH (x:Customer)-[tx_x:TRANSACTION]->(t:Terminal)<-[tx_y:TRANSACTION]-(y:Customer)
        WHERE x <> y
        WITH x, y, COUNT(DISTINCT t) AS shared_terminals, SUM(tx_x.amount) AS amount_x, SUM(tx_y.amount) AS amount_y
        WHERE shared_terminals >= 3 AND ABS(amount_x - amount_y) / amount_x < 0.1
        RETURN x.customer_id AS customer_x, amount_x, y.customer_id AS customer_y, amount_y
        """

        return self.__db.execute_query(query, query_name="a")

    def operation_b(self):
        """
        For each terminal identify the possible fraudulent transactions of the current month. The
        fraudulent transactions are those whose import is higher than 20% of the average import of
        the transactions executed on the same terminal in the previous month.
        """

        query = """
        WITH date() AS current_date, date() - duration({months: 1}) AS prev_month_date
        
        MATCH (t:Terminal)<-[tx:TRANSACTION]-(:Customer)
        WITH t, tx, date(tx.datetime) AS tx_datetime, current_date, prev_month_date
        WHERE tx_datetime >= prev_month_date AND tx_datetime < current_date
        WITH t, AVG(tx.amount) AS avg_amount_last_month, current_date
        
        MATCH (t)<-[tx:TRANSACTION]-(:Customer)
        WITH t.terminal_id AS terminal_id, avg_amount_last_month, current_date, tx, date(tx.datetime) as tx_datetime
        WHERE tx_datetime = current_date
        WITH terminal_id, avg_amount_last_month, tx.transaction_id as transaction_id, tx.amount as amount
        
        WHERE amount - avg_amount_last_month / avg_amount_last_month > 0.2
        RETURN terminal_id, COLLECT(transaction_id) AS possible_fraudulent_transaction
        """

        return self.__db.execute_query(query, query_name="b")

    def operation_c(self, u_customer_id: int, k: int):
        """
        Given a user u, determine the "co-customer-relationships CC of degree k". A user u' is a co-customer
        of u if you can determine a chain "u1-t1-u2-t2-…tk-1-uk" such that u1=u, uk=u', and for each
        1<=I,j<=k, ui <> uj, and t1,...tk-1 are the terminals on which a transaction has been executed.
        Therefore, CCk(u)={u'| a chain exists between u and u' of degree k}.
        """

        # If degree is less than 2 I cannot find solutions because in the case of degree < 0 it is impossible and
        # instead in the case degree = 1 the only possible co-customer could be u, but with the condition ui <> uj
        # for every i and j it would be impossible
        if k < 2:
            return []

        query_sections = []
        for i in range(1, k):
            if i == 1:
                query_sections.append(f"""
                MATCH (u{i}:Customer {{customer_id: {u_customer_id}}})-[:TRANSACTION]->(:Terminal)<-[:TRANSACTION]-(u{i + 1}:Customer)
                WHERE u{i}.customer_id <> u{i + 1}.customer_id
                WITH DISTINCT u{i + 1}, [u{i}.customer_id, u{i + 1}.customer_id] as path_customers
                """)
            elif i != 1:
                query_sections.append(f"""
                MATCH (u{i})-[:TRANSACTION]->(:Terminal)<-[:TRANSACTION]-(u{i + 1}:Customer)
                WHERE NOT u{i + 1}.customer_id IN path_customers
                WITH DISTINCT u{i + 1}, path_customers + u{i + 1}.customer_id as path_customers
                """)

        query = '\n'.join(query_sections)
        query += f"""
        RETURN DISTINCT u{k}.customer_id AS co_customer
        """

        return self.__db.execute_query(query, query_name="c")

    def operation_d_i(self):
        """
        Each transaction should be extended with:
        1. The period of the day {morning, afternoon, evening, night} in which the transaction has been executed.
        2. The kind of products that have been bought through the transaction {hightech, food, clothing, consumable, other}
        3. The feeling of security expressed by the user. This is an integer value between 1 and 5 expressed
        by the user when conclude the transaction.
        The values can be chosen randomly.
        """

        query = """
        MATCH ()-[tx:TRANSACTION]->()
        SET tx.period_of_day = CASE
            WHEN time(tx.datetime).hour >= 6 AND time(tx.datetime).hour < 12 THEN 'morning'
            WHEN time(tx.datetime).hour >= 12 AND time(tx.datetime).hour < 18 THEN 'afternoon'
            WHEN time(tx.datetime).hour >= 18 AND time(tx.datetime).hour < 22 THEN 'evening'
            ELSE 'night'
        END,
        tx.product_type = CASE
            WHEN rand() < 0.2 THEN 'hightech'
            WHEN rand() < 0.4 THEN 'food'
            WHEN rand() < 0.6 THEN 'clothing'
            WHEN rand() < 0.8 THEN 'consumable'
            ELSE 'other'
        END,
        tx.security_feeling = toInteger(rand() * 5) + 1
        """

        return self.__db.execute_query(query, query_name="d.i")

    def operation_d_ii(self):
        """
        Customers that make more than three transactions from the same terminal
        expressing a similar average feeling of security should be connected as
        “buying_friends”. Therefore, also this kind of relationship should be explicitly stored
        in the NOSQL database and can be queried. Note, two average feelings of security
        are considered similar when their difference is lower than 1.
        """

        query = """
        MATCH (c1:Customer)-[tx1:TRANSACTION]->(t:Terminal)<-[tx2:TRANSACTION]-(c2:Customer)
        WHERE c1 <> c2
        WITH c1, c2, 
             COUNT(DISTINCT tx1) AS tx1_count, COUNT(DISTINCT tx2) AS tx2_count,
             AVG(tx1.security_feeling) AS c1_avg_security, AVG(tx2.security_feeling) AS c2_avg_security
        WHERE tx1_count > 3 AND tx2_count > 3 AND ABS(c1_avg_security - c2_avg_security) < 1
        MERGE (c1)-[:BUYING_FRIENDS]->(c2)
        """

        return self.__db.execute_query(query, query_name="d.ii")

    def operation_e(self):
        """
        For each period of the day identifies the number of transactions that occurred in that period,
        and the average number of fraudulent transactions.
        """

        query = """
        MATCH ()-[tx:TRANSACTION]->()
        RETURN tx.period_of_day AS period_of_day, COUNT(tx) AS transactions,
            AVG(CASE WHEN tx.fraudulent THEN 1 ELSE 0 END) AS avg_fraudulent_transactions
        """

        return self.__db.execute_query(query, query_name="e")

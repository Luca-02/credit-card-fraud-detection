import time
from typing import Any

from neo4j import GraphDatabase, Session


class DatabaseInstance:
    def __init__(self, uri: str, user: str, password: str, database: str):
        """
        A class to handle interactions with a Neo4j database instance.

        :param uri: The URI of the Neo4j database.
        :param user: The username for authentication.
        :param password: The password for authentication.
        :param database: The name of the Neo4j database to connect to.
        """

        self.__uri = uri
        self.__user = user
        self.__password = password
        self.__database = database
        self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))

    def get_session(self) -> Session | None:
        if self.__driver is not None:
            return self.__driver.session(database=self.__database)
        return None

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def execute_query(self, query, query_name=None) -> dict[Any, float]:
        """
        Execute query on the database.

        :param query: Query string.
        :param query_name: Optional parameter that identifies the query name.
        :return: Query data result and execution time in seconds.
        """

        with self.get_session() as session:
            start_time = time.time()
            result = session.run(query)
            data = result.data()
            execution_time = time.time() - start_time

            if query_name is None:
                print(f"Time to execute query: {execution_time:.3f}s")
            else:
                print(f"Time to execute query {query_name}: {execution_time:.3f}s")

            return {
                "result": data,
                "execution_time": execution_time
            }

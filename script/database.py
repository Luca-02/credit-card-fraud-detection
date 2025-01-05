import time
from typing import Any

from neo4j import GraphDatabase, Session


class DatabaseInstance:
    def __init__(self, uri: str, user: str, password: str, database: str):
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

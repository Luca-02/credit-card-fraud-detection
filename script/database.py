from neo4j import GraphDatabase


class DatabaseInstance:
    def __init__(self, uri, user, password, database):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.__database = database
        self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))

    def get_session(self):
        return self.__driver.session(database=self.__database)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()